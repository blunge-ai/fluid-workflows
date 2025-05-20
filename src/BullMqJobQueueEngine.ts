import { v4 as uuidv4 } from 'uuid';

import { Worker as BullWorker, Queue as BullQueue } from 'bullmq';
import Redis from 'ioredis';
import { pack, unpack } from './packer';
import mapValues from 'lodash/mapValues';

import type { JobQueueEngine, JobData, JobStatus, JobResult } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { defaultRedisConnection, timeout, Logger, defaultLogger, assert } from './utils';

const bullWorkerBlockingTimeoutSecs = 2;
const fallbackDelayMs = 200;

export type Opts = {
  attempts: number,
  lockTimeoutMs: number,

  blockingTimeoutSecs: number,
  statusNotifierRepeatMs: number,
  redisConnection: () => Redis,
  concurrency: number,
  fallback: boolean,
  logger: Logger,
  maximumWaitTimeoutMs: number,
};

export type ConstructorOpts
 = Partial<Opts>
 & Pick<Opts, 'attempts' | 'lockTimeoutMs'>;

export type QueueOpts = Opts & {
  queue: string,
  statusNotifierQueue: string,
};

type InternalJobData<Input, Meta> = {
  job: JobData<Input, Meta>,
  dataKey: string,
  parentId?: string,
  childDataKeys?: Record<string, string>,
};

export function bullQueue<Input, Output>(name: string, redis: Redis, attempts = 1) {
  return new BullQueue<Input, Output>(name, {
    connection: redis,
    defaultJobOptions: {
      attempts,
      removeOnComplete: true,
      removeOnFail: true,
    },
  });
}

export function bullWorker<Input, Output>(
  name: string,
  redis: Redis,
  {
    timeoutMs,
    blockingTimeoutSecs,
  }: { timeoutMs: number; blockingTimeoutSecs: number }
) {
  return new BullWorker<Input, Output>(name, null, {
    connection: redis,
    lockDuration: timeoutMs,
    maxStalledCount: 0, // this makes any stalled jobs fail and get retried (using up attempts)
    drainDelay: blockingTimeoutSecs, // number of seconds to long poll for jobs
  });
}

function isSubset(objA: Record<string, string>, objB: Record<string, string>): boolean {
  return Object.keys(objA).every((key) => objA[key] === objB[key]);
}

async function deleteKeys(redis: Redis, keys: string[]) {
  let multi = redis.multi();
  for (const key of keys) {
    multi = multi.del(key)
  }
  await multi.exec();
}

export class BullMqJobQueue<Input, Output, Meta, ProgressInfo> {

  private redis: Redis;
  private queue: BullQueue<InternalJobData<Input, Meta>, JobResult<Output>>;
  private statusNotifierQueue: BullQueue;
  private opts: QueueOpts;

  private _worker: BullWorker<InternalJobData<Input, Meta>, JobResult<Output>> | undefined = undefined;
  private get worker() {
    if (this._worker == undefined) {
      this._worker = bullWorker(
        this.opts.queue,
        this.opts.redisConnection(),
        {
          timeoutMs: this.opts.lockTimeoutMs,
          blockingTimeoutSecs: bullWorkerBlockingTimeoutSecs,
        }
      );
      this.startMaintenanceWorkers();
    }
    return this._worker;
  }
  
  _getBullQueue() {
    return this.queue;
  }

  async startMaintenanceWorkers() {
    void this.worker.startStalledCheckTimer();
    this.addStatusNotifierJob();
    this.processStatusNotifierJobs();
  }

  constructor(opts: Opts & { queue: string, redis: Redis }) {
    this.opts = {
      statusNotifierQueue: `${opts.queue}/status-notifier`,
      ...opts,
    };
    this.redis = opts.redis;
    this.queue = bullQueue(this.opts.queue, this.redis, this.opts.attempts);
    this.statusNotifierQueue = bullQueue(this.opts.statusNotifierQueue, this.redis);
  }

  async submitJob(opts: { data: JobData<Input, Meta>, dataKey: string, queue: string, parentId?: string, parentQueue?: string }): Promise<void> {
    const job = opts.data;
    const jobData: InternalJobData<Input, Meta> = {
      job,
      dataKey: opts.dataKey,
    };
    jobData.job = { ...job, input: undefined as Input };
    await this.redis.set(`jobs:input:${jobData.dataKey}`, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
    let parent = undefined;
    if (opts.parentId) {
      assert(opts.parentQueue);
      parent = { id: opts.parentId, queue: opts.parentQueue };
    }
    const [waiting] = await Promise.all([
      this.queue.getWaitingCount(),
      this.queue.add('job', jobData, { jobId: job.id, parent })
    ]);
    await this.publishStatus(jobData.dataKey, {
      jobId: job.id,
      type: 'queued',
      meta: job.meta,
      waiting,
    });
  }

  async acquireJob(opts: { token: string, block?: boolean }): Promise<JobData<Input, Meta> | undefined> {
    const { token, block } = opts;
    const start = Date.now();
    while (true) {
      const elapsedMs = Date.now() - start;
      // we can't block exactly for the right time, but we can reduce the margin of error to 1/2
      const marginMs = bullWorkerBlockingTimeoutSecs * 1000 / 2;
      const remainingMs = this.opts.blockingTimeoutSecs * 1000 - elapsedMs - marginMs;
      if (remainingMs <= 0) {
        break;
      }
      if (this.opts.fallback && block) {
        await timeout(fallbackDelayMs);
      }
      const bullJob = await this.worker.getNextJob(token, {
        block: !!block && !this.opts.fallback
      });
      if (!bullJob) {
        if (!block) {
          break;
        }
        continue;
      }
      const jobId = bullJob.id;
      if (jobId == undefined) {
        this.opts.logger.error({
          queue: this.opts.queue
        }, 'get next job: job has no id');
        await bullJob.moveToCompleted({
          type: 'error',
          reason: 'get next job: job has no id'
        }, token, false);
        continue;
      }
      let input: Input | undefined = bullJob.data.job.input;
      const buffer = await this.redis.getBuffer(`jobs:input:${bullJob.data.dataKey}`);
      if (!buffer) {
        this.opts.logger.warn({
          jobId,
          queue: this.opts.queue,
        }, `get next job: missing job data, probably timed out`);
        await this.completeJob({
          token,
          jobId,
          result: { type: 'error', reason: 'get next job: missing job data' }
        });
        continue;
      }
      input = unpack(buffer) as Input;
      this.opts.logger.info({ jobId, queue: this.opts.queue }, `get next job: processing job`);
      return { ...bullJob.data.job, input } satisfies JobData<Input, Meta>;
    }
    return undefined;
  }

  async updateJob({ token, jobId, progressInfo, lockTimeoutMs, input }: {
    token: string,
    jobId: string,
    progressInfo?: ProgressInfo,
    lockTimeoutMs?: number,
    input?: Input,
  }): Promise<{ interrupt: boolean }> {
    const bullJob = await this.queue.getJob(jobId);
    if (!bullJob) {
      this.opts.logger.error({
        jobId,
        queue: this.opts.queue
      }, 'publish status: job not found');
      throw Error('update status: job not found');
    }
    void bullJob.extendLock(token, lockTimeoutMs ?? this.opts.lockTimeoutMs);
    if (progressInfo != undefined) {
      await this.publishStatus(bullJob.data.dataKey, {
        jobId,
        type: 'active',
        meta: bullJob.data.job.meta,
        info: progressInfo,
      });
    }
    if (input !== undefined) {
      await this.redis.set(`jobs:input:${bullJob.data.dataKey}`, pack(input), 'PX', this.opts.maximumWaitTimeoutMs);
    }
    return { interrupt: false };
  }

  async completeJob(opts: { 
    token: string, 
    jobId: string, 
    result: JobResult<Output> 
  }): Promise<void> {
    const { token, result } = opts;
    const jobId = opts.jobId;
    const bullJob = await this.queue.getJob(jobId);
    if (!bullJob) {
      this.opts.logger.warn({
        jobId,
        queue: this.opts.queue
      }, `complete job: no such job, probably timed out`);
      return;
    }
    const resultWithoutOutput = {
      ...result,
      output: undefined
    } as JobResult<Output | undefined>;
    this.redis.set(`jobs:result:${bullJob.data.dataKey}`, pack(result), 'PX', this.opts.maximumWaitTimeoutMs)
    if (result.type === 'error') {
      this.opts.logger.info({
        jobId,
        queue: this.opts.queue,
        reason: result.reason,
      }, 'complete job: completed with error');
      await bullJob.moveToFailed(new Error(`job completed with error: ${result.reason}`), token, false);
      await this.publishStatus(bullJob.data.dataKey, {
        jobId,
        type: result.type,
        meta: bullJob.data.job.meta,
        reason: result.reason,
      });
    } else if (result.type === 'cancelled') {
      await bullJob.moveToCompleted(resultWithoutOutput, token, false);
      await this.publishStatus(bullJob.data.dataKey, {
        jobId,
        type: result.type,
        meta: bullJob.data.job.meta,
        reason: result.reason,
      });
    } else {
      await bullJob.moveToCompleted(resultWithoutOutput, token, false);
      await this.publishStatus(bullJob.data.dataKey, {
        jobId,
        type: result.type,
        meta: bullJob.data.job.meta,
        resultKey: bullJob.data.dataKey,
      });
    }
    // cleanup
    await deleteKeys(this.redis, [
      `jobs:input:${bullJob.data.dataKey}`,
      ...Object.keys(bullJob.data.childDataKeys ?? []).map((dataKey) => `jobs:result:${dataKey}`)
    ]);
  }

  private addStatusNotifierJob() {
    this.opts.logger.info({ queue: this.opts.queue }, 'adding status notifier job');
    void this.statusNotifierQueue.add('statusNotifier', undefined, {
      jobId: `${this.opts.statusNotifierQueue}/${this.opts.queue}`,
      repeat: {
        every: this.opts.statusNotifierRepeatMs,
      },
    }).catch(async (err) => {
      this.opts.logger.error({
        err,
        queue: this.opts.queue
      }, 'adding status notifier job: error adding job to queue, waiting 5s before retrying');
      // retry after a while
      await timeout(5000);
      this.addStatusNotifierJob();
    });
  }

  processStatusNotifierJobs() {
    this.opts.logger.info({ queue: this.opts.statusNotifierQueue }, 'status notifier: start');
    const worker = new BullWorker(
      this.opts.statusNotifierQueue,
      async () => {
        try {
          await this.notifyStatusListeners();
        } catch (err) {
          this.opts.logger.error({
            err,
            queue: this.opts.statusNotifierQueue
          }, 'status notifier: error');
        }
      },
      {
        connection: this.opts.redisConnection(),
        concurrency: 1,
      }
    );
    worker.on('error', (err) => {
      this.opts.logger.error({
        err,
        queue: this.opts.statusNotifierQueue
      }, 'status notifier: unhandled error');
    });
  }

  async notifyStatusListeners() {
    const waiting = await this.queue.getWaitingCount();
    const batchSize = 10;
    if (waiting == 0) {
      return;
    }
    this.opts.logger.info({
      queue: this.opts.queue,
      waiting
    }, `status notifier: sending queueing status`);
    for (let index = 0; index < waiting; index += batchSize) {
      const bullJobs = await this.queue.getWaiting(index, Math.min(index + batchSize, waiting));
      await Promise.all(bullJobs.map(async (bullJob, subIndex) => {
        const jobId = bullJob.id;
        if (!jobId) {
          this.opts.logger.warn({
            queue: this.opts.queue,
            jobId
          }, 'job has no id');
          return;
        }
        await this.publishStatus(bullJob.data.dataKey, {
          jobId,
          type: 'queued',
          meta: bullJob.data.job.meta,
          waiting: index + subIndex,
        });
      }));
    }
  }

  async publishStatus(dataKey: string, status: JobStatus<Meta, ProgressInfo>): Promise<void> {
    await this.redis.publish(`jobs:status:${dataKey}`, pack(status));
  }
}

export class BullMqJobQueueEngine<Input, Output, Meta, ProgressInfo> implements JobQueueEngine<Input, Output, Meta, ProgressInfo> {
  private queues: Record<string, BullMqJobQueue<Input, Output, Meta, ProgressInfo>> = {};
  private redis: Redis;
  private subRedis: Redis;
  private opts: Opts;

  constructor(opts: ConstructorOpts) {
    this.opts = {
      redisConnection: defaultRedisConnection,
      blockingTimeoutSecs: 8,
      statusNotifierRepeatMs: 2000,
      concurrency: 1,
      fallback: false,
      logger: defaultLogger,
      maximumWaitTimeoutMs: 60*60*24*365*1000,
      ...opts,
    };
    this.redis = (opts.redisConnection ?? defaultRedisConnection)(); 
    this.subRedis = (opts.redisConnection ?? defaultRedisConnection)(); 
  }

  private getQueue(queue: string): BullMqJobQueue<Input, Output, Meta, ProgressInfo> {
    if (!this.queues[queue]) {
      this.queues[queue] = new BullMqJobQueue<Input, Output, Meta, ProgressInfo>({
        ...this.opts,
        queue,
        redis: this.redis,
      });
    }
    return this.queues[queue];
  }

  async submitJob(opts: {
    data: JobData<Input, Meta>,
    queue: string,
    parentId?: string,
    parentQueue?: string,
    dataKey?: string,
    statusHandler?: (status: JobStatus<Meta, ProgressInfo>) => void,
  }): Promise<void> {
    const dataKey = opts.dataKey ?? uuidv4();
    if (opts.statusHandler) {
      await this.subscribeToJobStatusWithDataKey(dataKey, opts.statusHandler);
    }
    await this.getQueue(opts.queue).submitJob({ ...opts, dataKey });
  }

  async subscribeToJobStatus(opts: {
    jobId: string,
    queue: string,
    statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<() => Promise<void>> {    
    const bullJob = await this.getQueue(opts.queue)._getBullQueue()?.getJob(opts.jobId);
    if (!bullJob) {
      throw Error('job not found');
    }
    return this.subscribeToJobStatusWithDataKey(bullJob.data.dataKey, opts.statusHandler);
  }

  private async subscribeToJobStatusWithDataKey(
    dataKey: string,
    statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void
  ): Promise<() => Promise<void>> {

    const subscriptionKey = `jobs:status:${dataKey}`;
    await this.subRedis.subscribe(subscriptionKey);
    // TODO: should create an EventEmitter to dispatch messages locally
    const handler = (channel: Buffer, payload: Buffer) => {
      const channelString = channel.toString();
      if (channelString === subscriptionKey) {
        const status = unpack(payload) as JobStatus<Meta, ProgressInfo>;
        if (isResultStatus(status.type)) {
          unsub();
        }
        statusHandler(status);
      }
    };
    this.subRedis.on("messageBuffer", handler);
    
    const unsub = async () => {
      await this.subRedis.unsubscribe(subscriptionKey);
      this.subRedis.off("messageBuffer", handler);
    };
    return unsub;
  }

  async acquireJob(opts: {
    queue: string,
    token: string,
    block?: boolean
  }): Promise<JobData<Input, Meta> | undefined> {
    return await this.getQueue(opts.queue).acquireJob({
      token: opts.token,
      block: opts.block
    });
  }

  async completeJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    result: JobResult<Output>
  }): Promise<void> {
    await this.getQueue(opts.queue).completeJob(opts);
  }

  async updateJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    lockTimeoutMs?: number,
    progressInfo?: ProgressInfo,
    input?: Input,
  }): Promise<{ interrupt: boolean }> {
    return await this.getQueue(opts.queue).updateJob(opts);
  }

  async getJobResult(opts: { resultKey: string, delete?: boolean }): Promise<JobResult<Output>> {
    const buffer = (
      opts.delete
        ? await this.redis.getdelBuffer(`jobs:result:${opts.resultKey}`)
        : await this.redis.getBuffer(`jobs:result:${opts.resultKey}`)
    );
    if (!buffer) {
      this.opts.logger.error({
        resultKey: opts.resultKey
      }, 'process job: no result data, probably timed out');
      return { type: 'error', reason: 'process job: no result data' } satisfies JobResult<Output>;
    }
    return unpack(buffer) as JobResult<Output>;
  }

  private async getChildResults(opts: { childDataKeys: Record<string, string>, delete?: boolean }) {
    let request = this.redis.multi();
    const entries = Object.entries(opts.childDataKeys);
    for (const [_childId, dataKey] of entries) {
      request = (
        opts.delete
          ? request.getdelBuffer(`jobs:result:${dataKey}`)
          : request.getBuffer(`jobs:result:${dataKey}`)
      );
    }
    const results = await request.exec();
    const resultMap = Object.fromEntries(
      entries.map(([childId], i) => [childId, results?.[i]?.[1] as Buffer | undefined])
    );
    if (Object.values(resultMap).some((buffer) => buffer == undefined)) {
      return undefined;
    }
    return mapValues(resultMap, (buffer) => unpack(buffer!) as JobResult<Output>);
  }

  // TODO separate getChildResults from submitChildrenSuspendParent
  async submitChildrenSuspendParent(opts: { 
    children: { data: JobData<Input, Meta>, queue: string }[], 
    token: string,
    parentId: string,
    parentQueue: string,
  }): Promise<Record<string, JobResult<unknown>> | undefined> {

    const bullJob = await this.getQueue(opts.parentQueue)._getBullQueue()?.getJob(opts.parentId);
    if (!bullJob) {
      throw Error('parent job not found');
    }

    const childDataKeys = Object.fromEntries(
      opts.children.map(({ data: { id } } ) => [id, `${bullJob.data.dataKey}:child:${id}`])
    );

    if (isSubset(childDataKeys, bullJob.data.childDataKeys ?? {})) {
      const childResults = await this.getChildResults({ childDataKeys });
      if (childResults) {
        return childResults;
      }
      const shouldWait = await bullJob.moveToWaitingChildren(opts.token);
      if (shouldWait) {
        return undefined;
      }
      this.opts.logger.warn({
        parentId: opts.parentId,
        parentQueue: opts.parentQueue,
        childrenCount: opts.children.length
      }, 'continue parent: parent job has existing childDataKeys but children are not running, retrying');
    }

    await Promise.all(opts.children.map((child) => {
      const dataKey = childDataKeys[child.data.id];
      assert(dataKey);
      this.submitJob({
        ...child,
        parentId: opts.parentId,
        parentQueue: opts.parentQueue,
        dataKey,
      });
    }));

    await bullJob.updateData({
      ...bullJob.data,
      childDataKeys: { ...bullJob.data.childDataKeys, ...childDataKeys },
    });

    const shouldWait = await bullJob.moveToWaitingChildren(opts.token);
    if (!shouldWait) {
      const childResults = await this.getChildResults({ childDataKeys });
      if (!childResults) {
        this.opts.logger.error({
          parentId: opts.parentId,
          parentQueue: opts.parentQueue,
          childrenCount: opts.children.length
        }, 'suspending parent: children are done but outputs are missing');
        throw Error('suspending parent: children are done but outputs are missing');
      }
      return childResults;
    }

    this.opts.logger.info({
      parentId: opts.parentId,
      parentQueue: opts.parentQueue,
      childrenCount: opts.children.length
    }, 'parent job suspended waiting for children');
    return undefined;
  }
}
