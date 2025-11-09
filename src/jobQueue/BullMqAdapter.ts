import { v4 as uuidv4 } from 'uuid';

import { Worker as BullWorker, Queue as BullQueue } from 'bullmq';
import Redis from 'ioredis';
import { pack, unpack } from '../packer';

import type { JobQueueEngine, JobData, JobStatus, JobResult } from './JobQueueEngine';
import { isResultStatusType } from './JobQueueEngine';
import { defaultRedisConnection, timeout, Logger, defaultLogger, assert, mapValues } from '../utils';

export type Opts = {
  attempts: number,
  lockTimeoutMs: number,

  blockingTimeoutSecs: number,
  statusNotifierRepeatMs: number,
  redisConnection: () => Redis,
  concurrency: number,
  polling: boolean,
  logger: Logger,
  maximumWaitTimeoutMs: number,

  bullWorkerBlockingTimeoutSecs: number,
  pollingDelayMs: number,
};

export type ConstructorOpts
 = Partial<Opts>
 & Pick<Opts, 'attempts' | 'lockTimeoutMs'>;

export type QueueOpts = Opts & {
  queue: string,
  statusNotifierQueue: string,
  bullWorkerBlockingTimeoutSecs: number,
  pollingDelayMs: number,
};

type InternalJobData<Input, Meta> = {
  job: JobData<Input, Meta>,
  dataKey: string,
  parentId?: string,
  childDataKeys?: Record<string, string>,
  waitingForChildren?: boolean,
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

export class BullMqJobQueue<Input = unknown, Output = unknown, Meta = unknown, ProgressInfo = unknown> {

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
          blockingTimeoutSecs: this.opts.bullWorkerBlockingTimeoutSecs,
        }
      );
      void this.startMaintenanceWorkers();
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

  constructor(opts: Opts & { queue: string, redis: Redis, bullWorkerBlockingTimeoutSecs: number, pollingDelayMs: number }) {
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

  async acquireJob(opts: { token: string, block?: boolean }): Promise<{
    data: JobData<Input, Meta> | undefined,
    childDataKeys?: Record<string, string>
  }> {
    const { token, block } = opts;
    const start = Date.now();
    const forcePolling = this.opts.blockingTimeoutSecs < this.opts.bullWorkerBlockingTimeoutSecs;
    if (forcePolling) {
      this.opts.logger.warn({
        queue: this.opts.queue
      }, 'acquire job: blockingTimeoutSecs is smaller than the minimum, forcing polling mode');
    }
    const polling = this.opts.polling || forcePolling;
    for (let iteration = 0;; iteration++) {
      const elapsedMs = Date.now() - start;
      // Every call to getNextJob blocks for bullWorkerBlockingTimeoutSecs. We can't block exactly
      // blockingTimeoutSecs, but we can prevent another blocking cycle when we have almost finished
      // waiting.
      const marginMs = this.opts.pollingDelayMs;
      const remainingMs = this.opts.blockingTimeoutSecs * 1000 - elapsedMs;
      if (iteration !== 0 && remainingMs - marginMs <= 0) {
        break;
      }
      if (polling && block) {
        await timeout(Math.min(remainingMs, this.opts.pollingDelayMs));
      }
      const bullJob = await this.worker.getNextJob(token, {
        block: !!block && !polling
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
          queue: this.opts.queue,
          iteration,
        }, 'acquire job: job has no id');
        await bullJob.moveToCompleted({
          type: 'error',
          reason: 'acquire job: job has no id'
        }, token, false);
        continue;
      }
      let input: Input | undefined = bullJob.data.job.input;
      const buffer = await this.redis.getBuffer(`jobs:input:${bullJob.data.dataKey}`);
      if (!buffer) {
        this.opts.logger.warn({
          jobId,
          queue: this.opts.queue,
          iteration,
        }, `acquire job: missing job data, probably timed out`);
        await this.completeJob({
          token,
          jobId,
          result: { type: 'error', reason: 'acquire job: missing job data' }
        });
        continue;
      }
      input = unpack(buffer) as Input;
      this.opts.logger.info({
        jobId,
        queue: this.opts.queue,
        iteration
      }, `acquire job: processing job`);
      return {
        data: { ...bullJob.data.job, input } satisfies JobData<Input, Meta>,
        childDataKeys: bullJob.data.childDataKeys
      };
    }
    return { data: undefined };
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
    await bullJob.extendLock(token, lockTimeoutMs ?? this.opts.lockTimeoutMs);
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
    await this.redis.set(`jobs:result:${bullJob.data.dataKey}`, pack(result), 'PX', this.opts.maximumWaitTimeoutMs)
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
      ...Object.keys(bullJob.data.childDataKeys ?? {}).map((dataKey) => `jobs:result:${dataKey}`)
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

export class BullMqAdapter implements JobQueueEngine {
  private queues: Record<string, BullMqJobQueue> = {};
  private redis: Redis;
  private subRedis: Redis;
  private opts: Opts;

  constructor(opts: ConstructorOpts) {
    this.opts = {
      redisConnection: defaultRedisConnection,
      blockingTimeoutSecs: 8,
      statusNotifierRepeatMs: 2000,
      concurrency: 1,
      polling: false,
      logger: defaultLogger,
      maximumWaitTimeoutMs: 60*60*24*365*1000,
      bullWorkerBlockingTimeoutSecs: 2,
      pollingDelayMs: 200,
      ...opts,
    };
    this.redis = (opts.redisConnection ?? defaultRedisConnection)(); 
    this.subRedis = (opts.redisConnection ?? defaultRedisConnection)(); 
  }

  private getQueue<Input, Output, Meta, ProgressInfo>(queue: string): BullMqJobQueue<Input, Output, Meta, ProgressInfo> {
    let bullQueue = this.queues[queue];
    if (!bullQueue) {
      bullQueue = this.queues[queue] = new BullMqJobQueue({
        ...this.opts,
        queue,
        redis: this.redis,
      });
    }
    return bullQueue as BullMqJobQueue<Input, Output, Meta, ProgressInfo>;
  }

  async submitJob<Meta, ProgressInfo>(opts: {
    data: JobData<unknown, Meta>,
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

  async subscribeToJobStatus<Meta, ProgressInfo>(opts: {
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

  private async subscribeToJobStatusWithDataKey<Meta, ProgressInfo>(
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
        if (isResultStatusType(status.type)) {
          void unsub();
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

  async acquireJob<Input, Meta, ChildOutput>(opts: {
    queue: string,
    token: string,
    block?: boolean
  }): Promise<{ data: JobData<Input, Meta> | undefined, childResults?: Record<string, JobResult<ChildOutput>> }> {
    const { data, childDataKeys } = await this.getQueue(opts.queue).acquireJob({
      token: opts.token,
      block: opts.block
    });
    if (!data) {
      return { data };
    }
    if (!childDataKeys) {
      return { data: data as JobData<Input, Meta> };
    }
    const childResults = await this.submitChildrenSuspendParent<ChildOutput>({
      children: [],
      token: opts.token,
      parentId: data.id,
      parentQueue: opts.queue
    })
    if (!childResults) {
      return { data: undefined };
    }
    return { data: data as JobData<Input, Meta>, childResults };
  }

  async completeJob<Output>(opts: {
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
    progressInfo?: unknown,
    input?: unknown,
  }): Promise<{ interrupt: boolean }> {
    return await this.getQueue(opts.queue).updateJob(opts);
  }

  async getJobResult<Output>(opts: { resultKey: string, delete?: boolean }): Promise<JobResult<Output>> {
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

  private async getChildResults<Output>(opts: { childDataKeys: Record<string, string>, delete?: boolean }) {
    let request = this.redis.multi();
    const entries = Object.entries(opts.childDataKeys);
    if (entries.length === 0) {
      return {};
    }
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

  private async deleteChildResults(childDataKeys: Record<string, string>) {
    await deleteKeys(this.redis, [
      ...Object.keys(childDataKeys).map((dataKey) => `jobs:result:${dataKey}`)
    ]);
  }

  // TODO separate getChildResults from submitChildrenSuspendParent
  async submitChildrenSuspendParent<ChildOutput>(opts: { 
    children: { data: JobData<unknown>, queue: string }[], 
    token: string,
    parentId: string,
    parentQueue: string,
  }): Promise<Record<string, JobResult<ChildOutput>> | undefined> {

    const parentBullJob = await this.getQueue(opts.parentQueue)._getBullQueue()?.getJob(opts.parentId);
    if (!parentBullJob) {
      throw Error('parent job not found');
    }

    const childDataKeys = (
      opts.children.length === 0
        ? parentBullJob.data.childDataKeys ?? {}
        : Object.fromEntries(opts.children.map(
          ({ data: { id } } ) => [id, `${parentBullJob.data.dataKey}:child:${id}`]
        ))
    );

    if (isSubset(childDataKeys, parentBullJob.data.childDataKeys ?? {})) {
      // children were already submitted previously, check to see whether we have results
      const childResults = await this.getChildResults<ChildOutput>({ childDataKeys });
      if (childResults) {
        await Promise.all([
          parentBullJob.updateData({ ...parentBullJob.data, childDataKeys: undefined }),
          this.deleteChildResults(childDataKeys)
        ]);
        return childResults;
      }
      // we don't have all results, suspend parent again
      const shouldWait = await parentBullJob.moveToWaitingChildren(opts.token);
      if (shouldWait) {
        return undefined;
      } else {
        const childResults = await this.getChildResults<ChildOutput>({ childDataKeys });
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
    }

    // submit all children
    await Promise.all(opts.children.map((child) => {
      const dataKey = childDataKeys[child.data.id];
      assert(dataKey);
      return this.submitJob({
        ...child,
        parentId: opts.parentId,
        parentQueue: parentBullJob.queueQualifiedName,
        dataKey,
      });
    }));

    await parentBullJob.updateData({
      ...parentBullJob.data,
      childDataKeys: childDataKeys,
    });

    const shouldWait = await parentBullJob.moveToWaitingChildren(opts.token);
    if (!shouldWait) {
      const childResults = await this.getChildResults<ChildOutput>({ childDataKeys });
      if (!childResults) {
        this.opts.logger.error({
          parentId: opts.parentId,
          parentQueue: opts.parentQueue,
          childrenCount: opts.children.length
        }, 'suspending parent: children are done but outputs are missing');
        throw Error('suspending parent: children are done but outputs are missing');
      }
      await Promise.all([
        parentBullJob.updateData({ ...parentBullJob.data, childDataKeys: undefined }),
        this.deleteChildResults(childDataKeys)
      ]);
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
