import { v4 as uuidv4 } from 'uuid';

import { Worker as BullWorker, Queue as BullQueue } from 'bullmq';
import Redis from 'ioredis';
import { pack, unpack } from './packer';

import type { JobQueueEngine, JobData, JobStatus, JobResult, JobResultStatus, JobActiveStatus } from './JobQueue';
import { defaultRedisConnection, timeout, Logger, defaultLogger } from './utils';

const bullWorkerBlockingTimeoutSecs = 2;
const fallbackDelayMs = 200;

export type Opts<Meta, ProgressInfo> = {
  queueName: string,
  attempts: number,
  lockTimeoutMs: number,

  blockingTimeoutSecs: number,
  statusNotifierQueueName: string,
  statusNotifierRepeatMs: number,
  redisConnection: () => Redis,
  concurrency: number,
  fallback: boolean,
  logger: Logger,
  maximumWaitTimeoutMs: number,

  publishStatus?: (status: JobStatus<Meta, ProgressInfo>) => Promise<void>,
  receiveResult?: (job: { uniqueId: string, meta: Meta }, timeoutMs: number) => Promise<JobResultStatus<Meta>>,
};

export type ConstructorOpts<Meta, ProgressInfo>
 = Partial<Opts<Meta, ProgressInfo>>
 & Pick<Opts<Meta, ProgressInfo>, 'queueName' | 'attempts' | 'lockTimeoutMs'>;

type InternalJobData<Input, Meta> = {
  job: JobData<Input, Meta>,
  inputKey: string,
  outputKey: string,
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

export class BullMqJobQueue<Input, Output, Meta, ProgressInfo> implements JobQueueEngine<Input, Output, Meta, ProgressInfo> {
  public name: string;

  private redis: Redis;
  private queue: BullQueue<InternalJobData<Input, Meta>, JobResult<Output>>;
  private worker: BullWorker<InternalJobData<Input, Meta>, JobResult<Output>>;
  private statusNotifierQueue: BullQueue;
  private opts: Opts<Meta, ProgressInfo>;

  constructor(opts: ConstructorOpts<Meta, ProgressInfo>) {
    this.opts = {
      redisConnection: defaultRedisConnection,
      blockingTimeoutSecs: 8,
      statusNotifierQueueName: `${opts.queueName}/status-notifier`,
      statusNotifierRepeatMs: 2000,
      concurrency: 1,
      fallback: false,
      logger: defaultLogger,
      maximumWaitTimeoutMs: 60*60*24*365*1000,
      ...opts,
    };
    this.name = opts.queueName;
    this.redis = this.opts.redisConnection();
    this.queue = bullQueue(this.opts.queueName, this.redis);
    // TODO worker is not always necessary; lazy init?
    this.worker = bullWorker(
      this.opts.queueName,
      this.opts.redisConnection(),
      {
        timeoutMs: this.opts.lockTimeoutMs,
        blockingTimeoutSecs: bullWorkerBlockingTimeoutSecs,
      }
    );
    this.statusNotifierQueue = bullQueue(this.opts.statusNotifierQueueName, this.redis);
    if (opts.publishStatus) {
      this.addStatusNotifierJob();
    }
  }
  
  async startMaintenanceWorkers() {
    void this.worker.startStalledCheckTimer();    
    this.processStatusNotifierJobs();
  }

  async updateJob(opts: {
    token: string,
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'>,
    lockTimeoutMs?: number,
    input?: Input,
  }): Promise<{ interrupt: boolean }> {
    const { token, status, lockTimeoutMs, input } = opts;
    const bullJob = await this.queue.getJob(status.uniqueId);
    if (!bullJob) {
      this.opts.logger.error({
        uniqueId: status.uniqueId,
        queue: this.opts.queueName
      }, 'publish status: job not found');
      throw Error('update status: job not found');
    }
    void bullJob.extendLock(token, lockTimeoutMs ?? this.opts.lockTimeoutMs);
    if (this.opts.publishStatus) {
      const statusWithMeta = { ...status, meta: bullJob.data.job.meta } as JobStatus<Meta, ProgressInfo>;
      void this.opts.publishStatus(statusWithMeta);
    }
    
    // If input was provided, update it
    if (input !== undefined) {
      await this._updateJobInput({ uniqueId: status.uniqueId, input });
    }
    
    return { interrupt: false };
  }

  private async _updateJobInput(job: Pick<JobData<Input, Meta>, 'uniqueId' | 'input'>) {
    const bullJob = await this.queue.getJob(job.uniqueId);
    if (!bullJob) {
      this.opts.logger.error({
        uniqueId: job.uniqueId,
        queue: this.opts.queueName
      }, 'update job: job not found');
      throw Error('update job: job not found');
    }
    await this.redis.set(bullJob.data.inputKey, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
    return { interrupt: false };
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
      if (this.opts.fallback) {
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

      const uniqueId = bullJob.id;
      if (uniqueId == undefined) {
        this.opts.logger.error({
          queue: this.opts.queueName
        }, 'get next job: job has no id');
        await bullJob.moveToCompleted({ type: 'error' }, token, false);
        continue;
      }
      let input: Input | undefined = bullJob.data.job.input;
      const buffer = await this.redis.getBuffer(bullJob.data.inputKey);
      if (!buffer) {
        this.opts.logger.warn({
          uniqueId,
          queue: this.opts.queueName,
        }, `get next job: missing job data, probably timed out`);
        await this.completeJob({ token, uniqueId, result: { type: 'error' } });
        continue;
      }
      input = unpack(buffer) as Input;
      this.opts.logger.info({ uniqueId, queue: this.opts.queueName }, `get next job: processing job`);
      return { ...bullJob.data.job, input } satisfies JobData<Input, Meta>;
    }
    return undefined;
  }

  async completeJob(opts: { 
    token: string, 
    jobId: string, 
    result: JobResult<Output> 
  }): Promise<void> {
    const { token, result } = opts;
    const uniqueId = opts.jobId;
    const job = await this.queue.getJob(uniqueId);
    if (!job) {
      this.opts.logger.warn({
        uniqueId,
        queue: this.opts.queueName
      }, `complete job: no such job, probably timed out`);
      return;
    }
    await this.redis.set(job.data.outputKey, pack(result), 'PX', this.opts.maximumWaitTimeoutMs);
    result = { ...result, output: undefined } as JobResult<Output>;
    if (result.type === 'error') {
      this.opts.logger.info({
        uniqueId,
        queue: this.opts.queueName,
      }, 'complete job: completed with error status');
      await job.moveToFailed(new Error('job completed with error status'), token, false);
    } else {
      await job.moveToCompleted(result, token, false);
    }
    if (this.opts.publishStatus) {
      void this.opts.publishStatus({
        uniqueId,
        type: result.type,
        meta: job.data.job.meta
      });
    }
  }

  private addStatusNotifierJob() {
    this.opts.logger.info({ queue: this.opts.queueName }, 'adding status notifier job');
    void this.statusNotifierQueue.add('statusNotifier', undefined, {
      jobId: `${this.opts.statusNotifierQueueName}/${this.opts.queueName}`,
      repeat: {
        every: this.opts.statusNotifierRepeatMs,
      },
    }).catch(async (err) => {
      this.opts.logger.error({
        err,
        queue: this.opts.queueName
      }, 'adding status notifier job: error adding job to queue, waiting 5s before retrying');
      // retry after a while
      await timeout(5000);
      this.addStatusNotifierJob();
    });
  }

  processStatusNotifierJobs() {
    this.opts.logger.info({ queue: this.opts.statusNotifierQueueName }, 'status notifier: start');
    const worker = new BullWorker(
      this.opts.statusNotifierQueueName,
      async () => {
        try {
          await this.notifyStatusListeners();
        } catch (err) {
          this.opts.logger.error({
            err,
            queue: this.opts.statusNotifierQueueName
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
        queue: this.opts.statusNotifierQueueName
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
      queue: this.opts.queueName,
      waiting
    }, `status notifier: sending queueing status`);
    for (let index = 0; index < waiting; index += batchSize) {
      const jobs = await this.queue.getWaiting(index, Math.min(index + batchSize, waiting));
      await Promise.all(jobs.map(async (job, subIndex) => {
        const uniqueId = job.id;
        if (!uniqueId) {
          console.error('job has no id');
          return;
        }
        if (this.opts.publishStatus) {
          await this.opts.publishStatus({
            uniqueId,
            type: 'queued',
            meta: job.data.job.meta,
            info: { waitingFor: index + subIndex },
          });
        }
      }));
    }
  }

  async submitJob(opts: { data: JobData<Input, Meta>, queue: string }): Promise<void> {
    const job = opts.data;
    const dataKey = uuidv4();
    const jobData: InternalJobData<Input, Meta> = {
      job,
      inputKey: `jobs:input:${dataKey}`,
      outputKey: `jobs:output:${dataKey}`,
    };
    // TODO remove inputKey when job is finished
    //void this.redis.del(jobData.inputKey);

    jobData.job = { ...job, input: undefined as Input };
    await this.redis.set(jobData.inputKey, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
    const [waitingFor] = await Promise.all([
      this.queue.getWaitingCount(),
      this.queue.add('job', jobData, { jobId: job.uniqueId })
    ]);
    if (this.opts.publishStatus) {
      void this.opts.publishStatus({
        uniqueId: job.uniqueId,
        type: 'queued',
        meta: job.meta,
        info: { waitingFor }
      });
    }
  }

  async submitChildrenSuspendParent(opts: { 
    children: { data: JobData<Input, Meta>, queue: string }[], 
    parentId: string 
  }): Promise<void> {
    //TODO
    throw Error('not implemented')
  }

  async getJobResult(opts: { outputKey: string, delete?: boolean }): Promise<JobResult<Output>> {
    const outputKey = opts.outputKey;
    const buffer = await this.redis.getdelBuffer(outputKey);
    if (!buffer) {
      this.opts.logger.warn({
        outputKey: outputKey,
        queue: this.opts.queueName,
      }, 'process job: no result data, probably timed out');
      return { type: 'error' } satisfies JobResult<Output>;
    }
    return unpack(buffer) as JobResult<Output>;
  }

  async subscribeToJobStatus(jobId: string, statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void): Promise<() => Promise<void>> {
    if (!this.opts.receiveResult || !this.opts.publishStatus) {
      throw Error('need receiveResult and publishStatus queue options to receive an event when a job has completed');
    }
    // TODO
    return async () => {
      // Unsubscribe logic
    };
  }
}
