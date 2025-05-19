import { v4 as uuidv4 } from 'uuid';

import { Worker as BullWorker, Queue as BullQueue } from 'bullmq';
import Redis from 'ioredis';
import { pack, unpack } from './packer';

import type { JobQueueEngine, JobData, JobStatus, JobResult, JobActiveStatus } from './JobQueue';
import { defaultRedisConnection, timeout, Logger, defaultLogger } from './utils';

const bullWorkerBlockingTimeoutSecs = 2;
const fallbackDelayMs = 200;

export type Opts = {
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
};

export type ConstructorOpts
 = Partial<Opts>
 & Pick<Opts, 'queueName' | 'attempts' | 'lockTimeoutMs'>;

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

export class BullMqJobQueue<Input, Output, Meta, ProgressInfo> {
  public name: string;

  private redis: Redis;
  private queue: BullQueue<InternalJobData<Input, Meta>, JobResult<Output>>;
  private statusNotifierQueue: BullQueue;
  private opts: Opts;

  private _worker: BullWorker<InternalJobData<Input, Meta>, JobResult<Output>> | undefined = undefined;
  private get worker() {
    if (this._worker == undefined) {
      this._worker = bullWorker(
        this.opts.queueName,
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
  
  async startMaintenanceWorkers() {
    void this.worker.startStalledCheckTimer();    
    this.addStatusNotifierJob();
    this.processStatusNotifierJobs();
  }

  constructor(opts: ConstructorOpts) {
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
    this.statusNotifierQueue = bullQueue(this.opts.statusNotifierQueueName, this.redis);
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
        queue: this.opts.queueName
      }, 'publish status: job not found');
      throw Error('update status: job not found');
    }
    void bullJob.extendLock(token, lockTimeoutMs ?? this.opts.lockTimeoutMs);
    if (progressInfo != undefined) {
      await this.publishStatus({
        jobId,
        type: 'active',
        meta: bullJob.data.job.meta,
        info: progressInfo,
      });
    }
    if (input !== undefined) {
      await this.redis.set(bullJob.data.inputKey, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
    }
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
      const jobId = bullJob.id;
      if (jobId == undefined) {
        this.opts.logger.error({
          queue: this.opts.queueName
        }, 'get next job: job has no id');
        await bullJob.moveToCompleted({
          type: 'error',
          reason: 'get next job: job has no id'
        }, token, false);
        continue;
      }
      let input: Input | undefined = bullJob.data.job.input;
      const buffer = await this.redis.getBuffer(bullJob.data.inputKey);
      if (!buffer) {
        this.opts.logger.warn({
          jobId,
          queue: this.opts.queueName,
        }, `get next job: missing job data, probably timed out`);
        await this.completeJob({
          token,
          jobId,
          result: { type: 'error', reason: 'get next job: missing job data' }
        });
        continue;
      }
      input = unpack(buffer) as Input;
      this.opts.logger.info({ jobId, queue: this.opts.queueName }, `get next job: processing job`);
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
    const jobId = opts.jobId;
    const job = await this.queue.getJob(jobId);
    if (!job) {
      this.opts.logger.warn({
        jobId,
        queue: this.opts.queueName
      }, `complete job: no such job, probably timed out`);
      return;
    }
    const resultWithoutOutput = {
      ...result,
      output: undefined
    } as JobResult<Output | undefined>;
    if (result.type === 'error') {
      this.opts.logger.info({
        jobId,
        queue: this.opts.queueName,
        reason: result.reason,
      }, 'complete job: completed with error');
      await job.moveToFailed(new Error(`job completed with error: ${result.reason}`), token, false);
      await this.publishStatus({
        jobId,
        type: result.type,
        meta: job.data.job.meta,
        reason: result.reason,
      });
    } else if (result.type === 'cancelled') {
      await job.moveToCompleted(resultWithoutOutput, token, false);
      await this.publishStatus({
        jobId,
        type: result.type,
        meta: job.data.job.meta,
        reason: result.reason,
      });
    } else {
      await this.redis.set(job.data.outputKey, pack(result), 'PX', this.opts.maximumWaitTimeoutMs);
      await job.moveToCompleted(resultWithoutOutput, token, false);
      await this.publishStatus({
        jobId,
        type: result.type,
        meta: job.data.job.meta,
        outputKey: job.data.outputKey,
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
        const jobId = job.id;
        if (!jobId) {
          this.opts.logger.warn({
            queue: this.opts.queueName,
            jobId
          }, 'job has no id');
          return;
        }
        await this.publishStatus({
          jobId,
          type: 'queued',
          meta: job.data.job.meta,
          waiting: index + subIndex,
        });
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
    jobData.job = { ...job, input: undefined as Input };
    await this.redis.set(jobData.inputKey, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
    const [waiting] = await Promise.all([
      this.queue.getWaitingCount(),
      this.queue.add('job', jobData, { jobId: job.id })
    ]);
    await this.publishStatus({
      jobId: job.id,
      type: 'queued',
      meta: job.meta,
      waiting,
    });
  }

  async getJobResult(opts: { outputKey: string, delete?: boolean }): Promise<JobResult<Output>> {
    const outputKey = opts.outputKey;
    const buffer = (
      opts.delete
        ? await this.redis.getdelBuffer(outputKey)
        : await this.redis.getBuffer(outputKey)
    );
    if (!buffer) {
      this.opts.logger.warn({
        outputKey: outputKey,
        queue: this.opts.queueName,
      }, 'process job: no result data, probably timed out');
      return { type: 'error', reason: 'process job: no result data' } satisfies JobResult<Output>;
    }
    return unpack(buffer) as JobResult<Output>;
  }

}

// can you please add the missing functions from JobQueueEngine to this class ai!
class BullMqJobQueueEngine implements JobQueueEngine<Input, Output, Meta, ProgressInfo> {

  async submitChildrenSuspendParent(opts: { 
    children: { data: JobData<Input, Meta>, queue: string }[], 
    parentId: string 
  }): Promise<void> {
    //TODO
    throw Error('not implemented')
  }

  async publishStatus<Meta, ProgressInfo>(status: JobStatus<Meta, ProgressInfo>) {
    //TODO
  }

  async subscribeToJobStatus(jobId: string, statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void): Promise<() => Promise<void>> {
    // TODO
    return async () => {
      // Unsubscribe logic
    };
  }
}
