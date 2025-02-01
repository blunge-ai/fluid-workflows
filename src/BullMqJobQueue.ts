import { v4 as uuidv4 } from 'uuid';

import { Worker as BullWorker, Queue as BullQueue } from 'bullmq';
import Redis from 'ioredis';
import { pack, unpack } from './packer';

import type { JobQueue, Job, JobStatus, JobResult, JobResultStatus, JobActiveStatus } from './JobQueue';
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

type JobData<Input, Meta> = {
  job: Job<Input, Meta>,
  inputKey: string,
  outputKey: string,
};

type StatusNotifierJobData = {
  queue: string
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

export class BullMqJobQueue<Input, Output, Meta, ProgressInfo> implements JobQueue<Input, Output, Meta, ProgressInfo> {
  public name: string;

  private redis: Redis;
  private queue: BullQueue<JobData<Input, Meta>, JobResult<Output>>;
  private worker: BullWorker<JobData<Input, Meta>, JobResult<Output>>;
  private statusNotifierQueue: BullQueue<StatusNotifierJobData>;
  private opts: Opts<Meta, ProgressInfo>;

  constructor(opts: ConstructorOpts<Meta, ProgressInfo>) {
    this.opts = {
      redisConnection: defaultRedisConnection,
      blockingTimeoutSecs: 8,
      statusNotifierQueueName: 'status-notifier',
      statusNotifierRepeatMs: 2000,
      concurrency: 1,
      fallback: false,
      logger: defaultLogger,
      maximumWaitTimeoutMs: 60*60*24*1000,
      ...opts,
    };
    this.name = opts.queueName;
    this.redis = this.opts.redisConnection();
    this.queue = bullQueue(this.opts.queueName, this.redis);
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
  
  async runStalledChecker() {
    await this.worker.startStalledCheckTimer();
  }

  async updateStatus({
    token,
    status,
    lockTimeoutMs
  }: {
    token: string,
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'>,
    lockTimeoutMs?: number,
  }) {
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
    return { interrupt: false };
  }

  async updateJob(job: Pick<Job<Input, Meta>, 'uniqueId' | 'input'>) {
    const bullJob = await this.queue.getJob(job.uniqueId);
    if (!bullJob) {
      this.opts.logger.error({
        uniqueId: job.uniqueId,
        queue: this.opts.queueName
      }, 'update job: job not found');
      throw Error('update job: job not found');
    }
    await this.redis.set(bullJob.data.inputKey, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
  }

  async getNextJob({ token, block }: { token: string; block?: boolean }) {
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
      return { ...bullJob.data.job, input } satisfies Job<Input, Meta>;
    }
    return undefined;
  }

  async completeJob({ token, uniqueId, result }: {
    token: string,
    uniqueId: string,
    result: JobResult<Output>,
  }) {
    const job = await this.queue.getJob(uniqueId);
    if (!job) {
      this.opts.logger.warn({
        uniqueId,
        queue: this.opts.queueName
      }, `complete job: no such job, probably timed out`);
      return;
    }
    if (job.data.outputKey != undefined) {
      await this.redis.set(job.data.outputKey, pack(result), 'PX', this.opts.maximumWaitTimeoutMs);
      result = { ...result, output: undefined } as JobResult<Output>;
    }
    if (result.type === 'error') {
      this.opts.logger.warn({
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
    const jobData = { queue: this.opts.queueName } satisfies StatusNotifierJobData;
    this.opts.logger.info({ queue: this.opts.queueName }, 'adding status notifier job');
    void this.statusNotifierQueue.add('statusNotifier', jobData, {
      jobId: `${this.opts.statusNotifierQueueName}/${this.opts.queueName}`,
      attempts: Number.MAX_SAFE_INTEGER,
      backoff: {
        type: 'fixed',
        delay: 10000,
      },
      repeat: {
        every: this.opts.statusNotifierRepeatMs,
      },
    }).catch(async (err) => {
      this.opts.logger.error({
        err,
        queue: this.opts.queueName
      }, 'adding status notifier job: error adding job to queue');
      // retry after a while
      await timeout(5000);
      this.addStatusNotifierJob();
    });
  }

  static processStatusNotifierJobs<Input, Output, Meta, ProgressInfo>({
    statusQueue,
    concurrency,
    getQueue,
    logger = defaultLogger,
    redisConnection = defaultRedisConnection,
  }: {
    statusQueue: string,
    concurrency: number,
    getQueue: (queue: string) => BullMqJobQueue<Input, Output, Meta, ProgressInfo>,
    logger: Logger,
    redisConnection: () => Redis,
  }) {
    logger.info({ queue: statusQueue }, 'status notifier: start');
    const worker = new BullWorker<StatusNotifierJobData>(
      statusQueue,
      async (job) => {
        try {
          const { queue } = job.data;
          await getQueue(queue).notifyStatusListeners();
        } catch (err) {
          logger.error({ err, queue: statusQueue }, 'status notifier: error');
          throw err;
        }
      },
      {
        connection: redisConnection(),
        concurrency,
      }
    );
    worker.on('error', (err) => {
      logger.error({ err, queue: statusQueue }, 'status notifier: unhandled error');
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
            info: { waiting: index + subIndex },
          });
        }
      }));
    }
  }

  async enqueueJob(job: Job<Input, Meta>) {
    const dataKey = uuidv4();
    const jobData: JobData<Input, Meta> = {
      job,
      inputKey: `jobs:input:${dataKey}`,
      outputKey: `jobs:output:${dataKey}`,
    };
    jobData.job = { ...job, input: undefined as Input };
    await this.redis.set(jobData.inputKey, pack(job.input), 'PX', this.opts.maximumWaitTimeoutMs);
    const [waiting] = await Promise.all([
      this.queue.getWaitingCount(),
      this.queue.add('job', jobData, { jobId: job.uniqueId })
    ]);
    if (this.opts.publishStatus) {
      void this.opts.publishStatus({
        uniqueId: job.uniqueId,
        type: 'queued',
        meta: job.meta,
        info: { waiting }
      });
    }
    return jobData;
  }

  async processJob(job: Job<Input, Meta>): Promise<JobResult<Output>> {
    if (!this.opts.receiveResult || !this.opts.publishStatus) {
      throw Error('need receiveResult and publishStatus queue options to receive an event when a job has completed');
    }
    const resultStatusPromise = this.opts.receiveResult(job, this.opts.maximumWaitTimeoutMs);
    // TODO look up the job and connect to the existing job
    const jobData = await this.enqueueJob(job);
    const resultStatus = await resultStatusPromise;
    const buffer = await this.redis.getdelBuffer(jobData.outputKey);
    void this.redis.del(jobData.inputKey);
    if (resultStatus == undefined) {
      this.opts.logger.error({
        uniqueId: job.uniqueId,
        queue: this.opts.queueName,
      }, 'process job: job timed out');
      return { type: 'error' } satisfies JobResult<Output>;
    }
    if (!buffer) {
      this.opts.logger.error({
        uniqueId: job.uniqueId,
        queue: this.opts.queueName,
        status: resultStatus,
      }, 'process job: no result data, probably timed out');
      return { type: 'error' } satisfies JobResult<Output>;
    }
    const result = unpack(buffer) as JobResult<Output>;
    if (result.type !== resultStatus.type) {
      this.opts.logger.error({
        uniqueId: job.uniqueId,
        queue: this.opts.queueName,
        separatelyStoredResultStatus: result.type,
        resultStatus,
      }, 'process job: received result status does not match job result status');
      return { type: 'error' } satisfies JobResult<Output>;
    }
    return result;
  }
}
