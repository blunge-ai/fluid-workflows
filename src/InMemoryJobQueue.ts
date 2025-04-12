import { JobQueue, Job, JobResult, JobStatus, JobActiveStatus } from './JobQueue';
import { v4 as uuidv4 } from 'uuid';

type InternalJobData<Input, Meta, ProgressInfo> = {
  job: Job<Input, Meta>,
  outputKey: string,
  status: JobStatus<Meta, ProgressInfo>
};

export class InMemoryJobQueue<Input = unknown, Output = unknown, Meta = unknown, ProgressInfo = unknown> implements JobQueue<Input, Output, Meta, ProgressInfo> {
  private queue: InternalJobData<Input, Meta, ProgressInfo>[] = [];
  private inProgress: InternalJobData<Input, Meta, ProgressInfo>[] = [];
  private subscriptions: Record<string, (status: JobStatus<Meta, ProgressInfo>) => void> = {};
  private getters: ((jobData: InternalJobData<Input, Meta, ProgressInfo> | undefined) => void)[] = [];
  private results: Record<string, JobResult<Output>> = {};

  constructor(
    public name: string
  ) {}

  async enqueueJob(job: Job<Input, Meta>) {
    const jobData = {
      job,
      outputKey: uuidv4(),
      status: {
        type: 'queued',
        uniqueId: job.uniqueId,
        meta: job.meta,
        info: { waiting: this.queue.length },
      }
    } satisfies InternalJobData<Input, Meta, ProgressInfo>;
    const queuedGetter = this.getters.shift();
    if (queuedGetter) {
      queuedGetter(jobData);
    } else {
      this.queue.push(jobData);
    }
    return { outputKey: jobData.outputKey };
  }

  async getResult({ uniqueId, outputKey }: { uniqueId: string, outputKey: string }) {
    const result = this.results[outputKey];
    if (!result) {
      throw Error('no result');
    }
    return result;
  }

  async subscribe(uniqueId: string, statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void) {
    if (this.subscriptions[uniqueId]) {
      throw Error('multiple subscriptions not implemented');
    }
    this.subscriptions[uniqueId] = statusHandler;
    return async () => {
      delete this.subscriptions[uniqueId];
    };
  }

  async getNextJob(opts: { token: string; block?: boolean }): Promise<Job<Input, Meta> | undefined> {
    let next = this.queue.shift();
    if (!next) {
      if (!opts.block) {
        return undefined;
      }
      next = await new Promise<InternalJobData<Input, Meta, ProgressInfo> | undefined>((resolve) => {
        this.getters.push(resolve);
      });
      if (!next) {
        return undefined;
      }
    }
    this.inProgress.push(next);
    return next.job;
  }

  releaseBlockedCalls() {
    const getters = this.getters;
    this.getters = [];
    for (const getter of getters) {
      void getter(undefined);
    }
  }

  async updateStatus(opts: {
    token: string,
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'>,
    lockTimeoutMs?: number,
  }): Promise<{ interrupt: boolean }> {
    const jobData = this.inProgress.find((data) => data.job.uniqueId === opts.status.uniqueId);
    if (!jobData) {
      return { interrupt: true };
    }
    jobData.status = { ...opts.status, meta: jobData.job.meta };
    this.subscriptions[opts.status.uniqueId]?.({
      uniqueId: opts.status.uniqueId,
      type: opts.status.type,
      meta: jobData.job.meta,
      info: opts.status.info
    });
    return { interrupt: false };
  }

  async updateJob(job: Pick<Job<Input, unknown>, 'uniqueId' | 'input'>): Promise<void> {
    const jobData = this.inProgress.find((data) => data.job.uniqueId === job.uniqueId);
    if (!jobData) {
      return;
    }
    jobData.job = { ...jobData.job, input: job.input };
  }

  async completeJob(opts: { token: string, uniqueId: string, result: JobResult<Output> }): Promise<void> {
    const jobData = this.inProgress.find((data) => data.job.uniqueId === opts.uniqueId);
    if (!jobData) {
      return;
    }
    this.inProgress = this.inProgress.filter((data) => data !== jobData);
    this.results[jobData.outputKey] = opts.result;
    this.subscriptions[opts.uniqueId]?.({
      uniqueId: opts.uniqueId,
      type: opts.result.type,
      meta: jobData.job.meta
    });
  }
  
}
