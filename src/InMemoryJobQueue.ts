import { JobQueue, Job, JobResult, JobStatus, JobActiveStatus } from './JobQueue';

type InternalJobData<Input, Output, Meta, ProgressInfo> = {
  job: Job<Input, Meta>,
  resolve: (result: JobResult<Output>) => void,
  status: JobStatus<Meta, ProgressInfo>
};

export class InMemoryJobQueue<Input = unknown, Output = unknown, Meta = unknown, ProgressInfo = unknown> implements JobQueue<Input, Output, Meta, ProgressInfo> {
  private queue: InternalJobData<Input, Output, Meta, ProgressInfo>[] = [];
  private inProgress: InternalJobData<Input, Output, Meta, ProgressInfo>[] = [];
  private getters: ((jobData: InternalJobData<Input, Output, Meta, ProgressInfo>) => void)[] = [];

  constructor(
    public name: string
  ) {}

  async _enqueueJob(job: Job<Input, Meta>) {
    return await new Promise<JobResult<Output>>((resolve) => {
      const jobData = {
        job,
        resolve,
        status: {
          type: 'queued',
          uniqueId: job.uniqueId,
          meta: job.meta,
          info: { waiting: this.queue.length },
        }
      } satisfies InternalJobData<Input, Output, Meta, ProgressInfo>;
      const queuedGetter = this.getters.shift();
      if (queuedGetter) {
        queuedGetter(jobData);
      } else {
        this.queue.push(jobData);
      }
    });
  }

  async enqueueJob(job: Job<Input, Meta>): Promise<void> {
    await this._enqueueJob(job);
  }

  async processJob(job: Job<Input, Meta>): Promise<JobResult<Output>> {
    return await this._enqueueJob(job);
  }

  async getNextJob(opts: { token: string; block?: boolean }): Promise<Job<Input, Meta> | undefined> {
    let next = this.queue.shift();
    if (!next) {
      if (!opts.block) {
        return undefined;
      }
      next = await new Promise<InternalJobData<Input, Output, Meta, ProgressInfo>>((resolve) => {
        this.getters.push(resolve);
      });
    }
    this.inProgress.push(next);
    return next.job;
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
    jobData.resolve(opts.result);
  }
  
}
