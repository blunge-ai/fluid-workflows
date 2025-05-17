import { JobQueue, Job, JobResult, JobStatus, JobActiveStatus, JobSleepingStatus, JobQueuedStatus } from './JobQueue';
import { v4 as uuidv4 } from 'uuid';

type InternalJobData<Input, Meta, ProgressInfo> = {
  job: Job<Input, Meta>,
  parentId: string | undefined,
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
    return await this._enqueueJob(job);
  }

  private async _enqueueJob(job: Job<Input, Meta>, parentId?: string) {
    const jobData = {
      job,
      outputKey: uuidv4(),
      parentId,
      status: {
        type: 'queued',
        uniqueId: job.uniqueId,
        meta: job.meta,
        info: { waitingFor: this.queue.length },
      }
    } satisfies InternalJobData<Input, Meta, ProgressInfo>;
    const queuedGetter = this.getters.shift();
    if (queuedGetter) {
      queuedGetter(jobData);
    } else {
      this.queue.push(jobData);
    }
    this._updateStatus({
      status: jobData.status
    });
    return { outputKey: jobData.outputKey };
  }

  async enqueueChildren(children: Job<Input, Meta>[], parentId: string) {
    for (const child of children) {
      await this._enqueueJob(child, parentId);
    }
    this._updateStatus({
      status: {
        type: 'sleeping',
        uniqueId: parentId,
        info: { waitingFor: children.length }
      }
    });
  }

  async getResult(outputKey: string) {
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

  async updateStatus(opts: {
    token: string,
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'>,
    lockTimeoutMs?: number,
  }): Promise<{ interrupt: boolean }> {
    return await this._updateStatus(opts);
  }

  private async _updateStatus(opts: {
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'> | Omit<JobSleepingStatus<Meta>, 'meta'> | Omit<JobQueuedStatus<Meta>, 'meta'>,
    lockTimeoutMs?: number,
  }): Promise<{ interrupt: boolean }> {
    const jobData = this.inProgress.find((data) => data.job.uniqueId === opts.status.uniqueId);
    if (!jobData) {
      return { interrupt: true };
    }
    const status = jobData.status = { ...opts.status, meta: jobData.job.meta };
    this.subscriptions[opts.status.uniqueId]?.(status);
    return { interrupt: false };
  }

  async updateJob(job: Pick<Job<Input, unknown>, 'uniqueId' | 'input'>): Promise<void> {
    const jobData = this.inProgress.find((data) => data.job.uniqueId === job.uniqueId);
    if (!jobData) {
      throw Error('no such job');
    }
    jobData.job = { ...jobData.job, input: job.input };
  }

  async completeJob(opts: { token: string, uniqueId: string, result: JobResult<Output> }): Promise<void> {
    const jobData = this.inProgress.find((data) => data.job.uniqueId === opts.uniqueId);
    if (!jobData) {
      throw Error('no such job');
    }
    this.inProgress = this.inProgress.filter((data) => data !== jobData);
    this.results[jobData.outputKey] = opts.result;
    this.subscriptions[opts.uniqueId]?.({
      uniqueId: opts.uniqueId,
      type: opts.result.type,
      meta: jobData.job.meta
    });
    if (jobData.parentId) {
      const parent = this.inProgress.find((data) => data.parentId === jobData.job.uniqueId);
      if (!parent) {
        throw Error('parent not found');
      }
      if (parent.status.type !== 'sleeping') {
        throw Error('parent is expected to be sleeping');
      }
      if (parent.status.info.waitingFor < 1) {
        throw Error('parent should be waiting for 1 ore more children');
      }
      parent.status.info.waitingFor -= 1;
      await this._updateStatus({ status: parent.status });
      if (parent.status.info.waitingFor === 0) {
        await this._enqueueJob(parent.job, parent.parentId);
      }
    }
  }

  releaseBlockedCalls() {
    const getters = this.getters;
    this.getters = [];
    for (const getter of getters) {
      void getter(undefined);
    }
  }
}
