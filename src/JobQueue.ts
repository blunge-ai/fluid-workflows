
export type JobResultType = 'cancelled' | 'success' | 'error';
export type JobStatusType = 'queued' | 'active' | JobResultType;
export type QueuedStatusInfo = { waiting: number };

export type JobResultStatus<Meta> = {
  type: JobResultType,
  uniqueId: string,
  meta: Meta,
};

export type JobQueuedStatus<Meta> = {
  type: 'queued',
  uniqueId: string,
  meta: Meta,
  info: QueuedStatusInfo,
};

export type JobActiveStatus<Meta, ProgressInfo> = {
  type: 'active',
  uniqueId: string,
  meta: Meta,
  info: ProgressInfo,
};

export type JobStatus<Meta, ProgressInfo>
  = JobQueuedStatus<Meta>
  | JobActiveStatus<Meta, ProgressInfo>
  | JobResultStatus<Meta>;

export type JobResult<Output> = {
  type: 'success',
  output: Output
} | {
  type: Exclude<JobResultType, 'success'>,
  output?: Output,
};

export type Job<Input, Meta = unknown> = {
  uniqueId: string,
  meta: Meta,
  input: Input,
};

export interface JobQueue<Input, Output, Meta, ProgressInfo> {
  name: string,
  processJob(job: Job<Input, Meta>): Promise<JobResult<Output>>;
  getNextJob(opts: { token: string; block?: boolean }): Promise<Job<Input, Meta> | undefined>;
  updateStatus(opts: {
    token: string,
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'>,
    lockTimeoutMs?: number,
  }): Promise<{ interrupt: boolean }>;
  updateJob(job: Pick<Job<Input, unknown>, 'uniqueId' | 'input'>): Promise<void>;
  completeJob(opts: { token: string, uniqueId: string, result: JobResult<Output> }): Promise<void>;
}
