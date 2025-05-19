
export type JobResultType = 'cancelled' | 'success' | 'error';
export type JobStatusType = 'queued' | 'active' | JobResultType;

export type JobResultStatus<Meta> = {
  type: JobResultType,
  jobId: string,
  meta: Meta,
  outputKey: string,
};

export type JobQueuedStatus<Meta> = {
  type: 'queued',
  jobId: string,
  meta: Meta,
  waiting: number,
};

export type JobActiveStatus<Meta, ProgressInfo> = {
  type: 'active',
  jobId: string,
  meta: Meta,
  info: ProgressInfo,
};

export type JobSuspendedStatus<Meta> = {
  type: 'suspended',
  jobId: string,
  meta: Meta,
  waiting: number,
};

export type JobStatus<Meta, ProgressInfo>
  = JobQueuedStatus<Meta>
  | JobActiveStatus<Meta, ProgressInfo>
  | JobSuspendedStatus<Meta>
  | JobResultStatus<Meta>;

export type JobResult<Output> = {
  type: 'success',
  output: Output | undefined
} | {
  type: Exclude<JobResultType, 'success'>,
  reason: string,
};

export type JobData<Input, Meta = unknown> = {
  id: string,
  meta: Meta,
  input: Input,
};

export interface JobQueueEngine<Input, Output, Meta, ProgressInfo> {
  submitJob(opts: { data: JobData<Input, Meta>, queue: string }): Promise<void>;
  submitChildrenSuspendParent(opts: { children: { data: JobData<Input, Meta>, queue: string }[], parentId: string }): Promise<void>;
  subscribeToJobStatus(jobId: string, statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void): Promise<() => Promise<void>>;
  getJobResult(opts: { outputKey: string, delete?: boolean }): Promise<JobResult<Output>>;
  acquireJob(opts: { token: string, block?: boolean }): Promise<JobData<Input, Meta> | undefined>;
  completeJob(opts: { token: string, jobId: string, result: JobResult<Output> }): Promise<void>;
  updateJob(opts: {
    token: string,
    status: Omit<JobActiveStatus<Meta, ProgressInfo>, 'meta'>,
    lockTimeoutMs?: number,
    input?: Input,
  }): Promise<{ interrupt: boolean }>;
  //TODO releaseBlockedCalls();
}
