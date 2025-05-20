
export type JobResultType = 'cancelled' | 'success' | 'error';
export type JobStatusType = 'queued' | 'active' | 'suspended' | JobResultType;

export function isResultStatus(statusType: JobStatusType): statusType is JobResultType {
  return statusType === 'cancelled' || statusType === 'success' || statusType === 'error';
}

export type JobResultStatus<Meta> = {
  type: 'success',
  jobId: string,
  meta: Meta,
  resultKey: string,
} | {
  type: Exclude<JobResultType, 'success'>,
  jobId: string,
  meta: Meta,
  reason: string,
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

  submitJob(opts: {
    data: JobData<Input, Meta>,
    queue: string,
    parentId?: string,
    parentQueue?: string,
    dataKey?: string,
    statusHandler?: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<void>;

  subscribeToJobStatus(opts: {
    jobId: string,
    queue: string,
    statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<() => Promise<void>>;

  acquireJob(opts: {
    queue: string,
    token: string,
    block?: boolean
  }): Promise<JobData<Input, Meta> | undefined>;

  completeJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    result: JobResult<Output>
  }): Promise<void>;

  updateJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    lockTimeoutMs?: number,
    progressInfo?: ProgressInfo,
    input?: Input,
  }): Promise<{ interrupt: boolean }>;

  getJobResult(opts: {
    resultKey: string,
    delete?: boolean
  }): Promise<JobResult<Output>>;

  submitChildrenSuspendParent(opts: {
    children: {
      data: JobData<Input, Meta>,
      queue: string
    }[],
    token: string,
    parentId: string,
    parentQueue: string,
  }): Promise<Record<string, unknown> | undefined>;

  //TODO releaseBlockedCalls();
}
