
export type JobResultType = 'cancelled' | 'success' | 'error';
export type JobStatusType = 'queued' | 'active' | 'suspended' | JobResultType;

export function isResultStatusType(statusType: JobStatusType): statusType is JobResultType {
  return statusType === 'cancelled' || statusType === 'success' || statusType === 'error';
}

export function isResultStatus(status: JobStatus<unknown, unknown>): status is JobResultStatus<unknown> {
  return isResultStatusType(status.type);
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

export interface JobQueueEngine {

  submitJob<Meta, ProgressInfo>(opts: {
    queue: string,
    data: JobData<unknown, Meta>,
    parentId?: string,
    parentQueue?: string,
    dataKey?: string,
    statusHandler?: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<void>;

  subscribeToJobStatus<Meta, ProgressInfo>(opts: {
    queue: string,
    jobId: string,
    statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<() => Promise<void>>;

  acquireJob<Input, Meta = unknown, ChildOutput = unknown>(opts: {
    queue: string,
    token: string,
    block?: boolean
  }): Promise<{
    data: JobData<Input, Meta> | undefined,
    childResults?: Record<string, JobResult<ChildOutput>>
  }>;

  completeJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    result: JobResult<unknown>
  }): Promise<void>;

  updateJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    lockTimeoutMs?: number,
    progressInfo?: unknown,
    input?: unknown,
  }): Promise<{ interrupt: boolean }>;

  getJobResult<Output>(opts: {
    resultKey: string,
    delete?: boolean
  }): Promise<JobResult<Output>>;

  submitChildrenSuspendParent<ChildOutput = unknown>(opts: {
    children: {
      data: JobData<unknown, unknown>,
      queue: string
    }[],
    token: string,
    parentId: string,
    parentQueue: string,
  }): Promise<Record<string, JobResult<ChildOutput>> | undefined>;

  //TODO releaseBlockedCalls();
}
