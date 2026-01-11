import type { JobQueueEngine, JobData, JobResult, JobStatus } from './JobQueueEngine';
import { pack, unpack } from '../utils/packer';
 
 function makeError(status: number, body: any) {
   const msg = (body && typeof body === 'object' && 'error' in body) ? String(body.error) : `HTTP ${status}`;
   return new Error(msg);
 }
 
 export class HttpJobQueueEngineClient implements JobQueueEngine {
   constructor(private readonly baseUrl: string, private readonly basePath = '/engine') {}
 
   private async post<T>(endpoint: string, payload: any): Promise<T> {
     const url = this.baseUrl + (this.basePath.endsWith('/') ? this.basePath.slice(0, -1) : this.basePath) + endpoint;
     const res = await fetch(url, {
       method: 'POST',
       headers: { 'Content-Type': 'application/octet-stream' },
       body: pack(payload ?? {}),
     } as any);
     const buf = new Uint8Array(await res.arrayBuffer());
     const body = buf.byteLength ? unpack(buf) : undefined;
     if (!res.ok) {
       throw makeError(res.status, body);
     }
     return body as T;
   }


  async submitJob<Meta, ProgressInfo>(opts: {
    queue: string,
    data: JobData<unknown, Meta>,
    parentId?: string,
    parentQueue?: string,
    dataKey?: string,
    statusHandler?: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<void> {
    if (opts.statusHandler) {
      throw Error('NotImplemented');
    }
    await this.post<void>('/submitJob', opts as any);
  }

  async subscribeToJobStatus<Meta, ProgressInfo>(_opts: {
    queue: string,
    jobId: string,
    statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<() => Promise<void>> {
    throw Error('NotImplemented');
  }

  async acquireJob<Input, Meta, ChildOutput>(opts: {
    queue: string,
    token: string,
    block?: boolean
  }): Promise<{
    data: JobData<Input, Meta> | undefined,
    childResults?: Record<string, JobResult<ChildOutput>>
  }> {
    return await this.post('/acquireJob', opts as any);
  }

  async completeJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    result: JobResult<unknown>
  }): Promise<void> {
    await this.post<void>('/completeJob', opts as any);
  }

  async updateJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    lockTimeoutMs?: number,
    progressInfo?: unknown,
    input?: unknown,
  }): Promise<{ interrupt: boolean }> {
    return await this.post('/updateJob', opts as any);
  }

  async getJobResult<Output>(
    opts: { resultKey: string, delete?: boolean }
  ): Promise<JobResult<Output>> {
    return await this.post('/getJobResult', opts as any);
  }

  async submitChildrenSuspendParent<ChildOutput>(opts: {
    children: { data: JobData<unknown>, queue: string }[],
    token: string,
    parentId: string,
    parentQueue: string,
  }): Promise<Record<string, JobResult<ChildOutput>> | undefined> {
    const res = await this.post<Record<string, JobResult<ChildOutput>> | null>('/submitChildrenSuspendParent', opts as any);
    return res ?? undefined;
  }
}
