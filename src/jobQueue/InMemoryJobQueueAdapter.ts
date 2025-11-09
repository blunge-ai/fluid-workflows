import { v4 as uuidv4 } from 'uuid';
import { EventEmitter } from 'events';
import { assert } from '~/utils';
import type { JobQueueEngine, JobData, JobResult, JobStatus, JobQueuedStatus, JobActiveStatus, JobResultStatus } from './JobQueueEngine';

type InternalJob = {
  queue: string,
  dataKey: string,
  job: JobData<any, any>,
  parentId?: string,
  parentQueue?: string,
};

// Note! This implementation is only useful for testing purposes.

export class InMemoryJobQueueAdapter implements JobQueueEngine {
  private queues = new Map<string, Array<InternalJob>>();
  private runningJobs = new Map<string, InternalJob>();
  private resultStore = new Map<string, JobResult<unknown>>();
  private emitter = new EventEmitter();
  private parentChildren = new Map<string, Set<string>>();
  private childToParent = new Map<string, string>();
  private readyChildResults = new Map<string, Record<string, JobResult<any>>>();

  private getQueue(name: string) {
    let q = this.queues.get(name);
    if (!q) {
      q = [];
      this.queues.set(name, q);
    }
    return q;
  }

  private emitStatus<Meta, ProgressInfo>(jobId: string, status: JobStatus<Meta, ProgressInfo>) {
    this.emitter.emit(`status:${jobId}`, status);
    if (status.type === 'success' || status.type === 'error' || status.type === 'cancelled') {
      this.emitter.removeAllListeners(`status:${jobId}`);
    }
  }

  private onStatus<Meta, ProgressInfo>(jobId: string, handler: (s: JobStatus<Meta, ProgressInfo>) => void) {
    const event = `status:${jobId}`;
    this.emitter.on(event, handler);
    return () => this.emitter.off(event, handler);
  }

  private async waitForJob(queue: string) {
    await new Promise<void>((resolve) => this.emitter.once(`job:${queue}`, () => resolve()));
  }

  async submitJob<Meta, ProgressInfo>(opts: {
    queue: string,
    data: JobData<unknown, Meta>,
    parentId?: string,
    parentQueue?: string,
    dataKey?: string,
    statusHandler?: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<void> {
    const dataKey = opts.dataKey ?? uuidv4();
    const q = this.getQueue(opts.queue);
    const job: InternalJob = {
      queue: opts.queue,
      dataKey,
      job: opts.data,
      parentId: opts.parentId,
      parentQueue: opts.parentQueue
    };
    const waiting = q.length;
    q.push(job);
    if (opts.statusHandler) {
      this.onStatus(opts.data.id, opts.statusHandler);
    }
    const queued: JobQueuedStatus<Meta> = {
      type: 'queued',
      jobId: opts.data.id,
      meta: opts.data.meta as Meta,
      waiting
    };
    this.emitter.emit(`job:${opts.queue}`);
    this.emitStatus(opts.data.id, queued);
  }

  async subscribeToJobStatus<Meta, ProgressInfo>(opts: {
    queue: string,
    jobId: string,
    statusHandler: (status: JobStatus<Meta, ProgressInfo>) => void
  }): Promise<() => Promise<void>> {
    const off = this.onStatus(opts.jobId, opts.statusHandler);
    return async () => { off(); };
  }

  async acquireJob<Input, Meta, ChildOutput>(opts: {
    queue: string,
    token: string,
    block?: boolean
  }): Promise<{
    data: JobData<Input, Meta> | undefined,
    childResults?: Record<string, JobResult<ChildOutput>>
  }> {
    const q = this.getQueue(opts.queue);
    while (q.length === 0 && opts.block) {
      await this.waitForJob(opts.queue);
    }
    const entry = q.shift();
    if (!entry) {
      return { data: undefined }
    };
    this.runningJobs.set(entry.job.id, entry);
    const results = this.readyChildResults.get(entry.job.id) as Record<string, JobResult<ChildOutput>> | undefined;
    if (results) {
      this.readyChildResults.delete(entry.job.id);
    }
    return {
      data: entry.job as JobData<Input, Meta>,
      childResults: results
    };
  }

  async completeJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    result: JobResult<unknown>
  }): Promise<void> {
    const entry = this.runningJobs.get(opts.jobId);
    if (!entry) {
      throw new Error('job not found');
    }
    this.runningJobs.delete(opts.jobId);

    if (opts.result.type === 'success') {
      this.resultStore.set(entry.dataKey, opts.result);
      const status: JobResultStatus<any> = {
        type: 'success',
        jobId: entry.job.id,
        meta: entry.job.meta,
        resultKey: entry.dataKey
      };
      this.emitStatus(entry.job.id, status);
    } else {
      const status: JobResultStatus<any> = {
        type: opts.result.type,
        jobId: entry.job.id,
        meta: entry.job.meta,
        reason: opts.result.reason
      };
      this.emitStatus(entry.job.id, status);
    }

    const parentId = this.childToParent.get(opts.jobId);
    if (parentId) {
      this.childToParent.delete(opts.jobId);
      const pending = this.parentChildren.get(parentId);
      assert(pending, 'parent found but no children pending');
      pending.delete(opts.jobId);
      const m = this.readyChildResults.get(parentId) ?? {};
      m[opts.jobId] = opts.result;
      this.readyChildResults.set(parentId, m);
      if (pending.size === 0) {
        this.parentChildren.delete(parentId);
        const parentEntry = this.runningJobs.get(parentId);
        assert(parentEntry, 'no such parent job');
        const pq = this.getQueue(parentEntry.queue);
        const waiting = pq.length;
        pq.push(parentEntry);
        this.emitStatus(parentId, {
          type: 'queued',
          jobId: parentId,
          meta: parentEntry.job.meta,
          waiting
        });
        this.emitter.emit(`job:${parentEntry.queue}`);
      }
    }
  }

  async updateJob(opts: {
    queue: string,
    token: string,
    jobId: string,
    lockTimeoutMs?: number,
    progressInfo?: unknown,
    input?: unknown,
  }): Promise<{ interrupt: boolean }> {
    const entry = this.runningJobs.get(opts.jobId);
    if (!entry) throw new Error('update status: job not found');
    if (opts.input !== undefined) {
      entry.job = { ...entry.job, input: opts.input };
    }
    if (opts.progressInfo !== undefined) {
      const status: JobActiveStatus<any, any> = {
        type: 'active',
        jobId: entry.job.id,
        meta: entry.job.meta,
        info: opts.progressInfo
      };
      this.emitStatus(entry.job.id, status);
    }
    return { interrupt: false };
  }

  async getJobResult<Output>(opts: { resultKey: string, delete?: boolean }): Promise<JobResult<Output>> {
    const res = this.resultStore.get(opts.resultKey) as JobResult<Output> | undefined;
    if (!res) {
      return { type: 'error', reason: 'process job: no result data' } as JobResult<Output>;
    }
    if (opts.delete) this.resultStore.delete(opts.resultKey);
    return res;
  }

  async submitChildrenSuspendParent<ChildOutput>(opts: {
    children: { data: JobData<unknown, unknown>, queue: string }[],
    token: string,
    parentId: string,
    parentQueue: string,
  }): Promise<Record<string, JobResult<ChildOutput>> | undefined> {
    const parent = this.runningJobs.get(opts.parentId);
    if (!parent) {
      throw new Error('parent job not found');
    }
    if (opts.children.length === 0) {
      const r = this.readyChildResults.get(opts.parentId) as Record<string, JobResult<ChildOutput>> | undefined;
      if (!r) {
        return undefined;
      }
      this.readyChildResults.delete(opts.parentId);
      return r;
    }

    const pending = new Set<string>();
    for (const child of opts.children) {
      pending.add(child.data.id);
      this.childToParent.set(child.data.id, opts.parentId);
      await this.submitJob({ queue: child.queue, data: child.data });
    }
    this.parentChildren.set(opts.parentId, pending);
    this.emitStatus(parent.job.id, {
      type: 'suspended',
      jobId: parent.job.id,
      meta: parent.job.meta,
      waiting: pending.size
    });
    return undefined;
  }
}
