import { randomUUID } from 'crypto';
import type { WfMeta, WfUpdateInfo, WfStatus } from '../types';
import type { Logger } from '../utils';
import { defaultLogger } from '../utils';
import type { LockResult, StatusListener, Storage, StoredJobState } from './Storage';

export type BestEffortWrapperOptions<Meta extends WfMeta = WfMeta, Info extends WfUpdateInfo = WfUpdateInfo> = {
  storage: Storage<Meta, Info>,
  logger?: Logger,
};

export class BestEffortWrapper<Meta extends WfMeta = WfMeta, Info extends WfUpdateInfo = WfUpdateInfo> implements Storage<Meta, Info> {
  private readonly storage: Storage<Meta, Info>;
  private readonly logger: Logger;

  constructor(storage: Storage<Meta, Info>);
  constructor(opts: BestEffortWrapperOptions<Meta, Info>);
  constructor(storageOrOpts: Storage<Meta, Info> | BestEffortWrapperOptions<Meta, Info>) {
    if ('storage' in storageOrOpts) {
      this.storage = storageOrOpts.storage;
      this.logger = storageOrOpts.logger ?? defaultLogger;
      return;
    }
    this.storage = storageOrOpts;
    this.logger = defaultLogger;
  }

  private logError(method: string, err: unknown, context: Record<string, unknown> = {}): void {
    try {
      this.logger.error({ method, err, ...context }, 'best-effort storage: ignored storage error');
    } catch {
      return;
    }
  }

  private async ignoreErrors<T>(
    method: string,
    fn: () => Promise<T> | T,
    fallback: T,
    context: Record<string, unknown> = {},
  ): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      this.logError(method, err, context);
      return fallback;
    }
  }

  async lock(jobId: string, ttlMs: number): Promise<LockResult> {
    return this.ignoreErrors(
      'lock',
      () => this.storage.lock(jobId, ttlMs),
      { acquired: true, token: `best-effort-${randomUUID()}` },
      { jobId, ttlMs },
    );
  }

  async unlock(jobId: string, token: string): Promise<boolean> {
    return this.ignoreErrors('unlock', () => this.storage.unlock(jobId, token), true, { jobId });
  }

  async refreshLock(jobId: string, token: string, ttlMs: number): Promise<boolean> {
    return this.ignoreErrors('refreshLock', () => this.storage.refreshLock(jobId, token, ttlMs), true, { jobId, ttlMs });
  }

  async updateState(jobId: string, opts: {
    state?: unknown,
    status?: WfStatus<Meta, Info>,
    ttlMs: number,
    refreshLock?: { token: string, timeoutMs: number },
  }): Promise<void> {
    await this.ignoreErrors('updateState', () => this.storage.updateState(jobId, opts), undefined, { jobId });
  }

  async getState<T = unknown>(jobId: string): Promise<StoredJobState<T> | undefined> {
    return this.ignoreErrors('getState', () => this.storage.getState<T>(jobId), undefined, { jobId });
  }

  async getActiveJobs<T = unknown>(): Promise<Array<{ jobId: string } & StoredJobState<T>>> {
    return this.ignoreErrors('getActiveJobs', () => this.storage.getActiveJobs<T>(), []);
  }

  async setResult(jobId: string, result: unknown, opts: { ttlMs: number, status?: WfStatus<Meta, Info> }): Promise<void> {
    await this.ignoreErrors('setResult', () => this.storage.setResult(jobId, result, opts), undefined, { jobId, ttlMs: opts.ttlMs });
  }

  async getResult<T = unknown>(jobId: string): Promise<T | undefined> {
    return this.ignoreErrors('getResult', () => this.storage.getResult<T>(jobId), undefined, { jobId });
  }

  async waitForLock(jobId: string, timeoutMs: number): Promise<boolean> {
    return this.ignoreErrors('waitForLock', () => this.storage.waitForLock(jobId, timeoutMs), true, { jobId, timeoutMs });
  }

  async cleanup(): Promise<number> {
    return this.ignoreErrors('cleanup', () => this.storage.cleanup(), 0);
  }

  subscribe(jobId: string, listener: StatusListener<Meta, Info>): () => void {
    try {
      const unsubscribe = this.storage.subscribe(jobId, listener);
      return () => {
        try {
          unsubscribe();
        } catch (err) {
          this.logError('unsubscribe', err, { jobId });
          return;
        }
      };
    } catch (err) {
      this.logError('subscribe', err, { jobId });
      return () => {};
    }
  }

  async close(): Promise<void> {
    await this.ignoreErrors('close', () => this.storage.close(), undefined);
  }
}
