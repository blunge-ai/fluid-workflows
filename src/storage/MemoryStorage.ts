import { randomUUID } from 'crypto';
import type { Storage, StoredJobState, LockResult } from './Storage';

type StoredEntry = {
  state: unknown,
  lastUpdatedAt: number,
};

type ResultEntry = {
  result: unknown,
  expiresAt: number,
};

type LockEntry = {
  token: string,
  expiresAt: number,
};

type LockWaiter = {
  resolve: () => void,
  timeoutId: ReturnType<typeof setTimeout>,
};

export class MemoryStorage implements Storage {
  private readonly states = new Map<string, StoredEntry>();
  private readonly results = new Map<string, ResultEntry>();
  private readonly locks = new Map<string, LockEntry>();
  private readonly lockWaiters = new Map<string, Set<LockWaiter>>();

  async updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
    refreshLock?: { token: string, timeoutMs: number },
  }): Promise<void> {
    if (opts.state !== undefined) {
      this.states.set(jobId, {
        state: opts.state,
        lastUpdatedAt: Date.now(),
      });
    }
    // Refresh lock if requested
    if (opts.refreshLock) {
      const existing = this.locks.get(jobId);
      if (existing && existing.token === opts.refreshLock.token) {
        existing.expiresAt = Date.now() + opts.refreshLock.timeoutMs;
      }
    }
    // status is ignored in memory storage (no pub/sub)
  }

  async getState<T = unknown>(jobId: string): Promise<StoredJobState<T> | undefined> {
    const entry = this.states.get(jobId);
    if (!entry) return undefined;
    return {
      state: entry.state as T,
      lastUpdatedAt: entry.lastUpdatedAt,
    };
  }

  async getActiveJobs<T = unknown>(): Promise<Array<{ jobId: string } & StoredJobState<T>>> {
    const entries = Array.from(this.states.entries()).map(([jobId, entry]) => ({
      jobId,
      state: entry.state as T,
      lastUpdatedAt: entry.lastUpdatedAt,
    }));
    // Sort by lastUpdatedAt ascending (oldest first)
    entries.sort((a, b) => a.lastUpdatedAt - b.lastUpdatedAt);
    return entries;
  }

  async setResult(jobId: string, result: unknown, opts: { ttlMs: number, status?: unknown }): Promise<void> {
    this.results.set(jobId, {
      result,
      expiresAt: Date.now() + opts.ttlMs,
    });
    // Remove from active states when result is set
    this.states.delete(jobId);
    // status is ignored in memory storage (no pub/sub)
  }

  async close(): Promise<void> {
    this.states.clear();
    this.results.clear();
  }

  async lock(jobId: string, ttlMs: number): Promise<LockResult> {
    const existing = this.locks.get(jobId);
    const now = Date.now();
    // Check if lock exists and hasn't expired
    if (existing && existing.expiresAt > now) {
      return { acquired: false };
    }
    const token = randomUUID();
    this.locks.set(jobId, { token, expiresAt: now + ttlMs });
    return { acquired: true, token };
  }

  async unlock(jobId: string, token: string): Promise<boolean> {
    const existing = this.locks.get(jobId);
    if (!existing || existing.token !== token) {
      return false;
    }
    this.locks.delete(jobId);
    // Notify all waiters that the lock is released
    this.notifyWaiters(jobId);
    return true;
  }

  async refreshLock(jobId: string, token: string, ttlMs: number): Promise<boolean> {
    const existing = this.locks.get(jobId);
    if (!existing || existing.token !== token) {
      return false;
    }
    // Extend the expiration time
    existing.expiresAt = Date.now() + ttlMs;
    return true;
  }

  private notifyWaiters(jobId: string): void {
    const waiters = this.lockWaiters.get(jobId);
    if (waiters) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timeoutId);
        waiter.resolve();
      }
      this.lockWaiters.delete(jobId);
    }
  }

  async getResult<T = unknown>(jobId: string): Promise<T | undefined> {
    const entry = this.results.get(jobId);
    if (!entry || entry.expiresAt <= Date.now()) return undefined;
    return entry.result as T;
  }

  async cleanup(): Promise<number> {
    const now = Date.now();
    let count = 0;
    
    // Clean up expired results
    for (const [jobId, entry] of this.results) {
      if (entry.expiresAt <= now) {
        this.results.delete(jobId);
        count++;
      }
    }
    
    // Clean up expired locks
    for (const [jobId, entry] of this.locks) {
      if (entry.expiresAt <= now) {
        this.locks.delete(jobId);
        count++;
      }
    }
    
    return count;
  }

  async waitForLock(jobId: string, timeoutMs: number): Promise<boolean> {
    // Check if lock is already free
    const existing = this.locks.get(jobId);
    const now = Date.now();
    if (!existing || existing.expiresAt <= now) {
      return true;
    }

    // Wait for notification or timeout
    return new Promise<boolean>((resolve) => {
      const timeoutId = setTimeout(() => {
        // Timeout - remove from waiters and return false
        const waiters = this.lockWaiters.get(jobId);
        if (waiters) {
          waiters.delete(waiter);
          if (waiters.size === 0) {
            this.lockWaiters.delete(jobId);
          }
        }
        resolve(false);
      }, timeoutMs);

      const waiter: LockWaiter = {
        resolve: () => resolve(true),
        timeoutId,
      };

      let waiters = this.lockWaiters.get(jobId);
      if (!waiters) {
        waiters = new Set();
        this.lockWaiters.set(jobId, waiters);
      }
      waiters.add(waiter);
    });
  }
}
