import type { Storage, StoredJobState } from './Storage';

type StoredEntry = {
  state: unknown,
  lastUpdatedAt: number,
};

export class MemoryStorage implements Storage {
  private readonly states = new Map<string, StoredEntry>();
  private readonly results = new Map<string, unknown>();

  async updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
  }): Promise<void> {
    if (opts.state !== undefined) {
      this.states.set(jobId, {
        state: opts.state,
        lastUpdatedAt: Date.now(),
      });
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

  async setResult(jobId: string, result: unknown, status?: unknown): Promise<void> {
    this.results.set(jobId, result);
    // Remove from active states when result is set
    this.states.delete(jobId);
    // status is ignored in memory storage (no pub/sub)
  }

  async close(): Promise<void> {
    this.states.clear();
    this.results.clear();
  }

  // Helper method for testing/debugging
  getResult(jobId: string): unknown | undefined {
    return this.results.get(jobId);
  }
}
