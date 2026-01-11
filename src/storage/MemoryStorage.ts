import type { Storage } from './Storage';

export class MemoryStorage implements Storage {
  private readonly states = new Map<string, unknown>();
  private readonly results = new Map<string, unknown>();

  async updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
  }): Promise<void> {
    if (opts.state !== undefined) {
      this.states.set(jobId, opts.state);
    }
    // status is ignored in memory storage (no pub/sub)
  }

  async setResult(jobId: string, result: unknown, status?: unknown): Promise<void> {
    this.results.set(jobId, result);
    // status is ignored in memory storage (no pub/sub)
  }

  async close(): Promise<void> {
    this.states.clear();
    this.results.clear();
  }

  // Helper methods for testing/debugging
  getState(jobId: string): unknown | undefined {
    return this.states.get(jobId);
  }

  getResult(jobId: string): unknown | undefined {
    return this.results.get(jobId);
  }
}
