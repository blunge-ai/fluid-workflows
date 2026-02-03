import { DurableObject } from 'cloudflare:workers';
import type { StoredJobState } from '../storage/Storage';
import { DurableObjectStorage } from '../storage/DurableObjectStorage';

export interface Env {
  TEST_DO: DurableObjectNamespace<TestDurableObject>;
}

/**
 * Test Durable Object that exposes storage operations for testing.
 * Uses SQLite-backed storage.
 * 
 * Storage operations are exposed as RPC methods since the Storage
 * object itself cannot be serialized across the RPC boundary.
 */
export class TestDurableObject extends DurableObject<Env> {
  private readonly storage: DurableObjectStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.storage = new DurableObjectStorage(ctx);
  }

  // Storage interface methods exposed via RPC

  async updateState(jobId: string, opts: {
    state?: unknown;
    status?: unknown;
    ttlMs: number;
  }): Promise<void> {
    return this.storage.updateState(jobId, opts);
  }

  async getState<T = unknown>(jobId: string): Promise<StoredJobState<T> | undefined> {
    return this.storage.getState<T>(jobId);
  }

  async getActiveJobs<T = unknown>(): Promise<Array<{ jobId: string } & StoredJobState<T>>> {
    return this.storage.getActiveJobs<T>();
  }

  async setResult(jobId: string, result: unknown, opts: { ttlMs: number, status?: unknown }): Promise<void> {
    return this.storage.setResult(jobId, result, opts);
  }

  async close(): Promise<void> {
    return this.storage.close();
  }

  /**
   * Clear all storage (for test isolation).
   */
  async clearStorage(): Promise<void> {
    await this.ctx.storage.deleteAll();
  }

  /**
   * Get result (for testing).
   */
  async getResult<T = unknown>(jobId: string): Promise<T | undefined> {
    return this.storage.getResult<T>(jobId);
  }

  /**
   * Clean up expired data.
   */
  async cleanup(): Promise<number> {
    return this.storage.cleanup();
  }
}

export default {
  async fetch(_request: Request, _env: Env): Promise<Response> {
    return new Response('OK');
  },
};
