import Redis from 'ioredis';
import type { Storage, StoredJobState } from './Storage';
import { pack, unpack } from '../utils/packer';
import { defaultRedisConnection } from '../utils/redis';

const ACTIVE_JOBS_KEY = 'jobs:active';

export class RedisStorage implements Storage {
  private readonly redis: Redis;

  constructor(redisConnection?: () => Redis) {
    this.redis = (redisConnection ?? defaultRedisConnection)();
  }

  async updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
  }): Promise<void> {
    if (opts.state !== undefined) {
      const now = Date.now();
      const stored = { state: opts.state, lastUpdatedAt: now };
      await this.redis.set(
        `jobs:state:${jobId}`,
        pack(stored),
        'PX',
        opts.ttlMs
      );
      // Add to sorted set with lastUpdatedAt as score
      await this.redis.zadd(ACTIVE_JOBS_KEY, now, jobId);
    }
    if (opts.status !== undefined) {
      await this.redis.publish(`jobs:status:${jobId}`, pack(opts.status));
    }
  }

  async getState<T = unknown>(jobId: string): Promise<StoredJobState<T> | undefined> {
    const data = await this.redis.getBuffer(`jobs:state:${jobId}`);
    if (!data) return undefined;
    const stored = unpack(data) as StoredJobState<T>;
    return stored;
  }

  async getActiveJobs<T = unknown>(): Promise<Array<{ jobId: string } & StoredJobState<T>>> {
    // Get all job IDs from sorted set, ordered by score (lastUpdatedAt) ascending
    const jobIds = await this.redis.zrange(ACTIVE_JOBS_KEY, 0, -1);
    if (jobIds.length === 0) return [];

    // Fetch all states in parallel
    const states = await Promise.all(
      jobIds.map(async (jobId) => {
        const state = await this.getState<T>(jobId);
        if (!state) {
          // State expired but still in sorted set, clean it up
          await this.redis.zrem(ACTIVE_JOBS_KEY, jobId);
          return null;
        }
        return { jobId, ...state };
      })
    );

    return states.filter((s): s is NonNullable<typeof s> => s !== null);
  }

  async setResult(jobId: string, result: unknown, status?: unknown): Promise<void> {
    await this.redis.set(`jobs:result:${jobId}`, pack(result));
    // Remove from active jobs set
    await this.redis.zrem(ACTIVE_JOBS_KEY, jobId);
    // Delete the state key
    await this.redis.del(`jobs:state:${jobId}`);
    if (status !== undefined) {
      await this.redis.publish(`jobs:status:${jobId}`, pack(status));
    }
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}
