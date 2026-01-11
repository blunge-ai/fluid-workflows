import Redis from 'ioredis';
import type { Storage } from './Storage';
import { pack, unpack } from './packer';
import { defaultRedisConnection } from './utils';

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
      await this.redis.set(
        `jobs:state:${jobId}`,
        pack(opts.state),
        'PX',
        opts.ttlMs
      );
    }
    if (opts.status !== undefined) {
      await this.redis.publish(`jobs:status:${jobId}`, pack(opts.status));
    }
  }

  async setResult(jobId: string, result: unknown, status?: unknown): Promise<void> {
    await this.redis.set(`jobs:result:${jobId}`, pack(result));
    if (status !== undefined) {
      await this.redis.publish(`jobs:status:${jobId}`, pack(status));
    }
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}
