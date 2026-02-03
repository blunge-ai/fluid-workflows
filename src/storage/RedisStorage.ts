import { randomUUID } from 'crypto';
import Redis, { ChainableCommander } from 'ioredis';
import type { Storage, StoredJobState, LockResult, StatusListener } from './Storage';
import { pack, unpack } from '../utils/packer';
import { defaultRedisConnection } from '../utils/redis';
import { chunk, timeout } from '~/utils';

const ACTIVE_JOBS_KEY = 'jobs:active';
const DEFAULT_BATCH_SIZE = 100;

type MultiCommand = (multi: ChainableCommander) => ChainableCommander;

/**
 * Execute Redis commands in a single multi/exec transaction.
 * @returns Array of results in the same order as commands, null for errors
 */
async function runMulti(
  redis: Redis,
  commands: MultiCommand[],
): Promise<(unknown | null)[]> {
  if (commands.length === 0) return [];

  let multi = redis.multi();
  for (const cmd of commands) {
    multi = cmd(multi);
  }
  const results = await multi.exec();
  if (!results) return commands.map(() => null);
  
  return results.map(([err, result]) => (err ? null : result));
}

// Lua script for atomic unlock: only delete if token matches
const UNLOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`;

// Lua script for atomic lock refresh: only extend TTL if token matches
const REFRESH_LOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
else
  return 0
end
`;

/**
 * Subscribe to a Redis channel with exponential backoff retry.
 * @param subscriber - Redis client in subscriber mode
 * @param channel - Channel to subscribe to
 * @param maxDelayMs - Maximum delay between retries (default 5000ms)
 */
async function ensureSubscribed(
  subscriber: Redis,
  channel: string,
  maxDelayMs = 5000,
): Promise<void> {
  let delayMs = 100;
  while (true) {
    try {
      await subscriber.subscribe(channel);
      return;
    } catch (err) {
      console.error('RedisStorage: subscribe error, retrying', { channel, err, delayMs });
      await timeout(delayMs);
      delayMs = Math.min(delayMs * 2, maxDelayMs);
    }
  }
}

export class RedisStorage implements Storage {
  private readonly redis: Redis;
  private subscriber: Redis | null = null;
  private readonly listeners = new Map<string, Set<StatusListener>>();

  constructor(private readonly redisConnection: () => Redis = defaultRedisConnection) {
    this.redis = redisConnection();
  }

  private getSubscriber(): Redis {
    if (!this.subscriber) {
      this.subscriber = this.redisConnection();
      this.subscriber.on('messageBuffer', (channelBuf: Buffer, messageBuf: Buffer) => {
        const channel = channelBuf.toString();
        const channelListeners = this.listeners.get(channel);
        if (channelListeners) {
          const status = unpack(messageBuf);
          for (const listener of channelListeners) {
            try {
              listener(status);
            } catch (err) {
              console.error('RedisStorage: listener error', { channel, err });
            }
          }
        }
      });
    }
    return this.subscriber;
  }

  async updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
    refreshLock?: { token: string, timeoutMs: number },
  }): Promise<void> {
    const hasState = opts.state !== undefined;
    const hasStatus = opts.status !== undefined;
    const hasRefreshLock = opts.refreshLock !== undefined;
    if (!hasState && !hasStatus && !hasRefreshLock) return;

    const multi = this.redis.multi();
    if (hasState) {
      const now = Date.now();
      const stored = { state: opts.state, lastUpdatedAt: now };
      multi.set(`jobs:state:${jobId}`, pack(stored), 'PX', opts.ttlMs);
      // Add to sorted set with lastUpdatedAt as score
      multi.zadd(ACTIVE_JOBS_KEY, now, jobId);
    }
    if (hasStatus) {
      multi.publish(`jobs:status:${jobId}`, pack(opts.status));
    }
    if (hasRefreshLock) {
      const lockKey = `jobs:lock:${jobId}`;
      // Use Lua script inline via EVAL to atomically refresh lock if token matches
      multi.eval(REFRESH_LOCK_SCRIPT, 1, lockKey, opts.refreshLock!.token, opts.refreshLock!.timeoutMs);
    }
    await multi.exec();
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

    const expiredJobIds: string[] = [];
    const activeJobs: Array<{ jobId: string } & StoredJobState<T>> = [];

    // Process in batches
    for (const jobIdsChunk of chunk(jobIds, DEFAULT_BATCH_SIZE)) {
      const stateResults = await runMulti(
        this.redis,
        jobIdsChunk.map((jobId) => (m: ChainableCommander) => m.getBuffer(`jobs:state:${jobId}`)),
      );

      for (let i = 0; i < jobIdsChunk.length; i++) {
        const jobId = jobIdsChunk[i]!;
        const data = stateResults[i] as Buffer | null;
        if (!data) {
          expiredJobIds.push(jobId);
        } else {
          const stored = unpack(data) as StoredJobState<T>;
          activeJobs.push({ jobId, ...stored });
        }
      }
    }

    // Batch remove expired entries from the sorted set
    for (const expiredChunk of chunk(expiredJobIds, DEFAULT_BATCH_SIZE)) {
      await runMulti(
        this.redis,
        expiredChunk.map((jobId) => (m: ChainableCommander) => m.zrem(ACTIVE_JOBS_KEY, jobId)),
      );
    }

    return activeJobs;
  }

  async setResult(jobId: string, result: unknown, opts: { ttlMs: number, status?: unknown }): Promise<void> {
    const multi = this.redis.multi();
    multi.set(`jobs:result:${jobId}`, pack(result), 'PX', opts.ttlMs);
    // Remove from active jobs set
    multi.zrem(ACTIVE_JOBS_KEY, jobId);
    // Delete the state key
    multi.del(`jobs:state:${jobId}`);
    if (opts.status !== undefined) {
      multi.publish(`jobs:status:${jobId}`, pack(opts.status));
    }
    await multi.exec();
  }

  async lock(jobId: string, ttlMs: number): Promise<LockResult> {
    const token = randomUUID();
    const key = `jobs:lock:${jobId}`;
    // SET key value NX PX ttlMs - atomic set-if-not-exists with expiration
    const result = await this.redis.set(key, token, 'PX', ttlMs, 'NX');
    if (result === 'OK') {
      return { acquired: true, token };
    }
    return { acquired: false };
  }

  async unlock(jobId: string, token: string): Promise<boolean> {
    const key = `jobs:lock:${jobId}`;
    // Use Lua script to atomically check token and delete
    const result = await this.redis.eval(UNLOCK_SCRIPT, 1, key, token);
    return result === 1;
  }

  async refreshLock(jobId: string, token: string, ttlMs: number): Promise<boolean> {
    const key = `jobs:lock:${jobId}`;
    // Use Lua script to atomically check token and extend TTL
    const result = await this.redis.eval(REFRESH_LOCK_SCRIPT, 1, key, token, ttlMs);
    return result === 1;
  }

  async getResult<T = unknown>(jobId: string): Promise<T | undefined> {
    const data = await this.redis.getBuffer(`jobs:result:${jobId}`);
    if (!data) return undefined;
    return unpack(data) as T;
  }

  async waitForLock(jobId: string, timeoutMs: number): Promise<boolean> {
    const key = `jobs:lock:${jobId}`;
    const pollIntervalMs = 50;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const exists = await this.redis.exists(key);
      if (!exists) return true;
      await timeout(pollIntervalMs);
    }
    return false;
  }

  async cleanup(): Promise<number> {
    // Redis handles TTL automatically for keys.
    // Clean up the active jobs set by removing entries whose state keys have expired.
    const jobIds = await this.redis.zrange(ACTIVE_JOBS_KEY, 0, -1);
    if (jobIds.length === 0) return 0;

    const expiredJobIds: string[] = [];

    // Check existence in batches
    for (const jobIdsChunk of chunk(jobIds, DEFAULT_BATCH_SIZE)) {
      const existsResults = await runMulti(
        this.redis,
        jobIdsChunk.map((jobId) => (m: ChainableCommander) => m.exists(`jobs:state:${jobId}`)),
      );

      for (let i = 0; i < jobIdsChunk.length; i++) {
        if (existsResults[i] === 0) {
          expiredJobIds.push(jobIdsChunk[i]!);
        }
      }
    }

    if (expiredJobIds.length === 0) return 0;

    // Remove expired entries in batches
    for (const expiredChunk of chunk(expiredJobIds, DEFAULT_BATCH_SIZE)) {
      await runMulti(
        this.redis,
        expiredChunk.map((jobId) => (m: ChainableCommander) => m.zrem(ACTIVE_JOBS_KEY, jobId)),
      );
    }

    return expiredJobIds.length;
  }

  subscribe(jobId: string, listener: StatusListener): () => void {
    const channel = `jobs:status:${jobId}`;
    
    let channelListeners = this.listeners.get(channel);
    if (!channelListeners) {
      channelListeners = new Set();
      this.listeners.set(channel, channelListeners);
      ensureSubscribed(this.getSubscriber(), channel);
    }
    channelListeners.add(listener);

    return () => {
      const currentListeners = this.listeners.get(channel);
      if (currentListeners) {
        currentListeners.delete(listener);
        if (currentListeners.size === 0) {
          this.listeners.delete(channel);
          this.subscriber?.unsubscribe(channel).catch((err) => {
            console.error('RedisStorage: unsubscribe error', { channel, err });
          });
        }
      }
    };
  }

  async close(): Promise<void> {
    if (this.subscriber) {
      await this.subscriber.quit();
      this.subscriber = null;
    }
    this.listeners.clear();
    await this.redis.quit();
  }
}
