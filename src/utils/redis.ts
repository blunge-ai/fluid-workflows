import Redis from 'ioredis';

export const redisOptions ={
  //family: 6, // IPv6 required for fly.io
  enableOfflineQueue: true,
  maxRetriesPerRequest: null,
  reconnectOnError: () => true,
  commandTimeout: 30000, // in ms; must not be lower than bullWorkerBlockingTimeout
};

export function defaultRedisConnection() {
  return new Redis(process.env.REDIS_URL ?? '', redisOptions);
}
