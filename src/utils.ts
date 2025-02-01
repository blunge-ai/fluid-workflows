import Redis from 'ioredis';

export const redisOptions ={
  family: 6, // IPv6 required for fly.io
  enableOfflineQueue: true,
  maxRetriesPerRequest: null,
  reconnectOnError: () => true,
  commandTimeout: 30000, // in ms; must not be lower than bullWorkerBlockingTimeout
};

export function defaultRedisConnection() {
  return new Redis(process.env.REDIS_URL ?? '', redisOptions);
}

export function timeout(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export type Logger = {
  error: (data: Record<string, unknown>, message: string) => void,
  warn: (data: Record<string, unknown>, message: string) => void,
  info: (data: Record<string, unknown>, message: string) => void,
};

export const defaultLogger = {
  error: (data: Record<string, unknown>, message: string) => console.error(`${message}:\n${JSON.stringify(data)}`),
  warn: (data: Record<string, unknown>, message: string) => console.warn(`${message}:\n${JSON.stringify(data)}`),
  info: (data: Record<string, unknown>, message: string) => console.log(`${message}:\n${JSON.stringify(data)}`),
};
