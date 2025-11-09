import Redis from 'ioredis';

type IsPlainObject<T> = T extends Record<string, unknown> ? true : false;

export type Simplify<T> =
  IsPlainObject<T> extends true
    ? { [KeyType in keyof T]: Simplify<T[KeyType]> }
    : T;

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

export function timeout(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function assertNever(value?: never): never {
  throw Error('reached unreachable code');
}

export function assert(condition: any, msg?: string): asserts condition {
  if (!condition) {
    throw new Error(msg);
  }
}

export function mapValues<I, O>(map: Record<string, I>, fn: (value: I) => O) {
  const result: Record<string, O> = {};
  for (const [key, value] of Object.entries(map)) {
    result[key] = fn(value);
  }
  return result;
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
