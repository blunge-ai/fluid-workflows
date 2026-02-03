type IsPlainObject<T> = T extends Record<string, unknown> ? true : false;

export type Simplify<T> =
  IsPlainObject<T> extends true
    ? { [KeyType in keyof T]: Simplify<T[KeyType]> }
    : T;

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

/**
 * Partition an array into chunks of the given size.
 * The last chunk may contain fewer items.
 */
export function chunk<T>(array: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}
