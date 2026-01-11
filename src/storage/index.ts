import type { Storage } from './Storage';
import { MemoryStorage } from './MemoryStorage';

export type { Storage, StoredJobState } from './Storage';
export { MemoryStorage } from './MemoryStorage';
export { RedisStorage } from './RedisStorage';
export { DurableObjectStorage } from './DurableObjectStorage';
export type { DurableObjectState, DurableObjectStorageAPI, SqlStorage, SqlStorageCursor } from './DurableObjectStorage';

/**
 * Global storage factory. Override this to change the default storage implementation.
 * 
 * Usage:
 * ```ts
 * import { setDefaultStorageFactory, getDefaultStorage } from './storage';
 * 
 * // Override globally (e.g., in test setup for Cloudflare Workers)
 * setDefaultStorageFactory(() => new DurableObjectStorage(ctx));
 * 
 * // Get storage instance
 * const storage = getDefaultStorage();
 * ```
 */
let defaultStorageFactory: () => Storage = () => new MemoryStorage();

/**
 * Set the default storage factory. Call this before any tests run
 * to override the storage implementation globally.
 */
export function setDefaultStorageFactory(factory: () => Storage): void {
  defaultStorageFactory = factory;
}

/**
 * Get a new storage instance from the default factory.
 */
export function getDefaultStorage(): Storage {
  return defaultStorageFactory();
}

/**
 * Reset the default storage factory to MemoryStorage.
 */
export function resetDefaultStorageFactory(): void {
  defaultStorageFactory = () => new MemoryStorage();
}
