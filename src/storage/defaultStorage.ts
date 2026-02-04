import type { Storage } from './Storage';
import { MemoryStorage } from './MemoryStorage';

/**
 * Default storage instance used by tests and when no storage is explicitly provided.
 * Override this in test setup to use a different storage implementation.
 */
export let defaultStorage: Storage<any, any> = new MemoryStorage();

/**
 * Set the default storage instance.
 * Call this in test setup to override the storage implementation.
 */
export function setDefaultStorage(storage: Storage<any, any>): void {
  defaultStorage = storage;
}

/**
 * Reset the default storage to a fresh MemoryStorage instance.
 */
export function resetDefaultStorage(): void {
  defaultStorage = new MemoryStorage();
}
