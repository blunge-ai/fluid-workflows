import { expect, test, vi } from 'vitest';
import { BestEffortWrapper } from './BestEffortWrapper';
import type { Storage } from './Storage';
import type { Logger } from '../utils';

function createThrowingStorage(): Storage {
  return {
    lock: async () => {
      throw new Error('lock');
    },
    unlock: async () => {
      throw new Error('unlock');
    },
    refreshLock: async () => {
      throw new Error('refreshLock');
    },
    updateState: async () => {
      throw new Error('updateState');
    },
    getState: async () => {
      throw new Error('getState');
    },
    getActiveJobs: async () => {
      throw new Error('getActiveJobs');
    },
    setResult: async () => {
      throw new Error('setResult');
    },
    getResult: async () => {
      throw new Error('getResult');
    },
    waitForLock: async () => {
      throw new Error('waitForLock');
    },
    cleanup: async () => {
      throw new Error('cleanup');
    },
    subscribe: () => {
      throw new Error('subscribe');
    },
    close: async () => {
      throw new Error('close');
    },
  } as Storage;
}

test('ignores errors for all storage methods and returns fallbacks', async () => {
  const logger: Logger = {
    error: vi.fn(),
    warn: vi.fn(),
    info: vi.fn(),
  };
  const storage = new BestEffortWrapper({ storage: createThrowingStorage(), logger });

  const lockResult = await storage.lock('job-1', 1000);
  expect(lockResult.acquired).toBe(true);
  if (lockResult.acquired) {
    expect(lockResult.token).toContain('best-effort-');
  }

  await expect(storage.unlock('job-1', 'token')).resolves.toBe(true);
  await expect(storage.refreshLock('job-1', 'token', 1000)).resolves.toBe(true);
  await expect(storage.updateState('job-1', { ttlMs: 1000 })).resolves.toBeUndefined();
  await expect(storage.getState('job-1')).resolves.toBeUndefined();
  await expect(storage.getActiveJobs()).resolves.toEqual([]);
  await expect(storage.setResult('job-1', { ok: true }, { ttlMs: 1000 })).resolves.toBeUndefined();
  await expect(storage.getResult('job-1')).resolves.toBeUndefined();
  await expect(storage.waitForLock('job-1', 1000)).resolves.toBe(true);
  await expect(storage.cleanup()).resolves.toBe(0);

  const unsubscribe = storage.subscribe('job-1', () => {});
  expect(() => unsubscribe()).not.toThrow();

  await expect(storage.close()).resolves.toBeUndefined();

  expect(logger.error).toHaveBeenCalled();
  expect(logger.error).toHaveBeenCalledWith(
    expect.objectContaining({ method: 'lock', jobId: 'job-1' }),
    'best-effort storage: ignored storage error',
  );
  expect(logger.error).toHaveBeenCalledWith(
    expect.objectContaining({ method: 'subscribe', jobId: 'job-1' }),
    'best-effort storage: ignored storage error',
  );
});

test('ignores errors thrown by unsubscribe callback', () => {
  const logger: Logger = {
    error: vi.fn(),
    warn: vi.fn(),
    info: vi.fn(),
  };
  const storage = new BestEffortWrapper({
    storage: {
      ...createThrowingStorage(),
      subscribe: () => () => {
        throw new Error('unsubscribe');
      },
    } as Storage,
    logger,
  });

  const unsubscribe = storage.subscribe('job-1', () => {});
  expect(() => unsubscribe()).not.toThrow();
  expect(logger.error).toHaveBeenCalledWith(
    expect.objectContaining({ method: 'unsubscribe', jobId: 'job-1' }),
    'best-effort storage: ignored storage error',
  );
});
