/**
 * Cloudflare Workers test setup.
 * 
 * This file is loaded before tests when running with vitest.cloudflare.config.ts.
 * It overrides the default storage to use DurableObjectStorage.
 */
import { env } from 'cloudflare:test';
import { setDefaultStorage } from '../storage/defaultStorage';
import type { TestDurableObject } from './cloudflare-worker';
import type { Storage } from '../storage/Storage';

declare module 'cloudflare:test' {
  interface ProvidedEnv {
    TEST_DO: DurableObjectNamespace<TestDurableObject>;
  }
}

/**
 * Creates a Storage implementation that proxies to the Durable Object via RPC.
 */
function createDOStorageProxy(stub: DurableObjectStub<TestDurableObject>): Storage {
  return {
    updateState: (jobId, opts) => stub.updateState(jobId, opts),
    getState: (jobId) => stub.getState(jobId),
    getActiveJobs: () => stub.getActiveJobs(),
    setResult: (jobId, result, opts) => stub.setResult(jobId, result, opts),
    getResult: (jobId) => stub.getResult(jobId),
    lock: () => Promise.resolve({ acquired: true, token: 'do-proxy-noop' }),
    unlock: () => Promise.resolve(true),
    refreshLock: () => Promise.resolve(true),
    waitForLock: () => Promise.resolve(true),
    cleanup: () => stub.cleanup(),
    close: () => stub.close(),
  };
}

// Create a unique DO for this test run
const testRunId = `test-${Date.now()}-${Math.random().toString(36).slice(2)}`;
const id = env.TEST_DO.idFromName(testRunId);
const stub = env.TEST_DO.get(id);

// Override default storage to use the DO
setDefaultStorage(createDOStorageProxy(stub));
