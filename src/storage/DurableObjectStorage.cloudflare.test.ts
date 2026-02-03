import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import type { TestDurableObject } from '../test/cloudflare-worker';

declare module 'cloudflare:test' {
  interface ProvidedEnv {
    TEST_DO: DurableObjectNamespace<TestDurableObject>;
  }
}

describe('DurableObjectStorage', () => {
  let stub: DurableObjectStub<TestDurableObject>;

  beforeEach(async () => {
    // Get a unique DO for each test to ensure isolation
    const testName = `test-${Date.now()}-${Math.random()}`;
    const id = env.TEST_DO.idFromName(testName);
    stub = env.TEST_DO.get(id);
  });

  it('should store and retrieve state', async () => {
    const jobId = 'test-job-1';
    const state = { step: 1, data: 'hello' };

    await stub.updateState(jobId, { state, ttlMs: 60000 });

    const retrieved = await stub.getState(jobId);
    expect(retrieved).toBeDefined();
    expect(retrieved?.state).toEqual(state);
    expect(retrieved?.lastUpdatedAt).toBeGreaterThan(0);
  });

  it('should return undefined for non-existent job', async () => {
    const result = await stub.getState('non-existent');
    expect(result).toBeUndefined();
  });

  it('should get active jobs sorted by lastUpdatedAt', async () => {
    // Create jobs with slight delays to ensure different timestamps
    await stub.updateState('job-1', { state: { order: 1 }, ttlMs: 60000 });
    await new Promise(r => setTimeout(r, 10));
    await stub.updateState('job-2', { state: { order: 2 }, ttlMs: 60000 });
    await new Promise(r => setTimeout(r, 10));
    await stub.updateState('job-3', { state: { order: 3 }, ttlMs: 60000 });

    const activeJobs = await stub.getActiveJobs();

    expect(activeJobs).toHaveLength(3);
    expect(activeJobs[0]?.jobId).toBe('job-1');
    expect(activeJobs[1]?.jobId).toBe('job-2');
    expect(activeJobs[2]?.jobId).toBe('job-3');
  });

  it('should remove job from active list when result is set', async () => {
    const jobId = 'result-job';
    await stub.updateState(jobId, { state: { step: 1 }, ttlMs: 60000 });

    let activeJobs = await stub.getActiveJobs();
    expect(activeJobs.find(j => j.jobId === jobId)).toBeDefined();

    await stub.setResult(jobId, { success: true }, { ttlMs: 60000 });

    activeJobs = await stub.getActiveJobs();
    expect(activeJobs.find(j => j.jobId === jobId)).toBeUndefined();

    // State should also be removed
    const state = await stub.getState(jobId);
    expect(state).toBeUndefined();
  });

  it('should update existing state', async () => {
    const jobId = 'update-job';

    await stub.updateState(jobId, { state: { step: 1 }, ttlMs: 60000 });
    const first = await stub.getState(jobId);

    await stub.updateState(jobId, { state: { step: 2 }, ttlMs: 60000 });
    const second = await stub.getState(jobId);

    expect(second?.state).toEqual({ step: 2 });
    expect(second?.lastUpdatedAt).toBeGreaterThanOrEqual(first!.lastUpdatedAt);
  });
});
