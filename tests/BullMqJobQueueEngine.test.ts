import { expect, test } from 'vitest'
import { assert } from '~/utils';
import { JobResultStatus, JobData } from '~/JobQueueEngine';
import { BullMqJobQueueEngine } from '~/BullMqJobQueueEngine';

test('submit job', async () => {
  const engine = new BullMqJobQueueEngine<string, string, string, string>({
    attempts: 1,
    lockTimeoutMs: 8000
  });
  const token = 'test-token';
  const queue = 'test-queue';
  let submitPromise: Promise<void> | undefined;
  const resultStatusPromise = new Promise<JobResultStatus<string>>((resolve) => {
    submitPromise = engine.submitJob({
      data: { id: 'job1', meta: 'meta', input: 'input-data' },
      queue,
      statusHandler: (status) => {
        if (status.type === 'success') {
          resolve(status);
        }
      }
    });
  });
  await submitPromise;
  const job = await engine.acquireJob({ queue, token });
  expect(job).toBeDefined();
  assert(job);
  expect(job.id).toBe('job1');
  expect(job.input).toBe('input-data');
  expect(job.meta).toBe('meta');
  const successResult = { type: 'success' as const, output: 'output' };
  await engine.completeJob({ queue, token, jobId: job.id, result: successResult });
  const resultStatus = await resultStatusPromise;
  expect(resultStatus.type).toBe('success');
  assert(resultStatus.type === 'success');
  const result = await engine.getJobResult({ resultKey: resultStatus.resultKey, delete: true });
  expect(result.type).toBe('success');
  assert(result.type === 'success');
  expect(result.output).toBe('output');
});

test('submit children', async () => {
  const engine = new BullMqJobQueueEngine<string, string, string, string>({
    attempts: 1,
    lockTimeoutMs: 8000
  });
  const token = 'test-token2';
  const queue = 'test-queue2';
  let submitPromise: Promise<void> | undefined;
  const resultStatusPromise = new Promise<JobResultStatus<string>>((resolve) => {
    submitPromise = engine.submitJob({
      data: { id: 'parent', meta: 'meta', input: 'input-data' },
      queue,
      statusHandler: (status) => {
        if (status.type === 'success') {
          resolve(status);
        }
      }
    });
  });
  await submitPromise;
  let job: JobData<string, string> | undefined = undefined;
  while (!job) {
    job = await engine.acquireJob({ queue, token });
  }
  assert(job);
  const results = await engine.submitChildrenSuspendParent({
    children: [
      { data: { id: 'child1', meta: 'child1-meta', input: 'child1-input' },
        queue: 'child1-queue' },
      { data: { id: 'child2', meta: 'child2-meta', input: 'child2-input' },
        queue: 'child2-queue' }
    ],
    token,
    parentId: job.id,
    parentQueue: queue,
  });
  expect(results).toBeUndefined();

  // complete child1
  const child1 = await engine.acquireJob({ queue: 'child1-queue', token });
  expect(child1).toBeDefined()
  assert(child1);
  await engine.completeJob({ queue: 'child1-queue', token, jobId: child1.id, result: { type: 'success', output: 'child1-output' }});

  // complete child2
  const child2 = await engine.acquireJob({ queue: 'child2-queue', token });
  expect(child2).toBeDefined()
  assert(child2);
  await engine.completeJob({ queue: 'child2-queue', token, jobId: child2.id, result: { type: 'success', output: 'child2-output' }});

  await engine.acquireJob({ queue, token });
  const results2 = await engine.submitChildrenSuspendParent({
    children: [
      { data: { id: 'child1', meta: 'child1-meta', input: 'child1-input' },
        queue: 'child1-queue' },
      { data: { id: 'child2', meta: 'child2-meta', input: 'child2-input' },
        queue: 'child2-queue' }
    ],
    token,
    parentId: job.id,
    parentQueue: queue,
  });
  expect(results2).toBeDefined();
  assert(results2);

  // child1 result
  expect(results2.child1?.type).toEqual('success');
  assert(results2.child1?.type === 'success');
  expect(results2.child1.output).toEqual('child1-output');

  // child2 result
  expect(results2.child2?.type).toEqual('success');
  assert(results2.child2?.type === 'success');
  expect(results2.child2.output).toEqual('child2-output');  
  console.log('XXXXXXXXXXXXXXXXXXXXXXXX');
  const successResult = { type: 'success' as const, output: 'done' };
  await engine.completeJob({ queue, token, jobId: job.id, result: successResult });
  const resultStatus = await resultStatusPromise;
  expect(resultStatus.type).toBe('success');
  assert(resultStatus.type === 'success');
  const result = await engine.getJobResult({ resultKey: resultStatus.resultKey, delete: true });
  expect(result.type).toBe('success');
  assert(result.type === 'success');
  expect(result.output).toBe('done');
});
