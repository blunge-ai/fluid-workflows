import { expect, test } from 'vitest'
import { assert, timeout } from '~/utils';
import { JobResultStatus } from '~/JobQueueEngine';
import { BullMqJobQueueEngine } from '~/BullMqJobQueueEngine';
import { v4 as uuidv4 } from 'uuid';

test('submit job', async () => {
  const engine = new BullMqJobQueueEngine({
    attempts: 1,
    lockTimeoutMs: 8000
  });
  const token = 'test-token';
  const queue = `queue-${uuidv4()}`;
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
  const { data: job } = await engine.acquireJob({ queue, token });
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
  const engine = new BullMqJobQueueEngine({
    attempts: 1,
    lockTimeoutMs: 8000
  });
  const token = 'test-token2';
  const queue = `queue-${uuidv4()}`;
  const queue2 = `queue2-${uuidv4()}`;
  const queue3 = `queue3-${uuidv4()}`;
  let submitPromise: Promise<void> | undefined;
  const resultStatusPromise = new Promise<JobResultStatus<string>>((resolve) => {
    submitPromise = engine.submitJob({
      data: { id: `parent-${uuidv4()}`, meta: 'meta', input: 'input-data' },
      queue,
      statusHandler: (status) => {
        if (status.type === 'success') {
          resolve(status);
        }
      }
    });
  });
  await submitPromise;
  let { data: job } = await engine.acquireJob({ queue, token });
  assert(job);

  const results = await engine.submitChildrenSuspendParent({
    children: [
      { data: { id: `child1-${uuidv4()}`, meta: 'child1-meta', input: 'child1-input' },
        queue: queue2 },
      { data: { id: `child2-${uuidv4()}`, meta: 'child2-meta', input: 'child2-input' },
        queue: queue3 }
    ],
    token,
    parentId: job.id,
    parentQueue: queue,
  });
  expect(results).toBeUndefined();

  // complete child1
  const { data: child1 } = await engine.acquireJob({ queue: queue2, token });
  expect(child1).toBeDefined()
  assert(child1);
  await engine.completeJob({ queue: queue2, token, jobId: child1.id, result: { type: 'success', output: 'child1-output' }});

  // complete child2
  const { data: child2 } = await engine.acquireJob({ queue: queue3, token });
  expect(child2).toBeDefined()
  assert(child2);
  await engine.completeJob({ queue: queue3, token, jobId: child2.id, result: { type: 'success', output: 'child2-output' }});

  const { data: parentAgain, childResults: results2 } = await engine.acquireJob({ queue, token, block: true });
  assert(parentAgain);

  expect(results2).toBeDefined();
  assert(results2);

  // child1 result
  const result1 = results2[child1.id];
  expect(result1?.type).toEqual('success');
  assert(result1?.type === 'success');
  expect(result1.output).toEqual('child1-output');

  // child2 result
  const result2 = results2[child2.id];
  expect(result2?.type).toEqual('success');
  assert(result2?.type === 'success');
  expect(result2.output).toEqual('child2-output');

  const successResult = { type: 'success' as const, output: 'done' };
  await engine.completeJob({ queue, token, jobId: parentAgain.id, result: successResult });

  const resultStatus = await resultStatusPromise;
  expect(resultStatus.type).toBe('success');
  assert(resultStatus.type === 'success');

  const result = await engine.getJobResult({ resultKey: resultStatus.resultKey, delete: true });
  expect(result.type).toBe('success');
  assert(result.type === 'success');
  expect(result.output).toBe('done');
});
