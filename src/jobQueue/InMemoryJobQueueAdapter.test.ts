import { expect, test } from 'vitest';
import { v4 as uuidv4 } from 'uuid';
import { InMemoryJobQueueAdapter } from '~/jobQueue/InMemoryJobQueueAdapter';
import type { JobData } from '~/jobQueue/JobQueueEngine';
import { assert } from '~/utils';

function setup() {
  const engine = new InMemoryJobQueueAdapter();
  const token = 'test-token';
  const queue = `queue-${uuidv4()}`;
  return { engine, token, queue };
}

test('submit job, status events, complete and get result', async () => {
  const { engine, token, queue } = setup();
  const dataKey = 'rk1';
  const statuses: string[] = [];
  let successKey: string | undefined;

  const job: JobData<{ a: number }, string> = { id: 'job-1', meta: 'meta1', input: { a: 1 } };
  await engine.submitJob({
    queue,
    data: job,
    dataKey,
    statusHandler: (s) => {
      statuses.push(s.type);
      if (s.type === 'success') successKey = s.resultKey;
    }
  });

  const { data } = await engine.acquireJob<typeof job.input, string, never>({ queue, token });
  expect(data).toBeDefined();
  assert(data);
  expect(data.id).toBe('job-1');
  expect(data.meta).toBe('meta1');
  expect(data.input.a).toBe(1);

  // progress -> active status
  const upd = await engine.updateJob({ queue, token, jobId: data.id, progressInfo: { p: 1 } });
  expect(upd.interrupt).toBe(false);

  await engine.completeJob({ queue, token, jobId: data.id, result: { type: 'success', output: 'done' } });

  expect(successKey).toBe(dataKey);
  const result = await engine.getJobResult<string>({ resultKey: successKey!, delete: true });
  expect(result.type).toBe('success');
  if (result.type === 'success') expect(result.output).toBe('done');

  expect(statuses[0]).toBe('queued');
  expect(statuses).toContain('active');
  expect(statuses[statuses.length - 1]).toBe('success');
});

test('submit children, parent suspends, resumes with child results', async () => {
  const { engine, token, queue } = setup();
  const q2 = `${queue}-2`;
  const q3 = `${queue}-3`;

  await engine.submitJob({ queue, data: { id: 'parent', meta: undefined, input: 'p-in' } });
  const { data: parent } = await engine.acquireJob<{},{},number>({ queue, token });
  expect(parent?.id).toBe('parent');
  assert(parent);

  const res = await engine.submitChildrenSuspendParent<number>({
    children: [
      { data: { id: 'child-1', meta: 'm1', input: 'c1-in' }, queue: q2 },
      { data: { id: 'child-2', meta: 'm2', input: 'c2-in' }, queue: q3 },
    ],
    token,
    parentId: parent.id,
    parentQueue: queue,
  });
  expect(res).toBeUndefined();

  const { data: c1 } = await engine.acquireJob<{},{},never>({ queue: q2, token });
  expect(c1?.id).toBe('child-1');
  await engine.completeJob({ queue: q2, token, jobId: 'child-1', result: { type: 'success', output: 10 } });

  const { data: c2 } = await engine.acquireJob<{},{},never>({ queue: q3, token });
  expect(c2?.id).toBe('child-2');
  await engine.completeJob({ queue: q3, token, jobId: 'child-2', result: { type: 'success', output: 20 } });

  const { data: parentAgain, childResults } = await engine.acquireJob<{},{},number>({ queue, token, block: true });
  expect(parentAgain?.id).toBe('parent');
  assert(childResults);
  const r1 = childResults['child-1'];
  const r2 = childResults['child-2'];
  expect(r1?.type).toBe('success');
  if (r1?.type === 'success') expect(r1.output).toBe(10);
  expect(r2?.type).toBe('success');
  if (r2?.type === 'success') expect(r2.output).toBe(20);

  await engine.completeJob({ queue, token, jobId: 'parent', result: { type: 'success', output: 'done' } });
});

test('subscribeToJobStatus before submit receives queued and terminal', async () => {
  const { engine, token, queue } = setup();
  const jobId = 'job-sub-1';
  const seen: string[] = [];
  const unsub = await engine.subscribeToJobStatus({ queue, jobId, statusHandler: (s) => { seen.push(s.type); } });

  await engine.submitJob({ queue, data: { id: jobId, meta: 'm', input: 1 } });
  const { data } = await engine.acquireJob<number, string, never>({ queue, token });
  assert(data);
  await engine.updateJob({ queue, token, jobId: data.id, progressInfo: { phase: 'x', progress: 0.3 } });
  await engine.completeJob({ queue, token, jobId: data.id, result: { type: 'success', output: 123 } });
  await unsub();

  expect(seen[0]).toBe('queued');
  expect(seen).toContain('active');
  expect(seen[seen.length - 1]).toMatch(/success|error|cancelled/);
});
