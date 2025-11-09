import { expect, test } from 'vitest';
import { createServer } from 'http';
import type { JobQueueEngine, JobData } from '~/JobQueueEngine';
import { HttpJobQueueEngineServer } from '~/HttpJobQueueEngineServer';
import { HttpJobQueueEngineClient } from '~/HttpJobQueueEngineClient';
import { InMemoryJobQueueAdapter } from '~/InMemoryJobQueueAdapter';

async function withServer(engine: JobQueueEngine, fn: (client: HttpJobQueueEngineClient) => Promise<void>) {
  const handler = new HttpJobQueueEngineServer(engine).handler;
  const server = createServer((req, res) => void handler(req, res));
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const addr = server.address();
  const port = typeof addr === 'object' && addr && 'port' in addr ? (addr as any).port : 0;
  const client = new HttpJobQueueEngineClient(`http://127.0.0.1:${port}`);
  try {
    await fn(client);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

test('submit/acquire/complete/get-result via HTTP + msgpack', async () => {
  const engine = new InMemoryJobQueueAdapter();
  await withServer(engine, async (client) => {
    const job: JobData<{ a: number }, string> = { id: 'job1', meta: 'meta1', input: { a: 1 } };
    await client.submitJob({ queue: 'q', data: job, dataKey: 'rk' });

    const acquired = await client.acquireJob<typeof job.input, string, number>({ queue: 'q', token: 't', block: false });
    expect(acquired.data?.id).toBe('job1');
    expect(acquired.data?.meta).toBe('meta1');
    expect(acquired.data?.input.a).toBe(1);

    const upd = await client.updateJob({ queue: 'q', token: 't', jobId: 'job1', progressInfo: { p: 1 } });
    expect(upd.interrupt).toBe(false);

    await client.completeJob({ queue: 'q', token: 't', jobId: 'job1', result: { type: 'success', output: 'done' } });

    const result = await client.getJobResult<string>({ resultKey: 'rk', delete: true });
    expect(result.type).toBe('success');
    if (result.type === 'success') {
      expect(result.output).toBe('done');
    }
  });
});

test('children suspend + resume over HTTP', async () => {
  const engine = new InMemoryJobQueueAdapter();
  await withServer(engine, async (client) => {
    const parent: JobData<{ x: number }, undefined> = { id: 'parent-1', meta: undefined, input: { x: 5 } };
    await client.submitJob({ queue: 'q', data: parent, dataKey: 'parent-key' });

    const { data: pJob } = await client.acquireJob<typeof parent.input, undefined, number>({ queue: 'q', token: 't', block: false });
    expect(pJob?.id).toBe('parent-1');

    const child1: JobData<{ y: number }, string> = { id: 'child-1', meta: 'm1', input: { y: 1 } };
    const child2: JobData<{ y: number }, string> = { id: 'child-2', meta: 'm2', input: { y: 2 } };

    const res = await client.submitChildrenSuspendParent<number>({
      children: [ { data: child1, queue: 'q2' }, { data: child2, queue: 'q3' } ],
      token: 't',
      parentId: 'parent-1',
      parentQueue: 'q',
    });
    expect(res).toBeUndefined();

    const { data: c1 } = await client.acquireJob<typeof child1.input, string, never>({ queue: 'q2', token: 't', block: false });
    expect(c1?.id).toBe('child-1');
    await client.completeJob({ queue: 'q2', token: 't', jobId: 'child-1', result: { type: 'success', output: 10 } });

    const { data: c2 } = await client.acquireJob<typeof child2.input, string, never>({ queue: 'q3', token: 't', block: false });
    expect(c2?.id).toBe('child-2');
    await client.completeJob({ queue: 'q3', token: 't', jobId: 'child-2', result: { type: 'success', output: 20 } });

    const { data: pAgain, childResults } = await client.acquireJob<typeof parent.input, undefined, number>({ queue: 'q', token: 't', block: true });
    expect(pAgain?.id).toBe('parent-1');
    expect(childResults).toBeDefined();
    expect(childResults?.['child-1']?.type).toBe('success');
    if (childResults?.['child-1']?.type === 'success') expect(childResults['child-1'].output).toBe(10);
    expect(childResults?.['child-2']?.type).toBe('success');
    if (childResults?.['child-2']?.type === 'success') expect(childResults['child-2'].output).toBe(20);

    await client.completeJob({ queue: 'q', token: 't', jobId: 'parent-1', result: { type: 'success', output: 'done' } });
    const out = await client.getJobResult<string>({ resultKey: 'parent-key', delete: true });
    expect(out.type).toBe('success');
    if (out.type === 'success') expect(out.output).toBe('done');
  });
});

test('server errors propagate to client', async () => {
  const engine = new InMemoryJobQueueAdapter();
  await withServer(engine, async (client) => {
    await expect(client.completeJob({ queue: 'q', token: 't', jobId: 'missing', result: { type: 'success', output: null } })).rejects.toThrow('job not found');
  });
});

test('NotImplemented for subscribe and submit statusHandler on HTTP client', async () => {
  const engine = new InMemoryJobQueueAdapter();
  await withServer(engine, async (client) => {
    await expect(client.subscribeToJobStatus({ queue: 'q', jobId: 'j', statusHandler: (_s: any) => {} })).rejects.toThrow('NotImplemented');
    await expect(client.submitJob({ queue: 'q', data: { id: 'j', meta: undefined as any, input: null }, statusHandler: (_s: any) => {} })).rejects.toThrow('NotImplemented');
  });
});
