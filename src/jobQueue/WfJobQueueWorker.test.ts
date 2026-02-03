import { expect, test } from 'vitest'
import { z } from 'zod';
import { WfBuilder } from '~/WfBuilder';
import { WfJobQueueWorker } from '~/jobQueue/WfJobQueueWorker';
import { JobQueueWorkflowDispatcher } from '~/jobQueue/JobQueueWorkflowDispatcher';
import { BullMqAdapter } from '~/jobQueue/BullMqAdapter';
import { JobQueueConfig } from '~/jobQueue/JobQueueConfig';
import { v4 as uuidv4 } from 'uuid';
import { timeout } from '~/utils';

function setup() {
  const engine = new BullMqAdapter({ 
    attempts: 1,
    jobTimeoutMs: 15000,
    blockingTimeoutSecs: 0.1,
  });
  const queue = `queue-${uuidv4()}`;
  return { engine, queue };
}

test('run step', async () => {
  const { engine, queue } = setup();

  const workflow = WfBuilder
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const config = new JobQueueConfig({ engine, workflows: [workflow], queues: { 'add-a-and-b': queue }, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 } as { a: number, b: number });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { engine, queue } = setup();

  const child = WfBuilder
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }: { childInput: string }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = WfBuilder
    .create({ name: 'parent-workflow', version: 1 })
    .step(async ({ parentInput }: { parentInput: string }) => {
      return { childInput: `input(${parentInput})` };
    })
    .step(child)
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const queues = { 'parent-workflow': queue, 'child-workflow': queue };
  const config2 = new JobQueueConfig({ engine, workflows: [workflow, child], queues, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config2);
  const dispatcher = new JobQueueWorkflowDispatcher(config2);
  const stop = worker.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { parentInput: 'XX' });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();

  expect(result.output).toBe('output(child(input(XX)))');
});

test('run parallel children', async () => {
  const { engine } = setup();

  const child1 = WfBuilder
    .create({ name: 'child1', version: 1 })
    .step(async ({ n, n2 }: { n: number, n2: number }) => {
      return { a2: (n + 1) * 2 };
    });

  const child2 = WfBuilder
    .create({ name: 'child2', version: 1 })
    .step(async ({ n, n2 }: { n: number, n2: number }) => {
      return { s2: `child2(n=${n})` };
    });

  const parent = WfBuilder
    .create({ name: 'parent-two-children', version: 1 })
    .step(async ({ n }: { n: number }) => {
      return { n2: n };
    })
    .parallel({ one: child1, two: child2 })
    .step(async ({ one, two }) => {
      return { out: `${one.a2}-${two.s2}` };
    });

  const queues = { 'parent-two-children': 'queue-a', child1: 'queue-b', child2: 'queue-c' } as const;
  const config = new JobQueueConfig({ engine, workflows: [parent], queues, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run(['queue-a', 'queue-b', 'queue-c']);
  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await timeout(100);
  await stop();

  expect(result.out).toBe('12-child2(n=5)');
});

test('restart restarts from the beginning in WfJobQueueWorker', async () => {
  const { engine, queue } = setup();

  const schema = z.object({ iterations: z.number(), value: z.number() });

  const workflow = WfBuilder
    .create({ name: 'restart-runner', version: 1, inputSchema: schema })
    .step(async (input, { restart }) => {
      if (input.iterations > 0) {
        return restart({ iterations: input.iterations - 1, value: input.value + 1 });
      }
      return { after: input.value };
    })
    .step(async ({ after }) => {
      return { out: after };
    });

  const config = new JobQueueConfig({ engine, workflows: [workflow], queues: { 'restart-runner': queue }, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { iterations: 3, value: 10 });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.out).toBe(13);
});

test('output schema validation', async () => {
  const { engine, queue } = setup();

  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const workflow = WfBuilder
    .create({ name: 'output-schema-jq', version: 1, inputSchema, outputSchema })
    .step(async ({ a }) => ({ result: a * 2 }));

  const config = new JobQueueConfig({ engine, workflows: [workflow], queues: { 'output-schema-jq': queue }, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 5 });
  await timeout(100);
  await stop();
  expect(result.result).toBe(10);
});

test('output schema validation rejects invalid output', async () => {
  const { engine, queue } = setup();

  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const workflow = WfBuilder
    .create({ name: 'output-invalid-jq', version: 1, inputSchema, outputSchema })
    .step(async ({ a }) => ({ result: 'not-a-number' as any }));

  const config = new JobQueueConfig({ engine, workflows: [workflow], queues: { 'output-invalid-jq': queue }, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run('all');
  await expect(dispatcher.dispatchAwaitingOutput(workflow, { a: 5 })).rejects.toThrow();
  await timeout(100);
  await stop();
});

test('output schema validation with complete()', async () => {
  const { engine, queue } = setup();

  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const workflow = WfBuilder
    .create({ name: 'complete-schema-jq', version: 1, inputSchema, outputSchema })
    .step(async ({ a }, { complete }) => complete({ result: a * 2 }))
    .step(async () => ({ result: 9999 }));

  const config = new JobQueueConfig({ engine, workflows: [workflow], queues: { 'complete-schema-jq': queue }, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 5 });
  await timeout(100);
  await stop();
  expect(result.result).toBe(10);
});

test('parallel dispatches all workflows before rethrowing error', async () => {
  const { engine } = setup();
  const dispatchedWorkflows: string[] = [];

  const child1 = WfBuilder
    .create({ name: 'parallel-err-child1', version: 1 })
    .step(async ({ n }: { n: number }) => {
      dispatchedWorkflows.push('child1');
      await timeout(50);
      throw new Error('child1 failed');
    });

  const child2 = WfBuilder
    .create({ name: 'parallel-err-child2', version: 1 })
    .step(async ({ n }: { n: number }) => {
      dispatchedWorkflows.push('child2');
      await timeout(100);
      return { result: n * 2 };
    });

  const parent = WfBuilder
    .create({ name: 'parallel-err-parent', version: 1 })
    .step(async ({ n }: { n: number }) => ({ n }))
    .parallel({ one: child1, two: child2 });

  const queues = { 
    'parallel-err-parent': 'queue-err-a', 
    'parallel-err-child1': 'queue-err-b', 
    'parallel-err-child2': 'queue-err-c',
  } as const;
  const config = new JobQueueConfig({ engine, workflows: [parent], queues, jobTimeoutMs: 30000 });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run(['queue-err-a', 'queue-err-b', 'queue-err-c']);
  
  await expect(dispatcher.dispatchAwaitingOutput(parent, { n: 5 })).rejects.toThrow();
  await timeout(150);
  await stop();

  // Both workflows should have been dispatched even though child1 failed
  expect(dispatchedWorkflows).toContain('child1');
  expect(dispatchedWorkflows).toContain('child2');
});
