import { expect, test } from 'vitest'
import { z } from 'zod';
import { WfBuilder } from '~/WfBuilder';
import { WfJobQueueWorker } from '~/jobQueue/WfJobQueueWorker';
import { JobQueueWorkflowDispatcher } from '~/jobQueue/JobQueueWorkflowDispatcher';
import { BullMqAdapter } from '~/jobQueue/BullMqAdapter';
import { Config } from '~/Config';
import { v4 as uuidv4 } from 'uuid';
import { timeout } from '~/utils';

function setup() {
  const engine = new BullMqAdapter({ 
    attempts: 1,
    lockTimeoutMs: 5000,
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

  const config = new Config({ engine, workflows: [workflow], queues: { 'add-a-and-b': queue } });
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
  const config2 = new Config({ engine, workflows: [workflow, child], queues });
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
  const config = new Config({ engine, workflows: [parent], queues });
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

  const config = new Config({ engine, workflows: [workflow], queues: { 'restart-runner': queue } });
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

  const config = new Config({ engine, workflows: [workflow], queues: { 'output-schema-jq': queue } });
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

  const config = new Config({ engine, workflows: [workflow], queues: { 'output-invalid-jq': queue } });
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

  const config = new Config({ engine, workflows: [workflow], queues: { 'complete-schema-jq': queue } });
  const worker = new WfJobQueueWorker(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = worker.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 5 });
  await timeout(100);
  await stop();
  expect(result.result).toBe(10);
});
