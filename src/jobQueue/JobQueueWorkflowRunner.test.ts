import { expect, test } from 'vitest'
import { z } from 'zod';
import { Workflow } from '~/Workflow';
import { JobQueueWorkflowRunner } from '~/jobQueue/JobQueueWorkflowRunner';
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

  const workflow = Workflow
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const config = new Config({ engine, workflows: [workflow], queues: { 'add-a-and-b': queue } });
  const runner = new JobQueueWorkflowRunner(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = runner.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 } as { a: number, b: number });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { engine, queue } = setup();

  const child = Workflow
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }: { childInput: string }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
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
  const runner = new JobQueueWorkflowRunner(config2);
  const dispatcher = new JobQueueWorkflowDispatcher(config2);
  const stop = runner.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { parentInput: 'XX' });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();

  expect(result.output).toBe('output(child(input(XX)))');
});

test('run parallel children', async () => {
  const { engine } = setup();

  const child1 = Workflow
    .create({ name: 'child1', version: 1 })
    .step(async ({ n, n2 }: { n: number, n2: number }) => {
      return { a2: (n + 1) * 2 };
    });

  const child2 = Workflow
    .create({ name: 'child2', version: 1 })
    .step(async ({ n, n2 }: { n: number, n2: number }) => {
      return { s2: `child2(n=${n})` };
    });

  const parent = Workflow
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
  const runner = new JobQueueWorkflowRunner(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = runner.run(['queue-a', 'queue-b', 'queue-c']);
  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await timeout(100);
  await stop();

  expect(result.out).toBe('12-child2(n=5)');
});

test('restart restarts from the beginning in JobQueueWorkflowRunner', async () => {
  const { engine, queue } = setup();

  const schema = z.object({ iterations: z.number(), value: z.number() });

  const workflow = Workflow
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
  const runner = new JobQueueWorkflowRunner(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = runner.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { iterations: 3, value: 10 });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.out).toBe(13);
});
