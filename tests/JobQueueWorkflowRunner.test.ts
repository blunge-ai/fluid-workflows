import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { JobQueueWorkflowRunner } from '~/JobQueueWorkflowRunner';
import { JobQueueWorkflowDispatcher } from '~/JobQueueWorkflowDispatcher';
import { BullMqJobQueueEngine } from '~/BullMqJobQueueEngine';
import { Config } from '~/Config';
import { v4 as uuidv4 } from 'uuid';
import { timeout } from '~/utils';

function setup() {
  const engine = new BullMqJobQueueEngine({ 
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
    .step(async ({ a, b }) => {
      return { c: a + b };
    });

  const config = new Config({ engine, workflows: [workflow], queues: { 'add-a-and-b': queue } });
  const runner = new JobQueueWorkflowRunner(config);
  const dispatcher = new JobQueueWorkflowDispatcher(config);
  const stop = runner.run('all');
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { engine, queue } = setup();

  const child = Workflow
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create({ name: 'parent-workflow', version: 1 })
    .step(async ({ parentInput }: { parentInput: string }) => {
      return { childInput: `input(${parentInput})` };
    })
    .childStep(child)
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

test('run two named children', async () => {
  const { engine } = setup();

  const child1 = Workflow
    .create({ name: 'child1', version: 1 })
    .step(async ({ a }) => {
      return { a2: a * 2 };
    });

  const child2 = Workflow
    .create({ name: 'child2', version: 1 })
    .step(async ({ s }) => {
      return { s2: `child2(${s})` };
    });

  const parent = Workflow
    .create({ name: 'parent-two-children', version: 1 })
    .step(async ({ n }) => {
      return { one: { a: n + 1 }, two: { s: `n=${n}` } };
    })
    .childStep({ one: child1, two: child2 })
    .step(async ({ one, two }) => {
      return { out: `${one.a2}-${two.s2}` };
    });

  const q = Workflow
    .create({ name: 'q', version: 1 })
    .step(async ({ n }) => ({ out: n }));

  const queues = { 'parent-two-children': 'queue-a', child1: 'queue-b', child2: 'queue-c' } as const;
  const config3 = new Config({ engine, workflows: [parent], queues });
  const runner = new JobQueueWorkflowRunner(config3);
  const dispatcher = new JobQueueWorkflowDispatcher(config3);
  const stop = runner.run(['queue-a', 'queue-b', 'queue-c']);
  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await timeout(100);
  await stop();

  expect(result.out).toBe('12-child2(n=5)');
});
