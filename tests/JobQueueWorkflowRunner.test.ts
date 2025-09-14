import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { JobQueueWorkflowRunner } from '~/JobQueueWorkflowRunner';
import { WorkflowDispatcher } from '~/WorkflowDispatcher';
import { BullMqJobQueueEngine } from '~/BullMqJobQueueEngine';
import { v4 as uuidv4 } from 'uuid';

function setup() {
  const engine = new BullMqJobQueueEngine({ 
    attempts: 1,
    lockTimeoutMs: 5000,
    blockingTimeoutSecs: 0.1,
  });
  const queue = `queue-${uuidv4()}`;
  const runner = new JobQueueWorkflowRunner(engine);
  return { engine, runner, queue };
}

test('run step', async () => {
  const { engine, runner, queue } = setup();

  const workflow = Workflow
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }) => {
      return { c: a + b };
    });

  const dispatcher = new WorkflowDispatcher(engine, [workflow], { queue });
  const stop = runner.run(workflow, { queue });
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { engine, runner, queue } = setup();

  const child = Workflow
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create({ name: 'parent-workflow', version: 1 })
    .step(async ({ parentInput }) => {
      return { childInput: `input(${parentInput})` };
    })
    .childStep(child)
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const queues = {
    'parent-workflow': queue,
    'child-workflow': queue
  };

  const dispatcher = new WorkflowDispatcher(engine, [workflow, child], { queue });
  const stopParent = runner.run(workflow, { queues });
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { parentInput: 'XX' });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stopParent();

  expect(result.output).toBe('output(child(input(XX)))');
});

test('run two named children', async () => {
  const { engine, runner, queue } = setup();

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

  const dispatcher = new WorkflowDispatcher(engine, [parent, child1, child2], { queue });
  const stop = runner.run(parent, { queue });
  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();

  expect(result.out).toBe('12-child2(n=5)');
});
