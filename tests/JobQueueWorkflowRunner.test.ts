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
  const dispatcher = new WorkflowDispatcher(engine, { queue });
  const runner = new JobQueueWorkflowRunner(engine);
  return { dispatcher, runner, queue };
}

test('run step', async () => {
  const { dispatcher, runner, queue } = setup();

  const workflow = Workflow
    .create<{ a: number, b: number }>({ name: 'add-a-and-b', version: 1 })
    .step<{ c: number }>(async ({ a, b }) => {
      return { c: a + b };
    });

  const stop = runner.run(workflow, { queue });
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 });
  // Wait for job processing to complete
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();
  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { dispatcher, runner, queue } = setup();

  const child = Workflow
    .create<{ childInput: string }>({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create<{ parentInput: string }>({ name: 'parent-workflow', version: 1 })
    .step(async ({ parentInput }) => {
      return { childInput: `input(${parentInput})` };
    })
    .childStep(child)
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const stopParent = runner.run(workflow, { queue });
  const stopChild = runner.run(child, { queue });
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { parentInput: 'XX' });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stopParent();
  await stopChild();

  expect(result.output).toBe('output(child(input(XX)))');
});

test('run two named children', async () => {
  const { dispatcher, runner, queue } = setup();

  const child1 = Workflow
    .create<{ a: number }>({ name: 'child1', version: 1 })
    .step(async ({ a }) => {
      return { a2: a * 2 };
    });

  const child2 = Workflow
    .create<{ s: string }>({ name: 'child2', version: 1 })
    .step(async ({ s }) => {
      return { s2: `child2(${s})` };
    });

  const parent = Workflow
    .create<{ n: number }>({ name: 'parent-two-children', version: 1 })
    .step(async ({ n }) => {
      return { one: { a: n + 1 }, two: { s: `n=${n}` } };
    })
    .childStep({ one: child1, two: child2 })
    .step(async ({ one, two }) => {
      return { out: `${one.a2}-${two.s2}` };
    });

  const stopParent = runner.run(parent, { queue });
  const stopChild1 = runner.run(child1, { queue });
  const stopChild2 = runner.run(child2, { queue });
  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await new Promise(resolve => setTimeout(resolve, 100));
  await stopParent();
  await stopChild1();
  await stopChild2();

  expect(result.out).toBe('12-child2(n=5)');
});
