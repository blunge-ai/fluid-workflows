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

  const stop = runner.run(queue, [workflow]);
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
    .step(async ({ parentInput }, { dispatch }) => {
      return dispatch(child, { childInput: `input(${parentInput})` });;
    })
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const stop = runner.run(queue, [workflow, child]);
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { parentInput: 'XX' });
  // Wait for job processing to complete
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();

  expect(result.output).toBe('output(child(input(XX)))');
});
