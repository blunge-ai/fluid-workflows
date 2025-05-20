import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { WorkflowJobData } from '~/WorkflowJob';
import type { JobQueueEngine } from '~/JobQueueEngine';
import { JobQueueWorkflowRunner } from '~/JobQueueWorkflowRunner';
import { WorkflowDispatcher } from '~/WorkflowDispatcher';
import { BullMqJobQueueEngine } from '~/BullMqJobQueueEngine';

function setup() {
  const engine = new BullMqJobQueueEngine<unknown, unknown, unknown, unknown>({ 
    attempts: 1, 
    lockTimeoutMs: 5000 
  });
  const dispatcher = new WorkflowDispatcher(engine, { queue: 'default-queue' });
  const runner = new JobQueueWorkflowRunner(engine);
  return { engine, dispatcher, runner };
}

test('run step', async () => {
  const { engine, dispatcher, runner } = setup();

  const workflow = Workflow
    .create<{ a: number, b: number }>({ name: 'add-a-and-b', version: 1 })
    .step<{ c: number }>(async ({ a, b }) => {
      return { c: a + b };
    });

  const stop = runner.run([workflow]);
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 });
  // Wait for job processing to complete
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { engine, dispatcher, runner } = setup();

  const child = Workflow
    .create<{ childInput: string }>({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create<{ inputString: string }>({ name: 'parent-workflow', version: 1 })
    .step(async ({ inputString }, { dispatch }) => {
      return dispatch(child, { childInput: `input(${inputString})` });;
    })
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const stop = runner.run([workflow]);
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { inputString: 'XX' });
  // Wait for job processing to complete
  await new Promise(resolve => setTimeout(resolve, 100));
  await stop();

  expect(result.output).toBe('output(child(input(XX)))');
});
