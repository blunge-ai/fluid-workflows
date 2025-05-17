import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { InMemoryJobQueue } from '~/InMemoryJobQueue';
import { WorkflowJobData } from '~/WorkflowJob';
import type { JobQueue } from '~/JobQueue';
import { JobQueueWorkflowRunner } from '~/JobQueueWorkflowRunner';
import { JobQueueWorkflowDispatcher } from '~/JobQueueWorkflowDispatcher';

function setup() {
  const queue = new InMemoryJobQueue<WorkflowJobData>('queue');
  const dispatcher = new JobQueueWorkflowDispatcher(<Input, Output>() => queue as JobQueue<Input, Output, unknown, unknown>);
  const runner = new JobQueueWorkflowRunner(queue);
  return { queue, dispatcher, runner };
}

test('run step', async () => {
  const { queue, dispatcher, runner } = setup();

  const workflow = Workflow
    .create<{ a: number, b: number }>({ name: 'add-a-and-b', version: 1 })
    .step<{ c: number }>(async ({ a, b }) => {
      return { c: a + b };
    });

  const stop = runner.run([workflow]);
  const result = await dispatcher.dispatchAwaitingOutput(workflow, { a: 12, b: 34 });
  queue.releaseBlockedCalls();
  await stop();

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const { queue, dispatcher, runner } = setup();

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
  queue.releaseBlockedCalls();
  await stop();

  expect(result.output).toBe('output(child(input(XX)))');
});
