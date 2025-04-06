import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { InMemoryJobQueue } from '~/InMemoryJobQueue';
import { JobQueueWorkflowRunner, WorkflowJobData } from '~/JobQueueWorkflowRunner';

test('run step', async () => {
  const jobQueue = new InMemoryJobQueue<WorkflowJobData>('queue');
  const runner = new JobQueueWorkflowRunner(jobQueue);

  const workflow = Workflow
    .create<{ a: number, b: number }>({ name: 'add-a-and-b', version: 1 })
    .step<{ c: number }>(async ({ a, b }) => {
      return { c: a + b };
    });

  const stop = runner.startWorkerProcess([workflow]);
  const result = await runner.run(workflow.withInput({ a: 12, b: 34 }));
  await stop;
  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const jobQueue = new InMemoryJobQueue<WorkflowJobData>('queue');
  const runner = new JobQueueWorkflowRunner(jobQueue);

  const child = Workflow
    .create<{ childInput: string }>({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create<{ inputString: string }>({ name: 'parent-workflow', version: 1 })
    .step(async ({ inputString }) => {
      return child.withInput({ childInput: `input(${inputString})` });;
    })
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const stop = runner.startWorkerProcess([workflow]);
  const result = await runner.run(workflow.withInput({ inputString: 'XX' }));
  await stop;
  expect(result.output).toBe('output(child(input(XX)))');
});
