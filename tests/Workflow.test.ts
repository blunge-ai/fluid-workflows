import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { InMemoryWorkflowRunner } from '~/InMemoryWorkflowRunner';

test('run', async () => {
  const runner = new InMemoryWorkflowRunner();

  const workflow = Workflow
    .create<{ a: number, b: number }>({ name: 'add-a-and-b', version: 1 })
    .run<{ c: number }>(async ({ a, b }) => {
      return { c: a + b };
    });

  const result = await runner.run(workflow.input({ a: 12, b: 34 }));

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const runner = new InMemoryWorkflowRunner();

  const child = Workflow
    .create<{ childInput: string }>({ name: 'child-workflow', version: 1 })
    .run(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create<{ inputString: string }>({ name: 'add-a-and-b', version: 1 })
    .run(async ({ inputString }) => {
      return child.input({ childInput: `input(${inputString})` });;
    })
    .run(async ({ childOutput }) => {
      return { result: `output(${childOutput})` };
    });

  const result = await runner.run(workflow.input({ inputString: 'XX' }));

  expect(result.result).toBe('output(child(input(XX)))');
});
