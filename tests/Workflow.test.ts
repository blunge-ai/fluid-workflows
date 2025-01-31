import { expect, test } from 'vitest'
import { Workflow } from '~/Workflow';
import { InMemoryWorkflowRunner } from '~/WorkflowRunner';

test('run', async () => {
  const runner = new InMemoryWorkflowRunner();

  const workflow = Workflow
    .create<{ a: number, b: number }>({ name: 'add-a-and-b' })
    .run<{ c: number }>(async ({ a, b }, runOpts) => {
      return { c: a + b };
    });

  const result = await runner.start(workflow.input({ a: 12, b: 34 }));

  expect(result.c).toBe(46);
});
