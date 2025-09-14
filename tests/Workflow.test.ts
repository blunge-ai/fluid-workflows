import { expect, test } from 'vitest'
import { z } from 'zod';
import { Workflow } from '~/Workflow';
import { InMemoryWorkflowRunner } from '~/InMemoryWorkflowRunner';

test('run step', async () => {
  const runner = new InMemoryWorkflowRunner();

  const workflow = Workflow
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }) => {
      return { c: a + b };
    });

  const result = await runner.run(workflow, { a: 12, b: 34 });

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {
  const runner = new InMemoryWorkflowRunner();

  const child = Workflow
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create({ name: 'parent-workflow', version: 1 })
    .step(async ({ inputString }) => {
      return { childInput: `input(${inputString})` };
    })
    .childStep(child)
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const result = await runner.run(workflow, { inputString: 'XX' });

  expect(result.output).toBe('output(child(input(XX)))');
});

test('zod input schema', async () => {
  const runner = new InMemoryWorkflowRunner();
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = Workflow
    .create({ name: 'sum-with-zod', version: 1, inputSchema: schema })
    .step(async (input) => {
      const { a, b } = input;
      return { sum: a + b };
    });

  const result = await runner.run(workflow, { a: 2, b: 3 });
  expect(result.sum).toBe(5);
});
