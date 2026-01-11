import { expect, test } from 'vitest'
import { z } from 'zod';
import { Workflow } from '~/Workflow';
import { WorkflowRunner } from '~/index';
test('run step', async () => {
  const workflow = Workflow
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const runner = new WorkflowRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  const result = await runner.run(workflow, { a: 12, b: 34 });
  expect(result.c).toBe(46);
});

test('complete finishes the workflow early', async () => {
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = Workflow
    .create({ name: 'complete', version: 1, inputSchema: schema })
    .step(async ({ a, b }, { complete }) => {
      if (a + b === 5) {
        return complete({ sum: 5 });
      }
      return { sum: a + b };
    })
    .step(async () => {
      return { sum: 9999 };
    });

  const runner = new WorkflowRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  const result = await runner.run(workflow, { a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('restart restarts from the beginning', async () => {
  const schema = z.object({ iterations: z.number(), value: z.number() });

  const workflow = Workflow
    .create({ name: 'restart', version: 1, inputSchema: schema })
    .step(async (input, { restart }) => {
      if (input.iterations > 0) {
        return restart({ iterations: input.iterations - 1, value: input.value + 1 });
      }
      return { after: input.value };
    })
    .step(async ({ after }) => {
      return { out: after };
    });

  const runner = new WorkflowRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  const result = await runner.run(workflow, { iterations: 3, value: 10 });
  expect(result.out).toBe(13);
});
