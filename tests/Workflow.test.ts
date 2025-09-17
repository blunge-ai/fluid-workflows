import { expect, test } from 'vitest'
import { z } from 'zod';
import { Workflow, runQueueless, withCompleteWrapper } from '~/Workflow';

test('run step', async () => {

  const workflow = Workflow
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }) => {
      return { c: a + b };
    });

  const result = await runQueueless(workflow, { a: 12, b: 34 });

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {

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

  const result = await runQueueless(workflow, { inputString: 'XX' });

  expect(result.output).toBe('output(child(input(XX)))');
});

test('zod input schema', async () => {
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = Workflow
    .create({ name: 'sum-with-zod', version: 1, inputSchema: schema })
    .step(async (input, { complete }) => {
      const { a, b } = input;
      return complete({ sum: a + b });
    });

  const result = await runQueueless(workflow, { a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('complete finishes the workflow early in runQueueless', async () => {
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = Workflow
    .create({ name: 'complete-queueless', version: 1, inputSchema: schema })
    .step(async ({ a, b }, { complete }) => {
      if (a + b === 5) {
        return complete({ sum: 5 });
      }
      return { sum: a + b };
    })
    .step(async () => {
      return { sum: 9999 };
    });

  const result = await runQueueless(workflow, { a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('restart restarts from the beginning in runQueueless', async () => {
  const schema = z.object({ iterations: z.number(), value: z.number() });

  const workflow = Workflow
    .create({ name: 'restart-queueless', version: 1, inputSchema: schema })
    .step(async (input, { restart }) => {
      if (input.iterations > 0) {
        return restart({ iterations: input.iterations - 1, value: input.value + 1 });
      }
      return { after: input.value };
    })
    .step(async ({ after }) => {
      return { out: after };
    });

  const result = await runQueueless(workflow, { iterations: 3, value: 10 });
  expect(result.out).toBe(13);
});
