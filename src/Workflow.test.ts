import { expect, test } from 'vitest'
import { z } from 'zod';
import { Workflow, runQueueless } from '~/Workflow';

test('run step', async () => {

  const workflow = Workflow
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const result = await runQueueless(workflow, { a: 12, b: 34 });

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {

  const child = Workflow
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }: { childInput: string }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = Workflow
    .create({ name: 'parent-workflow', version: 1 })
    .step(async ({ inputString }: { inputString: string }) => {
      return { childInput: `input(${inputString})` };
    })
    .step(child)
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

test('.parallel() runs children with full accumulated input', async () => {
  const child3 = Workflow
    .create({ name: 'child3', version: 1 })
    .step(async ({ s, s2 }: { s: string, s2: string }) => `child3(${s}, ${s2})`);

  const child4 = Workflow
    .create({ name: 'child4', version: 1 })
    .step(async ({ s, s2 }: { s: string, s2: string }) => `child4(${s}, ${s2})`);

  const parent = Workflow
    .create({ name: 'parent-steps', version: 1 })
    .step(async ({ s }: { s: string }) => ({ s2: `step1(${s})` }))
    .parallel({ s3: child3, s4: child4 })
    .step(async ({ s, s2, s3, s4 }) => ({
      out: `final(${s}, ${s2}, ${s3}, ${s4})`
    }));

  const result = await runQueueless(parent, { s: 'input' });
  expect(result.out).toBe('final(input, step1(input), child3(input, step1(input)), child4(input, step1(input)))');
});

test('.step(workflow) as first step', async () => {
  const child = Workflow
    .create({ name: 'first-child', version: 1 })
    .step(async ({ x }: { x: number }) => ({ y: x * 2 }));

  const parent = Workflow
    .create({ name: 'parent-first-child', version: 1 })
    .step(child)
    .step(async ({ x, y }) => ({
      result: x + y
    }));

  const result = await runQueueless(parent, { x: 5 });
  expect(result.result).toBe(15); // 5 + 10
});
