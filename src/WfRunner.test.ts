import { expect, test } from 'vitest'
import { z } from 'zod';
import { WfBuilder } from '~/WfBuilder';
import { WfRunner } from '~/index';

test('run step', async () => {
  const workflow = WfBuilder
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const runner = new WfRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  const result = await runner.run(workflow, { a: 12, b: 34 });
  expect(result.c).toBe(46);
});

test('input schema validation', async () => {
  const inputSchema = z.object({ a: z.number(), b: z.number() });
  const workflow = WfBuilder
    .create({ name: 'input-schema-runner', version: 1, inputSchema })
    .step(async ({ a, b }) => ({ sum: a + b }));

  const runner = new WfRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  
  const result = await runner.run(workflow, { a: 2, b: 3 });
  expect(result.sum).toBe(5);

  await expect(runner.run(workflow, { a: 'bad', b: 3 } as any)).rejects.toThrow();
});

test('output schema validation', async () => {
  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const validWorkflow = WfBuilder
    .create({ name: 'output-valid-runner', version: 1, inputSchema, outputSchema })
    .step(async ({ a }) => ({ result: a * 2 }));

  const invalidWorkflow = WfBuilder
    .create({ name: 'output-invalid-runner', version: 1, inputSchema, outputSchema })
    .step(async ({ a }) => ({ result: 'not-a-number' as any }));

  const validRunner = new WfRunner({ workflows: [validWorkflow], lockTimeoutMs: 1000 });
  const invalidRunner = new WfRunner({ workflows: [invalidWorkflow], lockTimeoutMs: 1000 });

  const result = await validRunner.run(validWorkflow, { a: 5 });
  expect(result.result).toBe(10);

  await expect(invalidRunner.run(invalidWorkflow, { a: 5 })).rejects.toThrow();
});

test('output schema validation with complete()', async () => {
  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const validWorkflow = WfBuilder
    .create({ name: 'complete-valid-runner', version: 1, inputSchema, outputSchema })
    .step(async ({ a }, { complete }) => complete({ result: a * 2 }))
    .step(async () => ({ result: 9999 }));

  const invalidWorkflow = WfBuilder
    .create({ name: 'complete-invalid-runner', version: 1, inputSchema, outputSchema })
    .step(async ({ a }, { complete }) => complete({ result: 'bad' as any }))
    .step(async () => ({ result: 9999 }));

  const validRunner = new WfRunner({ workflows: [validWorkflow], lockTimeoutMs: 1000 });
  const invalidRunner = new WfRunner({ workflows: [invalidWorkflow], lockTimeoutMs: 1000 });

  const result = await validRunner.run(validWorkflow, { a: 5 });
  expect(result.result).toBe(10);

  await expect(invalidRunner.run(invalidWorkflow, { a: 5 })).rejects.toThrow();
});

test('complete finishes the workflow early', async () => {
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = WfBuilder
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

  const runner = new WfRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  const result = await runner.run(workflow, { a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('restart restarts from the beginning', async () => {
  const schema = z.object({ iterations: z.number(), value: z.number() });

  const workflow = WfBuilder
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

  const runner = new WfRunner({ workflows: [workflow], lockTimeoutMs: 1000 });
  const result = await runner.run(workflow, { iterations: 3, value: 10 });
  expect(result.out).toBe(13);
});
