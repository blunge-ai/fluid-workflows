import { expect, test } from 'vitest'
import { z } from 'zod';
import { WfBuilder } from '~/WfBuilder';

test('run step', async () => {

  const workflow = WfBuilder
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const result = await workflow.run({ a: 12, b: 34 });

  expect(result.c).toBe(46);
});

test('run child workflow', async () => {

  const child = WfBuilder
    .create({ name: 'child-workflow', version: 1 })
    .step(async ({ childInput }: { childInput: string }) => {
      return { childOutput: `child(${childInput})` };
    });

  const workflow = WfBuilder
    .create({ name: 'parent-workflow', version: 1 })
    .step(async ({ inputString }: { inputString: string }) => {
      return { childInput: `input(${inputString})` };
    })
    .step(child)
    .step(async ({ childOutput }) => {
      return { output: `output(${childOutput})` };
    });

  const result = await workflow.run({ inputString: 'XX' });

  expect(result.output).toBe('output(child(input(XX)))');
});

test('zod input schema', async () => {
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = WfBuilder
    .create({ name: 'sum-with-zod', version: 1, inputSchema: schema })
    .step(async (input, { complete }) => {
      const { a, b } = input;
      return complete({ sum: a + b });
    });

  const result = await workflow.run({ a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('complete finishes the workflow early', async () => {
  const schema = z.object({ a: z.number(), b: z.number() });

  const workflow = WfBuilder
    .create({ name: 'complete-test', version: 1, inputSchema: schema })
    .step(async ({ a, b }, { complete }) => {
      if (a + b === 5) {
        return complete({ sum: 5 });
      }
      return { sum: a + b };
    })
    .step(async () => {
      return { sum: 9999 };
    });

  const result = await workflow.run({ a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('restart restarts from the beginning', async () => {
  const schema = z.object({ iterations: z.number(), value: z.number() });

  const workflow = WfBuilder
    .create({ name: 'restart-test', version: 1, inputSchema: schema })
    .step(async (input, { restart }) => {
      if (input.iterations > 0) {
        return restart({ iterations: input.iterations - 1, value: input.value + 1 });
      }
      return { after: input.value };
    })
    .step(async ({ after }) => {
      return { out: after };
    });

  const result = await workflow.run({ iterations: 3, value: 10 });
  expect(result.out).toBe(13);
});

test('.parallel() runs children with full accumulated input', async () => {
  const child3 = WfBuilder
    .create({ name: 'child3', version: 1 })
    .step(async ({ s, s2 }: { s: string, s2: string }) => `child3(${s}, ${s2})`);

  const child4 = WfBuilder
    .create({ name: 'child4', version: 1 })
    .step(async ({ s, s2 }: { s: string, s2: string }) => `child4(${s}, ${s2})`);

  const parent = WfBuilder
    .create({ name: 'parent-steps', version: 1 })
    .step(async ({ s }: { s: string }) => ({ s2: `step1(${s})` }))
    .parallel({ s3: child3, s4: child4 })
    .step(async ({ s, s2, s3, s4 }) => ({
      out: `final(${s}, ${s2}, ${s3}, ${s4})`
    }));

  const result = await parent.run({ s: 'input' });
  expect(result.out).toBe('final(input, step1(input), child3(input, step1(input)), child4(input, step1(input)))');
});

test('.step(workflow) as first step', async () => {
  const child = WfBuilder
    .create({ name: 'first-child', version: 1 })
    .step(async ({ x }: { x: number }) => ({ y: x * 2 }));

  const parent = WfBuilder
    .create({ name: 'parent-first-child', version: 1 })
    .step(child)
    .step(async ({ x, y }) => ({
      result: x + y
    }));

  const result = await parent.run({ x: 5 });
  expect(result.result).toBe(15); // 5 + 10
});

test('.parallel() with functions', async () => {
  const parent = WfBuilder
    .create({ name: 'parent-parallel-fns', version: 1 })
    .step(async ({ x }: { x: number }) => ({ y: x * 2 }))
    .parallel({
      a: async ({ x, y }) => x + y,
      b: async ({ x, y }) => x * y,
    })
    .step(async ({ a, b }) => ({
      result: a + b
    }));

  const result = await parent.run({ x: 3 });
  // y = 6, a = 3 + 6 = 9, b = 3 * 6 = 18, result = 27
  expect(result.result).toBe(27);
});

test('.parallel() with mixed workflows and functions', async () => {
  const child = WfBuilder
    .create({ name: 'parallel-child', version: 1 })
    .step(async ({ x, y }: { x: number, y: number }) => ({ z: x + y }));

  const parent = WfBuilder
    .create({ name: 'parent-parallel-mixed', version: 1 })
    .step(async ({ x }: { x: number }) => ({ y: x * 2 }))
    .parallel({
      fromWf: child,
      fromFn: async ({ x, y }) => x * y,
    })
    .step(async ({ fromWf, fromFn }) => ({
      result: fromWf.z + fromFn
    }));

  const result = await parent.run({ x: 3 });
  // y = 6, fromWf.z = 3 + 6 = 9, fromFn = 3 * 6 = 18, result = 27
  expect(result.result).toBe(27);
});

test('zod output schema', async () => {
  const inputSchema = z.object({ a: z.number(), b: z.number() });
  const outputSchema = z.object({ sum: z.number() });

  const workflow = WfBuilder
    .create({ name: 'sum-with-output-schema', version: 1, inputSchema, outputSchema })
    .step(async ({ a, b }) => {
      return { sum: a + b };
    });

  expect(workflow.inputSchema).toBe(inputSchema);
  expect(workflow.outputSchema).toBe(outputSchema);

  const result = await workflow.run({ a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('output schema is preserved through step and parallel', async () => {
  const inputSchema = z.object({ x: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const workflow = WfBuilder
    .create({ name: 'schema-preserved', version: 1, inputSchema, outputSchema })
    .step(async ({ x }) => ({ y: x * 2 }))
    .parallel({
      a: async ({ x, y }) => x + y,
      b: async ({ x, y }) => x * y,
    })
    .step(async ({ a, b }) => ({ result: a + b }));

  expect(workflow.inputSchema).toBe(inputSchema);
  expect(workflow.outputSchema).toBe(outputSchema);

  const result = await workflow.run({ x: 3 });
  expect(result.result).toBe(27);
});

test('input schema validation rejects invalid input', async () => {
  const inputSchema = z.object({ a: z.number(), b: z.number() });

  const workflow = WfBuilder
    .create({ name: 'input-validation-fail', version: 1, inputSchema })
    .step(async ({ a, b }) => ({ sum: a + b }));

  await expect(workflow.run({ a: 'not-a-number', b: 3 } as any)).rejects.toThrow();
});

test('output schema validation rejects invalid output', async () => {
  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const workflow = WfBuilder
    .create({ name: 'output-validation-fail', version: 1, inputSchema, outputSchema })
    .step(async ({ a }) => ({ result: 'not-a-number' as any }));

  await expect(workflow.run({ a: 5 })).rejects.toThrow();
});

test('output schema validation passes with valid output', async () => {
  const inputSchema = z.object({ a: z.number(), b: z.number() });
  const outputSchema = z.object({ sum: z.number() });

  const workflow = WfBuilder
    .create({ name: 'output-validation-pass', version: 1, inputSchema, outputSchema })
    .step(async ({ a, b }) => ({ sum: a + b }));

  const result = await workflow.run({ a: 2, b: 3 });
  expect(result.sum).toBe(5);
});

test('output schema validation with complete()', async () => {
  const inputSchema = z.object({ a: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const workflowValid = WfBuilder
    .create({ name: 'complete-output-valid', version: 1, inputSchema, outputSchema })
    .step(async ({ a }, { complete }) => complete({ result: a * 2 }))
    .step(async () => ({ result: 9999 }));

  const result = await workflowValid.run({ a: 5 });
  expect(result.result).toBe(10);

  const workflowInvalid = WfBuilder
    .create({ name: 'complete-output-invalid', version: 1, inputSchema, outputSchema })
    .step(async ({ a }, { complete }) => complete({ result: 'bad' as any }))
    .step(async () => ({ result: 9999 }));

  await expect(workflowInvalid.run({ a: 5 })).rejects.toThrow();
});

test('schema preserved through .step(workflow)', async () => {
  const inputSchema = z.object({ x: z.number() });
  const outputSchema = z.object({ result: z.number() });

  const child = WfBuilder
    .create({ name: 'schema-child', version: 1 })
    .step(async ({ x, y }: { x: number, y: number }) => ({ z: x + y }));

  const parent = WfBuilder
    .create({ name: 'schema-parent-child', version: 1, inputSchema, outputSchema })
    .step(async ({ x }) => ({ y: x * 2 }))
    .step(child)
    .step(async ({ z }) => ({ result: z }));

  expect(parent.inputSchema).toBe(inputSchema);
  expect(parent.outputSchema).toBe(outputSchema);

  const result = await parent.run({ x: 3 });
  expect(result.result).toBe(9); // 3 + 6 = 9
});

test('child workflow with its own schemas', async () => {
  const childInputSchema = z.object({ s: z.string() });
  const childOutputSchema = z.object({ childOut: z.string() });

  const child = WfBuilder
    .create({ name: 'child-with-schema', version: 1, inputSchema: childInputSchema, outputSchema: childOutputSchema })
    .step(async ({ s }) => ({ childOut: `child(${s})` }));

  const parent = WfBuilder
    .create({ name: 'parent-child-schema', version: 1 })
    .step(async ({ input }: { input: string }) => ({ s: input }))
    .step(child)
    .step(async ({ childOut }) => ({ result: childOut }));

  const result = await parent.run({ input: 'test' });
  expect(result.result).toBe('child(test)');
});

test('child workflow schema validation rejects invalid input', async () => {
  const childInputSchema = z.object({ s: z.string() });

  const child = WfBuilder
    .create({ name: 'child-input-fail', version: 1, inputSchema: childInputSchema })
    .step(async ({ s }) => ({ childOut: s }));

  const parent = WfBuilder
    .create({ name: 'parent-child-input-fail', version: 1 })
    .step(async ({ n }: { n: number }) => ({ s: n as any })) // passes number instead of string
    .step(child);

  await expect(parent.run({ n: 123 })).rejects.toThrow();
});

test('restart re-validates input against schema', async () => {
  const inputSchema = z.object({ count: z.number().min(0), value: z.number() });
  let restartAttempts = 0;

  const workflow = WfBuilder
    .create({ name: 'restart-revalidate', version: 1, inputSchema })
    .step(async (input, { restart }) => {
      restartAttempts++;
      if (input.count > 0) {
        return restart({ count: input.count - 1, value: input.value + 1 });
      }
      return { result: input.value };
    });

  const result = await workflow.run({ count: 2, value: 10 });
  expect(result.result).toBe(12);
  expect(restartAttempts).toBe(3);
});

test('restart with invalid input fails schema validation', async () => {
  const inputSchema = z.object({ count: z.number().min(0), value: z.number() });

  const workflow = WfBuilder
    .create({ name: 'restart-invalid', version: 1, inputSchema })
    .step(async (input, { restart }) => {
      if (input.count > 0) {
        return restart({ count: -999, value: input.value }); // -999 violates min(0)
      }
      return { result: input.value };
    });

  await expect(workflow.run({ count: 1, value: 10 })).rejects.toThrow();
});
