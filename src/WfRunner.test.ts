import { expect, test, beforeEach } from 'vitest'
import { z } from 'zod';
import { WfBuilder } from '~/WfBuilder';
import { WfRunner, SuspendExecutionException } from '~/WfRunner';
import { defaultStorage, resetDefaultStorage } from '~/storage/defaultStorage';
import type { WfJobData } from '~/types';

beforeEach(() => {
  resetDefaultStorage();
});

test('run step', async () => {
  const workflow = WfBuilder
    .create({ name: 'add-a-and-b', version: 1 })
    .step(async ({ a, b }: { a: number, b: number }) => {
      return { c: a + b };
    });

  const runner = new WfRunner({ workflows: [workflow], jobTimeoutMs: 1000 });
  const result = await runner.run(workflow, { a: 12, b: 34 });
  expect(result.c).toBe(46);
});

test('input schema validation', async () => {
  const inputSchema = z.object({ a: z.number(), b: z.number() });
  const workflow = WfBuilder
    .create({ name: 'input-schema-runner', version: 1, inputSchema })
    .step(async ({ a, b }) => ({ sum: a + b }));

  const runner = new WfRunner({ workflows: [workflow], jobTimeoutMs: 1000 });
  
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

  const validRunner = new WfRunner({ workflows: [validWorkflow], jobTimeoutMs: 1000 });
  const invalidRunner = new WfRunner({ workflows: [invalidWorkflow], jobTimeoutMs: 1000 });

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

  const validRunner = new WfRunner({ workflows: [validWorkflow], jobTimeoutMs: 1000 });
  const invalidRunner = new WfRunner({ workflows: [invalidWorkflow], jobTimeoutMs: 1000 });

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

  const runner = new WfRunner({ workflows: [workflow], jobTimeoutMs: 1000 });
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

  const runner = new WfRunner({ workflows: [workflow], jobTimeoutMs: 1000 });
  const result = await runner.run(workflow, { iterations: 3, value: 10 });
  expect(result.out).toBe(13);
});

test('durable execution: resume after shutdown', async () => {
  let runCount = 0;
  
  const workflow = WfBuilder
    .create({ name: 'durable-test', version: 1 })
    .step(async ({ value }: { value: number }) => {
      return { step1Result: value * 2 };
    })
    .step(async ({ step1Result }) => {
      runCount++;
      // Throw shutdown exception only on the first run
      if (runCount === 1) {
        throw new SuspendExecutionException('Simulated shutdown');
      }
      return { step2Result: step1Result + 10 };
    })
    .step(async ({ step2Result }) => {
      return { finalResult: step2Result * 3 };
    });

  // Use shared storage so state persists between runs
  const storage = defaultStorage;
  const runner = new WfRunner({ workflows: [workflow], jobTimeoutMs: 60000, storage });
  const jobId = 'test-durable-job-123';

  // First run: should abort at step 2 with SuspendExecutionException
  await expect(runner.run(workflow, { value: 5 }, { jobId }))
    .rejects.toThrow(SuspendExecutionException);
  
  // Get active jobs from storage - should find our interrupted job
  const activeJobs = await storage.getActiveJobs();
  expect(activeJobs).toHaveLength(1);
  const activeJob = activeJobs[0]!;
  expect(activeJob.jobId).toBe(jobId);
  expect(activeJob.state).toMatchObject({
    name: 'durable-test',
    version: 1,
    currentStep: 1,  // At step 2 (0-indexed)
    input: { value: 5, step1Result: 10 },  // Step 1 result merged
  });

  // Resume the job using its jobId and jobData (discovered from getActiveJobs)
  const result = await runner.resume<{ finalResult: number }>(activeJob.jobId, activeJob.state as WfJobData<unknown>);
  
  // step1Result = 5 * 2 = 10
  // step2Result = 10 + 10 = 20
  // finalResult = 20 * 3 = 60
  expect(result.finalResult).toBe(60);
  
  // Verify step 2 was only run once on the second invocation
  expect(runCount).toBe(2);

  // Verify job is no longer in active jobs after completion
  const activeJobsAfter = await storage.getActiveJobs();
  expect(activeJobsAfter).toHaveLength(0);
});

test('durable execution: new job if no existing state', async () => {
  const workflow = WfBuilder
    .create({ name: 'new-job-test', version: 1 })
    .step(async ({ x }: { x: number }) => ({ result: x + 1 }));

  const storage = defaultStorage;
  const runner = new WfRunner({ workflows: [workflow], jobTimeoutMs: 60000, storage });

  // Run with a jobId that doesn't exist - should create new job
  const result = await runner.run(workflow, { x: 10 }, { jobId: 'new-job-id' });
  expect(result.result).toBe(11);
});

test('durable execution: validates workflow name on resume', async () => {
  const workflow1 = WfBuilder
    .create({ name: 'workflow-one', version: 1 })
    .step(async () => {
      throw new SuspendExecutionException();
    });

  const workflow2 = WfBuilder
    .create({ name: 'workflow-two', version: 1 })
    .step(async () => ({ done: true }));

  const storage = defaultStorage;
  const runner = new WfRunner({ workflows: [workflow1, workflow2], jobTimeoutMs: 60000, storage });
  const jobId = 'mismatched-job';

  // Start workflow1
  await expect(runner.run(workflow1, {}, { jobId })).rejects.toThrow(SuspendExecutionException);

  // Try to resume with workflow2 - should fail
  await expect(runner.run(workflow2, {}, { jobId }))
    .rejects.toThrow('Job mismatched-job exists for workflow "workflow-one" but trying to run "workflow-two"');
});

test('durable execution: validates workflow version on resume', async () => {
  const workflowV1 = WfBuilder
    .create({ name: 'versioned-workflow', version: 1 })
    .step(async () => {
      throw new SuspendExecutionException();
    });

  const workflowV2 = WfBuilder
    .create({ name: 'versioned-workflow', version: 2 })
    .step(async () => ({ done: true }));

  const storage = defaultStorage;
  
  // Start with v1
  const runner1 = new WfRunner({ workflows: [workflowV1], jobTimeoutMs: 60000, storage });
  const jobId = 'version-mismatch-job';
  await expect(runner1.run(workflowV1, {}, { jobId })).rejects.toThrow(SuspendExecutionException);

  // Try to resume with v2 - should fail
  const runner2 = new WfRunner({ workflows: [workflowV2], jobTimeoutMs: 60000, storage });
  await expect(runner2.run(workflowV2, {}, { jobId }))
    .rejects.toThrow('Job version-mismatch-job has version 1 but workflow has version 2');
});
