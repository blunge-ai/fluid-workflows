import { v4 as uuidv4 } from 'uuid';
import type { WfArray, NamesOfWfs, MatchingWorkflow } from './typeHelpers';
import type { Workflow, WfDispatcher, DispatchOptions } from './types';
import { WfBuilder, findWorkflow, isRestartWrapper, isCompleteWrapper, withRestartWrapper, withCompleteWrapper, collectWorkflows, isStepsChildren } from './WfBuilder';
import type { StepFn } from './types';
import { makeWorkflowJobData, WfJobData, WfProgressInfo } from './types';
import { defaultLogger } from './utils';
import type { Logger } from './utils';
import { JobResult } from './jobQueue/JobQueueEngine';
import type { Storage } from './storage/Storage';
import { MemoryStorage } from './storage/MemoryStorage';

/**
 * Exception that causes the runner to abort without marking the job as failed.
 * Used for suspending workflow execution (e.g., waiting for child workflows,
 * graceful shutdown, or testing durable execution resume).
 */
export class SuspendExecutionException extends Error {
  constructor(message?: string) {
    super(message ?? 'Workflow suspended');
    this.name = 'SuspendExecutionException';
  }
}

type RunOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
};

export class WfRunner<
  const Wfs extends WfArray<string>,
> {
  private readonly storage: Storage;
  private readonly allWorkflows: Workflow<unknown, unknown>[];
  private readonly logger: Logger;  
  private readonly lockTimeoutMs: number;
  private readonly dispatcher?: WfDispatcher<Wfs>;

  constructor(
    opts: {
      workflows: Wfs,
      lockTimeoutMs: number,
      logger?: Logger,
      storage?: Storage,
      dispatcher?: WfDispatcher<Wfs>,
    }
  ) {
    this.storage = opts.storage ?? new MemoryStorage();
    this.logger = opts.logger ?? defaultLogger;
    this.allWorkflows = collectWorkflows(opts.workflows as unknown as Workflow<unknown, unknown>[]);
    this.lockTimeoutMs = opts.lockTimeoutMs;
    this.dispatcher = opts.dispatcher;
  }

  /**
   * Dispatch child workflows. Uses custom dispatcher if provided, otherwise runs inline.
   */
  private async dispatch(
    workflows: Record<string, Workflow<unknown, unknown>>,
    input: unknown,
    parentJobId: string,
    stepIndex: number,
    meta?: unknown,
  ): Promise<Record<string, unknown>> {
    const entries = Object.entries(workflows);
    
    if (this.dispatcher) {
      // Dispatch all in parallel via dispatcher - start all immediately
      const promises = entries.map(([key, workflow]) => {
        const foundWorkflow = findWorkflow(this.allWorkflows, workflow);
        const childJobId = `${parentJobId}:step:${stepIndex}:child:${key}`;
        const promise = this.dispatcher!.dispatchAwaitingOutput(
          foundWorkflow as any,
          input,
          { jobId: childJobId, meta },
        );
        return promise.then(
          (output) => ({ type: 'success' as const, key, output }),
          (error) => ({ type: 'error' as const, key, error }),
        );
      });
      
      const settled = await Promise.all(promises);
      const firstError = settled.find((r) => r.type === 'error');
      if (firstError && firstError.type === 'error') {
        throw firstError.error;
      }
      
      return Object.fromEntries(
        settled.map((r) => [r.key, r.type === 'success' ? r.output : undefined])
      );
    }
    
    // Default: run all inline in parallel
    const results = await Promise.all(entries.map(async ([key, workflow]) => {
      const foundWorkflow = findWorkflow(this.allWorkflows, workflow);
      const childJobId = `${parentJobId}:step:${stepIndex}:child:${key}`;
      const output = await this.runJob(foundWorkflow as Workflow<unknown, unknown>, input, { jobId: childJobId, meta });
      return [key, output] as const;
    }));
    return Object.fromEntries(results);
  }

  private async runSteps<Input, Output, Meta>(
    workflow: Workflow<Input, Output>,
    jobId: string,
    jobData: WfJobData<Input>,
    opts?: { meta?: Meta },
  ): Promise<Output> {
    let currentStep = jobData.currentStep;
    let result: unknown = jobData.input;

    // Only parse input schema at step 0
    if (currentStep === 0 && workflow.inputSchema) {
      result = workflow.inputSchema.parse(result);
    }

    const runOptions = {
      update: async (updateOpts: { input?: unknown, progress?: WfProgressInfo }) => {
        const status = (
          updateOpts.progress
            ? { type: 'active', jobId, meta: opts?.meta as Meta, info: updateOpts.progress } as const
            : undefined
        );
        const hasInput = 'input' in updateOpts;
        await this.storage.updateState(jobId, {
          state: hasInput ? { ...jobData, input: updateOpts.input, currentStep } : undefined,
          status,
          ttlMs: this.lockTimeoutMs,
        });
        return { interrupt: false } as const;
      },
      restart: withRestartWrapper,
      complete: withCompleteWrapper,
    };

    try {
      while (currentStep < workflow.stepFns.length) {
        const step = workflow.stepFns[currentStep];
        let out: unknown;

        if (typeof step === 'function' && !(step instanceof WfBuilder)) {
          // Handle step function
          out = await (step as any)(result, runOptions);
          if (isRestartWrapper(out)) {
            result = (out as any).input as Input;
            if (workflow.inputSchema) {
              result = workflow.inputSchema.parse(result);
            }
            currentStep = 0;
            await this.storage.updateState(jobId, {
              state: { ...jobData, input: result, currentStep },
              ttlMs: this.lockTimeoutMs,
            });
            continue;
          }
          if (isCompleteWrapper(out)) {
            let output = (out as any).output as Output;
            if (workflow.outputSchema) {
              output = workflow.outputSchema.parse(output) as Output;
            }
            const status = { type: 'success', jobId, meta: opts?.meta as Meta, resultKey: jobId } as const;
            await this.storage.setResult(jobId, { type: 'success', output } as JobResult<Output>, status);
            return output;
          }
          // Last step's output is the workflow output (no merge)
          if (currentStep === workflow.stepFns.length - 1) {
            result = out;
          } else {
            // Merge step output with accumulated state
            result = { ...(result as Record<string, unknown>), ...(out as Record<string, unknown>) };
          }
        } else if (isStepsChildren(step)) {
          // Handle .parallel() - run functions and child workflows in parallel
          const entries = Object.entries(step.children);
          const workflowEntries = entries.filter(([_, item]) => item instanceof WfBuilder) as [string, Workflow<unknown, unknown>][];
          const fnEntries = entries.filter(([_, item]) => typeof item === 'function' && !(item instanceof WfBuilder)) as [string, StepFn<unknown, unknown, Input, Output>][];

          // Run functions in parallel
          const fnOutputsPromise = Promise.all(fnEntries.map(async ([key, fn]) => {
            const fnOut = await fn(result, runOptions);
            if (isRestartWrapper(fnOut)) {
              throw new Error('restart() not supported inside parallel()');
            }
            if (isCompleteWrapper(fnOut)) {
              throw new Error('complete() not supported inside parallel()');
            }
            return [key, fnOut] as const;
          }));

          // Run child workflows via dispatch (handles both inline and custom dispatcher)
          const workflowsMap = Object.fromEntries(workflowEntries);
          const workflowOutputsPromise = Object.keys(workflowsMap).length > 0
            ? this.dispatch(workflowsMap, result, jobId, currentStep, opts?.meta)
            : Promise.resolve({});

          const [fnOutputs, workflowOutputs] = await Promise.all([fnOutputsPromise, workflowOutputsPromise]);
          const fnOutputRecord = Object.fromEntries(fnOutputs);

          result = { ...(result as Record<string, unknown>), ...fnOutputRecord, ...(workflowOutputs as Record<string, unknown>) };
        } else {
          // Handle .step(workflow) - single child workflow
          const childWorkflow = step as unknown as Workflow<unknown, unknown>;
          const childOut = (await this.dispatch({ '': childWorkflow }, result, jobId, currentStep, opts?.meta))[''];

          // Last step's output is the workflow output (no merge)
          if (currentStep === workflow.stepFns.length - 1) {
            result = childOut;
          } else {
            // Merge child output with accumulated state
            result = { ...(result as Record<string, unknown>), ...(childOut as Record<string, unknown>) };
          }
        }

        currentStep += 1;
        if (currentStep !== workflow.stepFns.length) {
          await this.storage.updateState(jobId, {
            state: { ...jobData, input: result, currentStep },
            ttlMs: this.lockTimeoutMs,
          });
        }
      }

      this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'finished workflow');
      let output = result as Output;
      if (workflow.outputSchema) {
        output = workflow.outputSchema.parse(output) as Output;
      }
      const status = { type: 'success', jobId, meta: opts?.meta as Meta, resultKey: jobId } as const;
      await this.storage.setResult(jobId, { type: 'success', output } as JobResult<Output>, status);
      return output;
    } catch (err) {
      // SuspendExecutionException aborts without marking job as failed
      if (err instanceof SuspendExecutionException) {
        this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'workflow suspended');
        throw err;
      }
      const reason = `exception when running workflow: ${String(err)}`;
      this.logger.error({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId, reason, err }, 'workflow error');
      const status = { type: 'error', jobId, meta: opts?.meta as Meta, reason } as const;
      await this.storage.setResult(jobId, { type: 'error', reason } as JobResult<Output>, status);
      throw err;
    }
  }

  private async runJob<Input, Output, Meta>(
    workflow: Workflow<Input, Output>,
    input: Input,
    opts?: RunOptions<Meta>,
  ): Promise<Output> {
    const jobId = opts?.jobId ?? `${workflow.name}-${uuidv4()}`;

    // Check if there's an existing job in progress
    const existingState = await this.storage.getState<WfJobData<Input>>(jobId);
    
    if (existingState) {
      // Validate the existing job matches the workflow
      const jobData = existingState.state;
      if (jobData.name !== workflow.name) {
        throw new Error(`Job ${jobId} exists for workflow "${jobData.name}" but trying to run "${workflow.name}"`);
      }
      if (jobData.version !== workflow.version) {
        throw new Error(`Job ${jobId} has version ${jobData.version} but workflow has version ${workflow.version}`);
      }
      
      this.logger.info({ 
        name: workflow.name, 
        version: workflow.version, 
        meta: opts?.meta, 
        jobId,
        currentStep: jobData.currentStep,
        totalSteps: jobData.totalSteps,
      }, 'resuming workflow');
      
      return this.runSteps(workflow, jobId, jobData, { meta: opts?.meta });
    }

    // Create new job
    const jobData = makeWorkflowJobData<Input>({ props: workflow, input }) as WfJobData<Input>;
    await this.storage.updateState(jobId, { state: jobData, ttlMs: this.lockTimeoutMs });

    this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'starting workflow');

    return this.runSteps(workflow, jobId, jobData, { meta: opts?.meta });
  }

  async run<const N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, NamesOfWfs<Wfs>, Input, Output, No, Co>,
    input: Input,
    opts?: RunOptions<Meta>,
  ) {
    const workflow = findWorkflow(this.allWorkflows, props) as Workflow<Input, Output>;
    return this.runJob(workflow, input, opts);
  }

  /**
   * Resume a workflow from its persisted state or provided job data.
   * If jobData is provided, uses it directly; otherwise looks up state from storage.
   * @throws Error if job not found (when no jobData provided) or workflow not registered
   */
  async resume<Output, Meta = unknown>(
    jobId: string,
    opts?: { meta?: Meta, jobData?: WfJobData<unknown> },
  ): Promise<Output> {
    let jobData: WfJobData<unknown>;
    
    if (opts?.jobData) {
      jobData = opts.jobData;
    } else {
      const existingState = await this.storage.getState<WfJobData<unknown>>(jobId);
      if (!existingState) {
        throw new Error(`No active job found with id: ${jobId}`);
      }
      jobData = existingState.state;
    }

    const workflow = this.allWorkflows.find(
      w => w.name === jobData.name && w.version === jobData.version
    );

    if (!workflow) {
      throw new Error(`No registered workflow found for "${jobData.name}" version ${jobData.version}`);
    }

    if (jobData.currentStep > 0) {
      this.logger.info({ 
        name: workflow.name, 
        version: workflow.version, 
        meta: opts?.meta, 
        jobId,
        currentStep: jobData.currentStep,
        totalSteps: jobData.totalSteps,
      }, 'resuming workflow');
    } else {
      this.logger.info({ 
        name: workflow.name, 
        version: workflow.version, 
        meta: opts?.meta, 
        jobId,
      }, 'starting workflow');
    }

    return this.runSteps(workflow, jobId, jobData, { meta: opts?.meta }) as Promise<Output>;
  }
}
