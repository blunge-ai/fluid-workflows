import { v4 as uuidv4 } from 'uuid';
import type { WfArray, NamesOfWfs, MatchingWorkflow } from './typeHelpers';
import type { Workflow } from './types';
import { WfBuilder, findWorkflow, isRestartWrapper, isCompleteWrapper, withRestartWrapper, withCompleteWrapper, collectWorkflows, isStepsChildren } from './WfBuilder';
import type { StepFn } from './types';
import { makeWorkflowJobData, WfJobData, WfProgressInfo } from './types';
import { defaultLogger } from './utils';
import type { Logger } from './utils';
import { JobResult } from './jobQueue/JobQueueEngine';
import type { Storage } from './storage/Storage';
import { MemoryStorage } from './storage/MemoryStorage';

/**
 * Special exception that causes the runner to abort without marking the job as failed.
 * Used for testing durable execution resume and for graceful shutdown scenarios.
 */
export class TestSystemShutdownException extends Error {
  constructor(message?: string) {
    super(message ?? 'System shutdown');
    this.name = 'TestSystemShutdownException';
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

  constructor(
    opts: {
      workflows: Wfs,
      lockTimeoutMs: number,
      logger?: Logger,
      storage?: Storage,
    }
  ) {
    this.storage = opts.storage ?? new MemoryStorage();
    this.logger = opts.logger ?? defaultLogger;
    this.allWorkflows = collectWorkflows(opts.workflows as unknown as Workflow<unknown, unknown>[]);
    this.lockTimeoutMs = opts.lockTimeoutMs;
  }

  private async runChild<ChildOutput>(
    childWorkflow: Workflow<unknown, ChildOutput>,
    input: unknown,
    parentJobId: string,
    stepIndex: number,
    childKey: string,
    meta?: unknown,
  ): Promise<ChildOutput> {
    const childJobId = `${parentJobId}:step:${stepIndex}:child:${childKey}`;
    const foundWorkflow = findWorkflow(this.allWorkflows, childWorkflow);
    
    // Run child workflow - will resume from storage if job exists
    return this.runJob(foundWorkflow as Workflow<unknown, ChildOutput>, input, { jobId: childJobId, meta });
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
      progress: async (progressInfo: WfProgressInfo) => {
        const status = { type: 'active', jobId, meta: opts?.meta as Meta, info: progressInfo } as const;
        await this.storage.updateState(jobId, { status, ttlMs: this.lockTimeoutMs });
        return { interrupt: false } as const;
      },
      update: async (stepInput: unknown, progressInfo?: WfProgressInfo) => {
        const status = (
          progressInfo
            ? { type: 'active', jobId, meta: opts?.meta as Meta, info: progressInfo } as const
            : undefined
        );
        await this.storage.updateState(jobId, {
          state: { ...jobData, input: stepInput, currentStep },
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

          // Run child workflows in parallel
          const workflowOutputsPromise = Promise.all(workflowEntries.map(async ([key, childWorkflow]) => {
            const childOut = await this.runChild(childWorkflow, result, jobId, currentStep, key, opts?.meta);
            return [key, childOut] as const;
          }));

          const [fnOutputs, workflowOutputs] = await Promise.all([fnOutputsPromise, workflowOutputsPromise]);
          const fnOutputRecord = Object.fromEntries(fnOutputs);
          const workflowOutputRecord = Object.fromEntries(workflowOutputs);

          result = { ...(result as Record<string, unknown>), ...fnOutputRecord, ...workflowOutputRecord };
        } else {
          // Handle .step(workflow) - single child workflow
          const childWorkflow = step as unknown as Workflow<unknown, unknown>;
          const childOut = await this.runChild(childWorkflow, result, jobId, currentStep, '', opts?.meta);

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
      // TestSystemShutdownException aborts without marking job as failed (for durable execution testing)
      if (err instanceof TestSystemShutdownException) {
        this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'workflow aborted for shutdown');
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
   * Resume a workflow from its persisted state.
   * Looks up the job by jobId, finds the matching workflow, and continues execution.
   * @throws Error if job not found or workflow not registered
   */
  async resume<Output, Meta = unknown>(
    jobId: string,
    opts?: { meta?: Meta },
  ): Promise<Output> {
    const existingState = await this.storage.getState<WfJobData<unknown>>(jobId);
    
    if (!existingState) {
      throw new Error(`No active job found with id: ${jobId}`);
    }

    const jobData = existingState.state;
    const workflow = this.allWorkflows.find(
      w => w.name === jobData.name && w.version === jobData.version
    );

    if (!workflow) {
      throw new Error(`No registered workflow found for "${jobData.name}" version ${jobData.version}`);
    }

    this.logger.info({ 
      name: workflow.name, 
      version: workflow.version, 
      meta: opts?.meta, 
      jobId,
      currentStep: jobData.currentStep,
      totalSteps: jobData.totalSteps,
    }, 'resuming workflow');

    return this.runSteps(workflow, jobId, jobData, { meta: opts?.meta }) as Promise<Output>;
  }
}
