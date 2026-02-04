import { v4 as uuidv4 } from 'uuid';
import type { WfArray, NamesOfWfs, MatchingWorkflow } from './typeHelpers';
import type { Workflow, WfDispatcher } from './types';
import { WfBuilder, findWorkflow, isRestartWrapper, isCompleteWrapper, withRestartWrapper, withCompleteWrapper, collectWorkflows, isStepsChildren } from './WfBuilder';
import type { StepFn } from './types';
import { makeWorkflowJobData, WfJobData, WfUpdateInfo, WfStatus, WfMeta } from './types';
import { defaultLogger } from './utils';
import type { Logger } from './utils';
import { JobResult } from './jobQueue/JobQueueEngine';
import type { Storage, LockContext, StatusListener } from './storage/Storage';
import { withLock, LockAcquisitionError } from './storage/Storage';
import { MemoryStorage } from './storage/MemoryStorage';

export { LockAcquisitionError } from './storage/Storage';

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

/**
 * Thrown when a job exceeds the configured jobTimeoutMs.
 */
export class JobTimeoutError extends Error {
  public readonly jobId: string;
  public readonly timeoutMs: number;

  constructor(jobId: string, timeoutMs: number) {
    super(`Job "${jobId}" timed out after ${timeoutMs}ms`);
    this.name = 'JobTimeoutError';
    this.jobId = jobId;
    this.timeoutMs = timeoutMs;
  }
}

const JOB_TIMEOUT_SYMBOL = Symbol('JOB_TIMEOUT');

function createJobTimeout(timeoutMs: number): { promise: Promise<typeof JOB_TIMEOUT_SYMBOL>; clear: () => void } {
  let timeoutId: ReturnType<typeof setTimeout>;
  const promise = new Promise<typeof JOB_TIMEOUT_SYMBOL>((resolve) => {
    timeoutId = setTimeout(() => resolve(JOB_TIMEOUT_SYMBOL), timeoutMs);
  });
  return {
    promise,
    clear: () => clearTimeout(timeoutId),
  };
}

async function withJobTimeout<T>(
  jobId: string,
  timeoutMs: number,
  fn: (lockCtx?: LockContext) => Promise<T>,
  lockCtx: LockContext | undefined,
  logger: Logger,
): Promise<T> {
  const timeout = createJobTimeout(timeoutMs);
  try {
    const result = await Promise.race([
      fn(lockCtx),
      timeout.promise,
    ]);
    if (result === JOB_TIMEOUT_SYMBOL) {
      logger.error({ jobId, timeoutMs }, 'job timeout');
      throw new JobTimeoutError(jobId, timeoutMs);
    }
    return result as T;
  } finally {
    timeout.clear();
  }
}

type RunOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
  /**
   * Skip acquiring a distributed lock for this job.
   * Use when the caller already guarantees exclusive access (e.g., job queue engines
   * that handle job locking and timeout/rescheduling internally).
   */
  noLock?: boolean,
  /**
   * If true, throw LockAcquisitionError immediately when the lock cannot be acquired.
   * If false (default), wait for the lock to be released, check if a result exists,
   * and return it or retry running the job.
   */
  nonBlocking?: boolean,
  /**
   * If true, automatically refresh the lock in a background loop.
   * Defaults to false.
   */
  autoRefreshLock?: boolean,
};

export class WfRunner<
  const Wfs extends WfArray<string>,
  Meta extends WfMeta = WfMeta,
  Info extends WfUpdateInfo = WfUpdateInfo,
> {
  private readonly storage: Storage<WfStatus<Meta, Info>>;
  private readonly allWorkflows: Workflow<unknown, unknown>[];
  private readonly logger: Logger;  
  private readonly jobTimeoutMs: number;
  private readonly lockTimeoutMs: number;
  private readonly lockRefreshIntervalMs: number;
  private readonly resultTtlMs: number;
  private readonly dispatcher?: WfDispatcher<Wfs>;

  constructor(
    opts: {
      workflows: Wfs,
      /**
       * Maximum time a job is expected to run.
       * Used to derive lockTimeoutMs (default: jobTimeoutMs / 3).
       */
      jobTimeoutMs: number,
      /**
       * How long a lock is held before it expires.
       * Defaults to jobTimeoutMs / 3.
       */
      lockTimeoutMs?: number,
      /**
       * How often to refresh the lock while a job is running.
       * Should be significantly less than lockTimeoutMs to ensure the lock
       * doesn't expire during execution. Defaults to lockTimeoutMs / 3.
       * Minimum value is 100ms.
       */
      lockRefreshIntervalMs?: number,
      /**
       * How long to keep job results in storage.
       * Defaults to 24 hours.
       */
      resultTtlMs?: number,
      logger?: Logger,
      storage?: Storage<WfStatus<Meta, Info>>,
      dispatcher?: WfDispatcher<Wfs>,
    }
  ) {
    this.storage = opts.storage ?? new MemoryStorage() as Storage<WfStatus<Meta, Info>>;
    this.logger = opts.logger ?? defaultLogger;
    this.allWorkflows = collectWorkflows(opts.workflows as unknown as Workflow<unknown, unknown>[]);
    this.jobTimeoutMs = opts.jobTimeoutMs;
    this.lockTimeoutMs = opts.lockTimeoutMs ?? Math.floor(opts.jobTimeoutMs / 3);
    this.lockRefreshIntervalMs = Math.max(100, opts.lockRefreshIntervalMs ?? Math.floor(this.lockTimeoutMs / 3));
    this.resultTtlMs = opts.resultTtlMs ?? 24 * 60 * 60 * 1000; // 24 hours default
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
    meta?: Meta,
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

  private async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    jobId: string,
    jobData: WfJobData<Input>,
    opts?: { meta?: Meta, lockCtx?: LockContext },
  ): Promise<Output> {
    let currentStep = jobData.currentStep;
    let result: unknown = jobData.input;

    // Only parse input schema at step 0
    if (currentStep === 0 && workflow.inputSchema) {
      result = workflow.inputSchema.parse(result);
    }

    const refreshLock = opts?.lockCtx ? { token: opts.lockCtx.token, timeoutMs: opts.lockCtx.timeoutMs } : undefined;

    const runOptions = {
      update: async (updateOpts: { input?: unknown, info?: Info }) => {
        const status: WfStatus<Meta, Info> | undefined = (
          updateOpts.info
            ? { type: 'active', jobId, meta: opts?.meta, info: updateOpts.info }
            : undefined
        );
        const hasInput = 'input' in updateOpts;
        await this.storage.updateState(jobId, {
          state: hasInput ? { ...jobData, input: updateOpts.input, currentStep } : undefined,
          status,
          ttlMs: this.jobTimeoutMs,
          refreshLock,
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
              ttlMs: this.jobTimeoutMs,
              refreshLock,
            });
            continue;
          }
          if (isCompleteWrapper(out)) {
            let output = (out as any).output as Output;
            if (workflow.outputSchema) {
              output = workflow.outputSchema.parse(output) as Output;
            }
            const status: WfStatus<Meta, Info> = { type: 'success', jobId, meta: opts?.meta, resultKey: jobId };
            await this.storage.setResult(jobId, { type: 'success', output } as JobResult<Output>, { ttlMs: this.resultTtlMs, status });
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
            const fnOut = await fn(result, runOptions as any);
            if (isRestartWrapper(fnOut)) {
              this.logger.error({ jobId, key }, 'restart() not supported inside parallel()');
              throw new Error('restart() not supported inside parallel()');
            }
            if (isCompleteWrapper(fnOut)) {
              this.logger.error({ jobId, key }, 'complete() not supported inside parallel()');
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
            ttlMs: this.jobTimeoutMs,
            refreshLock,
          });
        }
      }

      this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'finished workflow');
      let output = result as Output;
      if (workflow.outputSchema) {
        output = workflow.outputSchema.parse(output) as Output;
      }
      const status: WfStatus<Meta, Info> = { type: 'success', jobId, meta: opts?.meta, resultKey: jobId };
      await this.storage.setResult(jobId, { type: 'success', output } as JobResult<Output>, { ttlMs: this.resultTtlMs, status });
      return output;
    } catch (err) {
      // SuspendExecutionException aborts without marking job as failed
      if (err instanceof SuspendExecutionException) {
        this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'workflow suspended');
        throw err;
      }
      const reason = `exception when running workflow: ${String(err)}`;
      this.logger.error({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId, reason, err }, 'workflow error');
      const status: WfStatus<Meta, Info> = { type: 'error', jobId, meta: opts?.meta, reason };
      await this.storage.setResult(jobId, { type: 'error', reason } as JobResult<Output>, { ttlMs: this.resultTtlMs, status });
      throw err;
    }
  }

  private async runJob<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    opts?: RunOptions<Meta>,
  ): Promise<Output> {
    const userProvidedJobId = opts?.jobId != undefined;
    const jobId = opts?.jobId ?? `${workflow.name}-${uuidv4()}`;

    const runJobInner = async (lockCtx?: LockContext): Promise<Output> => {
      // Check if there's an existing job to resume
      const existingState = await this.storage.getState<WfJobData<Input>>(jobId);
      
      if (existingState) {
        // Validate the existing job matches the workflow
        const jobData = existingState.state;
        if (jobData.name !== workflow.name) {
          this.logger.error({ jobId, existingName: jobData.name, requestedName: workflow.name }, 'job exists for different workflow');
          throw new Error(`Job ${jobId} exists for workflow "${jobData.name}" but trying to run "${workflow.name}"`);
        }
        if (jobData.version !== workflow.version) {
          this.logger.error({ jobId, existingVersion: jobData.version, requestedVersion: workflow.version }, 'job version mismatch');
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
        
        return await this.runSteps(workflow, jobId, jobData, { meta: opts?.meta, lockCtx });
      }

      // Create new job
      const jobData = makeWorkflowJobData<Input>({ props: workflow, input }) as WfJobData<Input>;
      await this.storage.updateState(jobId, { state: jobData, ttlMs: this.jobTimeoutMs });

      this.logger.info({ name: workflow.name, version: workflow.version, meta: opts?.meta, jobId }, 'starting workflow');

      return await this.runSteps(workflow, jobId, jobData, { meta: opts?.meta, lockCtx });
    };

    // Auto-generated UUIDs don't need locking (no conflict possible)
    const noLock = !userProvidedJobId || opts?.noLock;

    if (noLock) {
      return withJobTimeout(jobId, this.jobTimeoutMs, runJobInner, undefined, this.logger);
    }

    const lockResult = await withLock({
      storage: this.storage,
      jobId,
      lockTimeoutMs: this.lockTimeoutMs,
      lockRefreshIntervalMs: this.lockRefreshIntervalMs,
      logger: this.logger,
      autoRefreshLock: opts?.autoRefreshLock,
    }, (lockCtx) => withJobTimeout(jobId, this.jobTimeoutMs, runJobInner, lockCtx, this.logger));

    if (lockResult.type === 'lock-not-acquired') {
      if (opts?.nonBlocking) {
        this.logger.error({ jobId }, 'lock not acquired (nonBlocking)');
        throw new LockAcquisitionError(jobId);
      }
      return this.waitAndRetry<Input, Output>(workflow, input, jobId, opts);
    }

    return lockResult.result;
  }

  /**
   * Wait for the lock to be released, check for existing result, and retry if needed.
   */
  private async waitAndRetry<Input, Output>(
    workflow: Workflow<Input, Output>,
    input: Input,
    jobId: string,
    opts?: RunOptions<Meta>,
  ): Promise<Output> {
    // Wait for the lock to be released
    const lockReleased = await this.storage.waitForLock(jobId, this.jobTimeoutMs);
    if (!lockReleased) {
      this.logger.error({ jobId }, 'lock wait timeout');
      throw new LockAcquisitionError(jobId);
    }

    // Check if we have a result from the other execution
    const existingResult = await this.storage.getResult<JobResult<Output>>(jobId);
    if (existingResult) {
      if (existingResult.type === 'success') {
        return existingResult.output as Output;
      }
      // Re-throw the error from the previous execution
      this.logger.error({ jobId, reason: existingResult.reason }, 'previous execution failed');
      throw new Error(existingResult.reason);
    }

    // No result yet, retry running the job
    return this.runJob(workflow, input, opts);
  }

  async run<const N extends string, Input, Output, No, Co>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, NamesOfWfs<Wfs>, Input, Output, No, Co>,
    input: Input,
    opts?: RunOptions<Meta>,
  ) {
    const workflow = findWorkflow(this.allWorkflows, props) as Workflow<Input, Output>;
    return this.runJob(workflow, input, opts);
  }

  /**
   * Resume a workflow from provided job data.
   * @param jobId - The job ID to resume
   * @param jobData - The job data containing workflow state
   * @param opts.meta - Optional metadata to pass to the workflow
   * @param opts.noLock - Skip acquiring a distributed lock. Use when the caller already
   *   guarantees exclusive access (e.g., job queue engines handle locking internally).
   * @throws Error if workflow not registered
   * @throws LockAcquisitionError if lock cannot be acquired (when noLock is false)
   */
   async resume<Output>(
    jobId: string,
    jobData: WfJobData<unknown>,
    opts?: { meta?: Meta, noLock?: boolean, autoRefreshLock?: boolean },
  ): Promise<Output> {
    const runResumeInner = async (lockCtx?: LockContext): Promise<Output> => {
      const workflow = this.allWorkflows.find(
        w => w.name === jobData.name && w.version === jobData.version
      );

      if (!workflow) {
        this.logger.error({ jobId, name: jobData.name, version: jobData.version }, 'no registered workflow found');
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

      return await this.runSteps(workflow, jobId, jobData, { meta: opts?.meta, lockCtx }) as Promise<Output>;
    };

    if (opts?.noLock) {
      return withJobTimeout(jobId, this.jobTimeoutMs, runResumeInner, undefined, this.logger);
    }

    const lockResult = await withLock({
      storage: this.storage,
      jobId,
      lockTimeoutMs: this.lockTimeoutMs,
      lockRefreshIntervalMs: this.lockRefreshIntervalMs,
      logger: this.logger,
      autoRefreshLock: opts?.autoRefreshLock,
    }, (lockCtx) => withJobTimeout(jobId, this.jobTimeoutMs, runResumeInner, lockCtx, this.logger));

    if (lockResult.type === 'lock-not-acquired') {
      // resume always throws if lock can't be acquired
      this.logger.error({ jobId }, 'lock not acquired on resume');
      throw new LockAcquisitionError(jobId);
    }

    return lockResult.result;
  }

  /**
   * Subscribe to status updates for a job.
   * @param jobId - The job ID to subscribe to
   * @param listener - Callback invoked when status is published
   * @returns Unsubscribe function
   */
  subscribe(jobId: string, listener: StatusListener<WfStatus<Meta, Info>>): () => void {
    return this.storage.subscribe(jobId, listener);
  }
}
