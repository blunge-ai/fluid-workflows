import type { ZodTypeAny } from 'zod';
import type { MatchingWorkflow, NamesOfWfs, WfArray } from './typeHelpers';

// ============================================================================
// Workflow Job Types
// ============================================================================

// The job data (or state) used by the WfJobQueueWorker to run a workflow

export type WfJobData<Input = unknown> = {
  name: string,
  version: number,
  totalSteps: number,
  currentStep: number,
  input: Input,
};

export type WfProgressInfo = {
  phase: string,
  progress: number,
};

export function makeWorkflowJobData<Input = unknown>({ props, input }: { props: WorkflowProps, input: Input }) {
  return {
    name: props.name,
    version: props.version,
    totalSteps: props.numSteps,
    currentStep: 0,
    input: input,
  } satisfies WfJobData<Input>;
}

// ============================================================================
// Workflow Types
// ============================================================================

export type WorkflowProps<Name extends string = string> = {
  name: Name,
  version: number,
  numSteps: number,
};

export type WorkflowRunOptions<WfInput, WfOutput, StepInput> = {
  update: UpdateFn<StepInput>,
  restart: RestartFn<WfInput>,
  complete: CompleteFn<WfOutput>,
};

export type UpdateFn<StepInput>
  = (opts: { input?: StepInput, progress?: WfProgressInfo }) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output, WfInput, WfOutput>
  = (input: Input, runOpts: WorkflowRunOptions<WfInput, WfOutput, Input>) => Promise<Output>;

export type WorkflowNames<W>
  = W extends Workflow<any, any, infer N, any, any> ? N : never;

export class RestartWrapper<WfInput> {
  public __type = 'RestartWrapper' as const;
  constructor(public input: WfInput) {}
}

export class CompleteWrapper<WfOutput> {
  public __type = 'CompleteWrapper' as const;
  constructor(public output: WfOutput) {}
}

// Wrapper class to distinguish .parallel() at runtime
export class StepsChildren<Cm extends Record<string, Workflow<any, any, string, any, any> | StepFn<any, any, any, any>>> {
  public __type = 'StepsChildren' as const;
  constructor(public children: Cm) {}
}

export type RestartFn<WfInput> = (input: WfInput) => RestartWrapper<WfInput>;
export type CompleteFn<WfOutput> = <T extends WfOutput>(output: T) => CompleteWrapper<T>;

export type StripCtrl<StepOutput>
  = Exclude<StepOutput, RestartWrapper<any> | CompleteWrapper<any>>

// For .parallel() - items can be workflows or functions
export type ParallelItem<Input, WfInput, WfOutput>
  = Workflow<Input, any, string, any, any>
  | StepFn<Input, any, WfInput, WfOutput>;

export type ParallelMap<Input, WfInput, WfOutput>
  = { [K in string]: ParallelItem<Input, WfInput, WfOutput> };

/**
 * Workflow interface - represents a runnable workflow
 */
export interface Workflow<Input = unknown, Output = unknown, Names extends string = string, NextOutput = unknown, CtrlOutput = unknown> extends WorkflowProps<Names> {
  inputSchema?: ZodTypeAny;
  outputSchema?: ZodTypeAny;
  stepFns: Array<
    StepFn<unknown, unknown, unknown, unknown> |
    Workflow<unknown, unknown, string, any, any> |
    Record<string, Workflow<unknown, unknown, string, any, any>> |
    StepsChildren<any>
  >;
  run(input: Input, opts?: RunOptions<unknown>): Promise<Output>;
}

// ============================================================================
// Runner Types
// ============================================================================

export type RunOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
};

/**
 * Interface for running workflows. Implemented by WorkflowRunner.
 */
export interface Runner {
  run<Input, Output>(
    workflow: Workflow<Input, Output, any, any, any>,
    input: Input,
    opts?: RunOptions<unknown>,
  ): Promise<Output>;
}

// ============================================================================
// Dispatcher Types
// ============================================================================

export type DispatchOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
};

export interface WfDispatcher<
  Wfs extends WfArray<string>
> {
  dispatch<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, NamesOfWfs<Wfs>, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<void>;

  dispatchAwaitingOutput<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, NamesOfWfs<Wfs>, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<Output>;
}

// ============================================================================
// Helper functions for wrapper classes
// ============================================================================

export function isStepsChildren(value: unknown): value is StepsChildren<any> {
  return value instanceof StepsChildren;
}

export function isRestartWrapper(value: unknown): value is RestartWrapper<unknown> {
  return value instanceof RestartWrapper;
}

export function withRestartWrapper<WfInput>(input: WfInput) {
  return new RestartWrapper(input);
}

export function isCompleteWrapper(value: unknown): value is CompleteWrapper<unknown> {
  return value instanceof CompleteWrapper;
}

export function withCompleteWrapper<WfOutput>(output: WfOutput) {
  return new CompleteWrapper(output);
}
