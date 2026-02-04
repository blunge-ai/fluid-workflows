import { z, type ZodTypeAny } from 'zod';
import type {
  Workflow,
  WorkflowProps,
  StepFn,
  ParallelMap,
  Runner,
  RunOptions,
  StripCtrl,
  WfMeta,
  WfUpdateInfo,
  WfStatus,
} from './types';
import {
  StepsChildren,
  RestartWrapper,
  CompleteWrapper,
  isStepsChildren,
} from './types';
import { WfRunner } from './WfRunner';
import type { StatusListener } from './storage/Storage';

// Re-export everything from types.ts
export * from './types';

// Internal type helpers (not exported from types.ts)
type ParallelOutputs<Cm, WfInput, WfOutput>
  = { [K in keyof Cm]: Cm[K] extends Workflow<any, infer O, string, any, any> ? O
    : Cm[K] extends StepFn<any, infer O, WfInput, WfOutput> ? StripCtrl<O>
    : never };

type ParallelNames<Cm>
  = Cm[keyof Cm] extends Workflow<any, any, infer N, any, any> ? N : never;

type MkCtrlOut<Ctrl, StepOutput>
  = Ctrl
  | (StepOutput extends CompleteWrapper<infer U> ? U : never);

type MkOutput<Ctrl, StepOutput>
  = MkCtrlOut<Ctrl, StepOutput>
  | StripCtrl<StepOutput>;

// Sentinel type for type-dispatching the first .step() which gives a Workflow its input
declare const __WF_UNSET__: unique symbol;
type Unset = typeof __WF_UNSET__;

export type StripSentinel<T> = T extends Unset ? never : T;

export class WfBuilder<Input = Unset, Output = never, const Names extends string = never, NextOutput = never, CtrlOutput = never> implements Workflow<Input, Output, Names, NextOutput, CtrlOutput> {
  public name: Names;
  public version: number;
  public numSteps: number;
  public inputSchema?: ZodTypeAny;
  public outputSchema?: ZodTypeAny;
  private _runner?: Runner;

  constructor(
    props: { name: Names, version: number },
    public stepFns: Array<
      StepFn<unknown, unknown, unknown, unknown> |
      Workflow<unknown, unknown, string, any, any> |
      Record<string, Workflow<unknown, unknown, string, any, any>> |
      StepsChildren<any>
    >,
    inputSchema?: ZodTypeAny,
    outputSchema?: ZodTypeAny,
    runner?: Runner,
  ) {
    this.name = props.name;
    this.version = props.version;

    this.numSteps = stepFns.length;
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
    this._runner = runner;
  }

  static create<const Name extends string>(props: { name: Name, version: number, runner?: Runner }): WfBuilder<Unset, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny>(props: { name: Name, version: number, inputSchema: S, runner?: Runner }): WfBuilder<z.input<S>, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny, O extends ZodTypeAny>(props: { name: Name, version: number, inputSchema: S, outputSchema: O, runner?: Runner }): WfBuilder<z.input<S>, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny, O extends ZodTypeAny>(props: { name: Name, version: number, inputSchema?: S, outputSchema?: O, runner?: Runner }): WfBuilder<Unset | z.input<S>, Unset, Name> {
    const { name, version, runner } = props;
    return new WfBuilder<unknown, unknown, Name>({ name, version }, [], props.inputSchema, props.outputSchema, runner) as unknown as WfBuilder<Unset | z.input<S>, Unset, Name>;
  }

  private async getRunner(): Promise<Runner> {
    if (this._runner) {
      return this._runner;
    }
    const runner = new WfRunner({ workflows: [this as Workflow], jobTimeoutMs: 60000 });
    this._runner = runner;
    return runner;
  }

  async run<Meta extends WfMeta = WfMeta>(input: Input, opts?: RunOptions<Meta>): Promise<Output> {
    const runner = await this.getRunner();
    return runner.run(this, input, opts) as Promise<Output>;
  }

  async subscribe<Meta extends WfMeta = WfMeta, Info extends WfUpdateInfo = WfUpdateInfo>(
    jobId: string, 
    listener: StatusListener<WfStatus<Meta, Info>>
  ): Promise<() => void> {
    const runner = await this.getRunner();
    return (runner as any).subscribe(jobId, listener);
  }

  // .step(workflow) - first step with child workflow
  step<
    ChildInput,
    ChildOutput,
    const Cn extends string,
    ChildNext,
    ChildCtrl,
  >(
    this: WfBuilder<Unset, Output, Names, NextOutput, CtrlOutput>,
    child: Workflow<ChildInput, ChildOutput, Cn, ChildNext, ChildCtrl>
  ): WfBuilder<ChildInput, MkOutput<CtrlOutput, ChildOutput>, Names | Cn, ChildInput & ChildOutput, CtrlOutput>;
  // .step(workflow) - subsequent step with child workflow, receives accumulated state, output is merged
  step<
    ChildInput,
    ChildOutput,
    const Cn extends string,
    ChildNext,
    ChildCtrl,
  >(
    this: WfBuilder<Input, Output, Names, NextOutput, CtrlOutput>,
    child: Workflow<ChildInput, ChildOutput, Cn, ChildNext, ChildCtrl>
  ): WfBuilder<Input, MkOutput<CtrlOutput, NextOutput & ChildOutput>, Names | Cn, NextOutput & ChildOutput, CtrlOutput>;
  // .step(fn) - first step, infers workflow input from function parameter (no schema)
  step<StepInput, StepOutput, SwfOutput>(
    this: WfBuilder<Unset, Output, Names, NextOutput, CtrlOutput>,
    stepFn: StepFn<StepInput, StepOutput, StepInput, SwfOutput>
  ): WfBuilder<StepInput, MkOutput<CtrlOutput, StepOutput>, Names, StepInput & StripCtrl<StepOutput>, MkCtrlOut<CtrlOutput, StepOutput>>;
  // .step(fn) - first step with schema input (Input is set, Output is Unset, NextOutput is never)
  step<StepOutput, SwfOutput>(
    this: WfBuilder<Input, Unset, Names, never, never>,
    stepFn: StepFn<Input, StepOutput, Input, SwfOutput>
  ): WfBuilder<Input, MkOutput<never, StepOutput>, Names, Input & StripCtrl<StepOutput>, MkCtrlOut<never, StepOutput>>;
  // .step(fn) - subsequent step with function
  step<StepOutput, WfOutput, SwfOutput>(
    this: WfBuilder<Input, never, Names, never, never>,
    stepFn: StepFn<Input, StepOutput, Input, SwfOutput>
  ): WfBuilder<Input, MkOutput<CtrlOutput, StepOutput>, Names, Input & StripCtrl<StepOutput>, MkCtrlOut<CtrlOutput, StepOutput>>;
  step<StepOutput, SwfOutput>(
    this: WfBuilder<Input, Output, Names, NextOutput, CtrlOutput>,
    stepFn: StepFn<NextOutput, StepOutput, Input, SwfOutput>
  ): WfBuilder<Input, MkOutput<CtrlOutput, StepOutput>, Names, NextOutput & StripCtrl<StepOutput>, MkCtrlOut<CtrlOutput, StepOutput>>;
  step(
    stepFnOrChild: StepFn<any, any, any, any> | Workflow<any, any, string, any, any>
  ): WfBuilder<any, any, string, any, any> {
    return new WfBuilder(
      { name: this.name, version: this.version },
      [ ...this.stepFns, stepFnOrChild ],
      this.inputSchema,
      this.outputSchema,
      this._runner,
    );
  }

  // .parallel() as first step - infers workflow input from children
  parallel<
    ChildInput,
    const Cm extends ParallelMap<ChildInput, ChildInput, Output>,
  >(
    this: WfBuilder<Unset, Output, Names, NextOutput, CtrlOutput>,
    childrenMap: Cm
  ): WfBuilder<ChildInput, MkOutput<CtrlOutput, ChildInput & ParallelOutputs<Cm, ChildInput, Output>>, Names | ParallelNames<Cm>, ChildInput & ParallelOutputs<Cm, ChildInput, Output>, CtrlOutput>;
  // .parallel() - invoke children with the full accumulated input, map outputs to keys
  parallel<
    const Cm extends ParallelMap<Input & NextOutput, Input, Output>,
  >(childrenMap: Cm): WfBuilder<Input, MkOutput<CtrlOutput, Input & NextOutput & ParallelOutputs<Cm, Input, Output>>, Names | ParallelNames<Cm>, Input & NextOutput & ParallelOutputs<Cm, Input, Output>, CtrlOutput>;
  parallel<
    const Cm extends ParallelMap<any, any, any>,
  >(childrenMap: Cm): WfBuilder<any, any, any, any, any> {
    return new WfBuilder(
      { name: this.name, version: this.version },
      [ ...this.stepFns, new StepsChildren(childrenMap) ],
      this.inputSchema,
      this.outputSchema,
      this._runner,
    );
  }
}

export function findWorkflow(workflows: Workflow<unknown, unknown>[], props: Pick<WorkflowProps, 'name' | 'version'>) {
  const { name, version } = props;
  const workflow = workflows.find((w) => w.name === name && w.version === version);
  if (!workflow) {
    throw Error(`no workflow found for '${name}' version ${version}`);
  }
  return workflow;
}

export function validateWorkflowSteps(workflow: Workflow<unknown, unknown>, { totalSteps, currentStep }: { totalSteps: number, currentStep: number }) {
  if (currentStep >= totalSteps) {
    throw Error(`inconsistent jobData: current step is ${currentStep} but expected value smaller than ${totalSteps}`);
  }
  if (workflow.stepFns.length !== totalSteps) {
    throw Error(`job totalSteps mismatch: expected ${workflow.stepFns.length}, received ${totalSteps}`);
  }
}

export function collectWorkflows(workflows: Workflow<unknown, unknown>[]): Workflow<unknown, unknown>[] {

  const result: Workflow<unknown, unknown>[] = [];
  const seen = new Set<string>();

  const visit = (wf: Workflow<unknown, unknown>) => {
    const key = `${wf.name}:${wf.version}`;
    if (seen.has(key)) {
      if (!result.includes(wf)) {
        throw Error(`duplicate workflow with mismatching instance identity: ${key}`);
      }
      return;
    };
    seen.add(key);
    result.push(wf);
    for (const step of wf.stepFns) {
      if (step instanceof WfBuilder) {
        visit(step as unknown as Workflow<unknown, unknown>);
      } else if (typeof step === 'function') {
        continue;
      } else if (isStepsChildren(step)) {
        for (const child of Object.values(step.children)) {
          if (child instanceof WfBuilder) {
            visit(child as unknown as Workflow<unknown, unknown>);
          }
        }
      }
    }
  };

  for (const root of workflows) {
    visit(root);
  }
  return result;
}
