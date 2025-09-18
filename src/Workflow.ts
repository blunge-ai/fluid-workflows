import { z, type ZodTypeAny } from 'zod';
import { WorkflowProgressInfo } from './WorkflowJob';

export type WorkflowProps = {
  name: string,
  version: number,
  numSteps: number,
};

export type WorkflowRunOptions<WfInput, WfOutput, StepInput> = {
  progress: ProgressFn,
  update: UpdateFn<StepInput>,
  restart: RestartFn<WfInput>,
  complete: CompleteFn<WfOutput>,
};

export type ProgressFn
  = (progressInfo: WorkflowProgressInfo) => Promise<{ interrupt: boolean }>;
export type UpdateFn<StepInput>
  = (stepInput: StepInput, progressInfo?: WorkflowProgressInfo) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output, WfInput, WfOutput>
  = (input: Input, runOpts: WorkflowRunOptions<WfInput, WfOutput, Input>) => Promise<Output>;

export type WorkflowNames<W>
  = W extends Workflow<any, any, infer N, any, any> ? N : never;

type ChildrenMap<Out, Cn extends string>
  = { [K in keyof Out]: Workflow<Out[K], any, Cn, any, any> };

type ExactChildren<Out, Cn extends string, Cm extends ChildrenMap<Out, Cn>>
  = keyof Cm extends keyof Out ? Cm : never;

type OutputsOfChildren<Cm>
  = { [K in keyof Cm]: Cm[K] extends Workflow<any, infer O, string, any, any> ? O : never };

type ChildrenNames<Cm>
  = Cm[keyof Cm] extends Workflow<any, any, infer N, any, any> ? N : never;

// Sentinel type for type-dispatching the first .step() which gives a Workflow its input
declare const __WF_UNSET__: unique symbol;
type Unset = typeof __WF_UNSET__;

export type RestartFn<WfInput> = (input: WfInput) => RestartWrapper<WfInput>;
export type CompleteFn<WfOutput> = <T extends WfOutput>(output: T) => CompleteWrapper<T>;

export type StripSentinel<T> = T extends Unset ? never : T;

export type StripCtrl<StepOutput>
  = Exclude<StepOutput, RestartWrapper<any> | CompleteWrapper<any>>

type MkCtrlOut<Ctrl, StepOutput>
  = Ctrl
  | (StepOutput extends CompleteWrapper<infer U> ? U : never);

type MkOutput<Ctrl, StepOutput>
  = MkCtrlOut<Ctrl, StepOutput>
  | StripCtrl<StepOutput>;

export class RestartWrapper<WfInput> {
  public __type = 'RestartWrapper' as const;
  constructor(public input: WfInput) {}
}

export function isRestartWrapper(value: unknown): value is RestartWrapper<unknown> {
  return value instanceof RestartWrapper;
}

export function withRestartWrapper<WfInput>(input: WfInput) {
  return new RestartWrapper(input);
}

export class CompleteWrapper<WfOutput> {
  public __type = 'CompleteWrapper' as const;
  constructor(public output: WfOutput) {}
}

export function isCompleteWrapper(value: unknown): value is CompleteWrapper<unknown> {
  return value instanceof CompleteWrapper;
}

export function withCompleteWrapper<WfOutput>(output: WfOutput) {
  return new CompleteWrapper(output);
}

export class Workflow<Input = Unset, Output = never, const Names extends string = never, NextOutput = never, CtrlOutput = never> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;
  public inputSchema?: ZodTypeAny;

  constructor(
    props: Pick<WorkflowProps, 'name' | 'version'>,
    public steps: Array<
      StepFn<unknown, unknown, unknown, unknown> |
      Workflow<unknown, unknown, string, any, any> |
      Record<string, Workflow<unknown, unknown, string, any, any>>
    >,
    inputSchema?: ZodTypeAny,
  ) {
    this.name = props.name;
    this.version = props.version;

    this.numSteps = steps.length;
    this.inputSchema = inputSchema;
  }

  static create<const Name extends string>(props: { name: Name, version: number }): Workflow<Unset, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny>(props: { name: Name, version: number, inputSchema: S }): Workflow<z.input<S>, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny>(props: { name: Name, version: number, inputSchema?: S }): Workflow<Unset | z.input<S>, Unset, Name> {
    const { name, version } = props;
    return new Workflow<unknown, unknown, Name>({ name, version }, [], props.inputSchema) as unknown as Workflow<Unset | z.input<S>, Unset, Name>;
  }

  step<StepInput, StepOutput, SwfOutput>(
    this: Workflow<Unset, Output, Names, NextOutput, CtrlOutput>,
    stepFn: StepFn<StepInput, StepOutput, StepInput, SwfOutput>
  ): Workflow<StepInput, MkOutput<CtrlOutput, StepOutput>, Names, StripCtrl<StepOutput>, MkCtrlOut<CtrlOutput, StepOutput>>;
  step<StepOutput, WfOutput, SwfOutput>(
    this: Workflow<Input, never, Names, never, never>,
    stepFn: StepFn<Input, StepOutput, Input, SwfOutput>
  ): Workflow<Input, MkOutput<CtrlOutput, StepOutput>, Names, StripCtrl<StepOutput>, MkCtrlOut<CtrlOutput, StepOutput>>;
  step<StepInput, StepOutput, SwfOutput>(
    this: Workflow<Input, Output, Names, StepInput, CtrlOutput>,
    stepFn: StepFn<StepInput, StepOutput, Input, SwfOutput>
  ): Workflow<Input, MkOutput<CtrlOutput, StepOutput>, Names, StripCtrl<StepOutput>, MkCtrlOut<CtrlOutput, StepOutput>>;
  step(
    stepFn: StepFn<any, any, any, any>
  ): Workflow<any, any, string, any, any> {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, stepFn ],
      this.inputSchema,
    );
  }

  // TODO dispatch on Workflow<Unset, Unset> like step() to allow a childStep to be the first step in a workflow
  // childStep(child1)
  childStep<
    ChildOutput,
    const Cn extends string,
  >(child: Workflow<NextOutput, ChildOutput, Cn, any, any>): Workflow<Input, MkOutput<CtrlOutput, ChildOutput>, Names | Cn, ChildOutput, CtrlOutput>;
  // childStep({ child1, child2 })
  childStep<
    const Cm extends ChildrenMap<NextOutput, Cn>,
    const Cn extends string,
  >(childrenMap: ExactChildren<NextOutput, Cn, Cm>): Workflow<Input, MkOutput<CtrlOutput, OutputsOfChildren<Cm>>, Names | ChildrenNames<Cm>, OutputsOfChildren<Cm>, CtrlOutput>;
  childStep(childOrChildrenMap: Workflow<any, any, string, any, any> | Record<string, Workflow<any, any, string, any, any>>): Workflow<any, any, string, any, any> {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, childOrChildrenMap  ],
      this.inputSchema,
    );
  }
}

export async function runQueueless<Input, Output, Names extends string, NextOutput, CtrlOutput>(workflow: Workflow<Input, Output, Names, NextOutput, CtrlOutput>, input: Input) {
  let result: unknown = input;

  if (workflow.inputSchema) {
    result = workflow.inputSchema.parse(result);
  }

  let stepIndex = 0;

  const runOptions: WorkflowRunOptions<Input, Output, unknown> = {
    progress: async (progressInfo: WorkflowProgressInfo) => {
      console.log(`progress: ${JSON.stringify(progressInfo)}`);
      return { interrupt: false };
    },
    update: async (_stepInput: unknown, progressInfo?: WorkflowProgressInfo) => {
      if (progressInfo) {
        console.log(`progress: ${JSON.stringify(progressInfo)}`);
      }
      return { interrupt: false };
    },
    restart: withRestartWrapper,
    complete: withCompleteWrapper,
  };

  while (stepIndex < workflow.steps.length) {
    const step = workflow.steps[stepIndex];
    if (step instanceof Workflow) {
      result = await runQueueless(step as unknown as Workflow<unknown, unknown, any, any, any>, result);
    } else if (typeof step === 'function') {
      const stepFn = step as StepFn<unknown, unknown, Input, Output>;
      const stepResult = await stepFn(result, runOptions);
      if (isRestartWrapper(stepResult)) {
        result = stepResult.input as Input;
        if (workflow.inputSchema) {
          result = workflow.inputSchema.parse(result);
        }
        stepIndex = 0;
        continue;
      }
      if (isCompleteWrapper(stepResult)) {
        return stepResult.output as Output;
      }
      result = stepResult as unknown;
    } else {
      const children = step as unknown as Record<string, Workflow<unknown, unknown, any, any, any>>;
      const inputRecord = result as Record<string, unknown>;
      const entries = Object.entries(children);
      const outputs = await Promise.all(entries.map(([key, child]) => runQueueless(child as any, inputRecord[key] as any)));
      result = Object.fromEntries(entries.map(([key], i) => [key, outputs[i]]));
    }

    stepIndex += 1;
  }
  return result as Output;
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
  if (workflow.steps.length !== totalSteps) {
    throw Error(`job totalSteps mismatch: expected ${workflow.steps.length}, received ${totalSteps}`);
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
    for (const step of wf.steps) {
      if (step instanceof Workflow) {
        visit(step as unknown as Workflow<unknown, unknown>);
      } else if (typeof step === 'function') {
        continue;
      } else {
        for (const child of Object.values(step)) {
          visit(child as unknown as Workflow<unknown, unknown>);
        }
      }
    }
  };

  for (const root of workflows) {
    visit(root);
  }
  return result;
}
