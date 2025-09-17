import { z, type ZodTypeAny } from 'zod';

export type WorkflowProps = {
  name: string,
  version: number,
  numSteps: number,
};

export type WorkflowRunOptions<WfInput> = {
  progress: ProgressFn,
  restart: RestartFn<WfInput>,
};

export type ProgressFn = (phase: string, progress: number) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output, WfInput> = (input: Input, runOpts: WorkflowRunOptions<WfInput>) => Promise<Output | RestartWrapper<WfInput>>;

export type WorkflowNames<W>
  = W extends Workflow<unknown, unknown, infer N> ? N : never;

type ChildrenMap<Out, Cn extends string>
  = { [K in keyof Out]: Workflow<Out[K], any, Cn> };

type ExactChildren<Out, Cn extends string, Cm extends ChildrenMap<Out, Cn>>
  = keyof Cm extends keyof Out ? Cm : never;

type OutputsOfChildren<Cm>
  = { [K in keyof Cm]: Cm[K] extends Workflow<any, infer O, any> ? O : never };

type ChildrenNames<Cm>
  = Cm[keyof Cm] extends Workflow<any, any, infer N> ? N : never;

// Sentinel type for type-dispatching the first .step() which gives a Workflow its input
declare const __WF_UNSET__: unique symbol;
type Unset = typeof __WF_UNSET__;

export type RestartFn<WfInput> = (input: WfInput) => RestartWrapper<WfInput>;

export class RestartWrapper<T> {
  constructor(public input: T) {}
}

export function isRestartWrapper(value: unknown): value is RestartWrapper<unknown> {
  return value instanceof RestartWrapper;
}

export function withRestartWrapper<WfInput>(input: WfInput) {
  return new RestartWrapper(input);
}

export class Workflow<Input = Unset, Output = Unset, const Names extends string = never> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;
  public inputSchema?: ZodTypeAny;

  constructor(
     props: Pick<WorkflowProps, 'name' | 'version'>,
    public steps: Array<
      StepFn<unknown, unknown, unknown> |
      Workflow<unknown, unknown, string> |
      Record<string, Workflow<unknown, unknown, string>>
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

  step<NewOutput, StepInput>(this: Workflow<Unset, Unset, Names>, stepFn: StepFn<StepInput, NewOutput, StepInput>): Workflow<StepInput, NewOutput, Names>;
  step<NewOutput>(this: Workflow<Input, Unset, Names>, stepFn: StepFn<Input, NewOutput, Input>): Workflow<Input, NewOutput, Names>;
  step<NewOutput, StepInput>(this: Workflow<Input, StepInput, Names>, stepFn: StepFn<StepInput, NewOutput, Input>): Workflow<Input, NewOutput, Names>;
  step<NewOutput>(this: Workflow<any, any, Names>, stepFn: StepFn<any, any, any>): Workflow<any, NewOutput, Names> {
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
  >(child: Workflow<Output, ChildOutput, Cn>): Workflow<Input, ChildOutput, Names | Cn>;
  // childStep({ child1, child2 })
  childStep<
    const Cm extends ChildrenMap<Output, Cn>,
    const Cn extends string,
  >(childrenMap: ExactChildren<Output, Cn, Cm>): Workflow<Input, OutputsOfChildren<Cm>, Names | ChildrenNames<Cm>>;
  childStep(childOrChildrenMap: Workflow<any, unknown, string> | Record<string, Workflow<unknown, unknown, string>>): unknown {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, childOrChildrenMap  ],
      this.inputSchema,
    );
  }
}

export async function runQueueless<Input, Output>(workflow: Workflow<Input, Output, any>, input: Input): Promise<Output> {
  let result: unknown = input;

  if (workflow.inputSchema) {
    result = workflow.inputSchema.parse(result);
  }

  const runOptions: WorkflowRunOptions<Input> = {
    progress: async (phase: string, progress: number) => {
      console.log(`phase: ${phase}, progress: ${progress}`);
      return { interrupt: false };
    },
    restart: withRestartWrapper,
  };

  let i = 0;
  while (i < workflow.steps.length) {
    const step = workflow.steps[i];
    if (step instanceof Workflow) {
      result = await runQueueless(step as Workflow<unknown, unknown, any>, result);
    } else if (typeof step === 'function') {
      const stepFn = step as StepFn<unknown, unknown, Input>;
      const stepResult = await stepFn(result, runOptions);
      if (isRestartWrapper(stepResult)) {
        result = stepResult.input as Input;
        if (workflow.inputSchema) {
          result = workflow.inputSchema.parse(result);
        }
        i = 0;
        continue;
      }
      result = stepResult as unknown;
    } else {
      const children = step as Record<string, Workflow<unknown, unknown, any>>;
      const inputRecord = result as Record<string, unknown>;
      const entries = Object.entries(children);
      const outputs = await Promise.all(entries.map(([key, child]) => runQueueless(child, inputRecord[key])));
      result = Object.fromEntries(entries.map(([key], i) => [key, outputs[i]]));
    }

    i += 1;
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

export function validateWorkflowSteps(workflow: Workflow<unknown, unknown, any>, { totalSteps, currentStep }: { totalSteps: number, currentStep: number }) {
  if (currentStep >= totalSteps) {
    throw Error(`inconsistent jobData: current step is ${currentStep} but expected value smaller than ${totalSteps}`);
  }
  if (workflow.steps.length !== totalSteps) {
    throw Error(`job totalSteps mismatch: expected ${workflow.steps.length}, received ${totalSteps}`);
  }
}

export function collectWorkflows(workflows: ReadonlyArray<Workflow<any, any>>): Workflow<any, any>[] {

  const result: Workflow<any, any>[] = [];
  const seen = new Set<string>();

  const visit = (wf: Workflow<any, any>) => {
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
        visit(step as Workflow<any, any>);
      } else if (typeof step === 'function') {
        continue;
      } else {
        const record = step as Record<string, Workflow<any, any>>;
        for (const child of Object.values(record)) {
          visit(child);
        }
      }
    }
  };

  for (const root of workflows) {
    visit(root);
  }
  return result;
}
