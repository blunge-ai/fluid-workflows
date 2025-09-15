import { z, type ZodTypeAny } from 'zod';

export type WorkflowProps = {
  name: string,
  version: number,
  numSteps: number,
  queue?: string,
};

export type WorkflowRunOptions = {
  progress: ProgressFn,
};

export type ProgressFn = (phase: string, progress: number) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output> = (input: Input, runOpts: WorkflowRunOptions) => Promise<Output>;

export type WorkflowNames<W>
  = W extends Workflow<unknown, unknown, infer N> ? N : never;

type ChildrenFor<Out>
  = { [K in keyof Out]: Workflow<Out[K], any, string> };

type ExactChildren<Out, C extends ChildrenFor<Out>>
  = keyof C extends keyof Out ? C : never;

type OutputsOfChildren<C>
  = { [K in keyof C]: C[K] extends Workflow<unknown, infer O, string> ? O : never };

type ChildrenNames<C> = keyof C & string;

// Sentinel type for type-dispatching the first .step() which gives a Workflow its input
declare const __WF_UNSET__: unique symbol;
type Unset = typeof __WF_UNSET__;

export class Workflow<Input = Unset, Output = Unset, const Names extends string = never> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;
  public queue?: string;
  public inputSchema?: ZodTypeAny;

  constructor(
    props: Pick<WorkflowProps, 'name' | 'version' | 'queue'>,
    public steps: Array<
      StepFn<unknown, unknown> |
      Workflow<unknown, unknown, string> |
      Record<string, Workflow<unknown, unknown, string>>
    >,
    inputSchema?: ZodTypeAny,
  ) {
    this.name = props.name;
    this.version = props.version;
    this.queue = props.queue;
    this.numSteps = steps.length;
    this.inputSchema = inputSchema;
  }

  static create<const Name extends string>(props: { name: Name, version: number, queue?: string }): Workflow<Unset, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny>(props: { name: Name, version: number, queue?: string, inputSchema: S }): Workflow<z.input<S>, Unset, Name>;
  static create<const Name extends string, S extends ZodTypeAny>(props: { name: Name, version: number, queue?: string, inputSchema?: S }): Workflow<Unset | z.input<S>, Unset, Name> {
    const { name, version, queue } = props;
    return new Workflow<unknown, unknown, Name>({ name, version, queue }, [], props.inputSchema) as unknown as Workflow<Unset | z.input<S>, Unset, Name>;
  }

  step<NewOutput, StepInput>(this: Workflow<Unset, Unset, Names>, stepFn: StepFn<StepInput, NewOutput>): Workflow<StepInput, NewOutput, Names>;
  step<NewOutput>(this: Workflow<Input, Unset, Names>, stepFn: StepFn<Input, NewOutput>): Workflow<Input, NewOutput, Names>;
  step<NewOutput, StepInput>(this: Workflow<Input, StepInput, Names>, stepFn: StepFn<StepInput, NewOutput>): Workflow<Input, NewOutput, Names>;
  step<NewOutput, StepInput>(this: Workflow<Unset | unknown, Unset | StepInput, Names>, stepFn: StepFn<StepInput, NewOutput>): Workflow<unknown, NewOutput, Names> {
    return new Workflow(
      { name: this.name, version: this.version, queue: this.queue },
      [ ...this.steps, stepFn as StepFn<unknown, unknown> ],
      this.inputSchema,
    );
  }

  // TODO dispatch on Workflow<Unset, Unset> like step() to allow a childStep to be the first step in a workflow
  // childStep(child1)
  childStep<ChildOutput, const ChildNames extends string>(child: Workflow<Output, ChildOutput, ChildNames>): Workflow<Input, ChildOutput, Names | ChildNames>;
  // childStep({ child1, child2 })
  childStep<const C extends ChildrenFor<Output>>(childrenMap: ExactChildren<Output, C>): Workflow<Input, OutputsOfChildren<C>, Names | ChildrenNames<C>>;
  childStep(childOrChildrenMap: unknown): unknown {
    return new Workflow<Input, unknown, string>(
      { name: this.name, version: this.version, queue: this.queue },
      [ ...this.steps, childOrChildrenMap as Workflow<unknown, unknown, string> ],
      this.inputSchema,
    );
  }
}

export async function executeQueueless<Input, Output>(workflow: Workflow<Input, Output, any>, input: Input): Promise<Output> {
  let result: unknown = input;

  if (workflow.inputSchema) {
    result = workflow.inputSchema.parse(result);
  }

  const runOptions = {
    progress: async (phase: string, progress: number) => {
      console.log(`phase: ${phase}, progress: ${progress}`);
      return { interrupt: false };
    },
  } satisfies WorkflowRunOptions;

  for (const step of workflow.steps) {
    if (step instanceof Workflow) {
      result = await executeQueueless(step as Workflow<unknown, unknown, any>, result);
    } else if (typeof step === 'function') {
      result = await step(result, runOptions);
    } else {
      const children = step as Record<string, Workflow<unknown, unknown, any>>;
      const inputRecord = result as Record<string, unknown>;
      const entries = Object.entries(children);
      const outputs = await Promise.all(entries.map(([key, child]) => executeQueueless(child, inputRecord[key])));
      result = Object.fromEntries(entries.map(([key], i) => [key, outputs[i]]));
    }
  }
  return result as Output;
}
