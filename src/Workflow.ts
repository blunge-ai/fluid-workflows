
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

export type WorkflowNames<W> = W extends Workflow<any, any, infer N> ? N : never;

type ChildrenFor<Out> = { [K in keyof Out]: Workflow<Out[K], any, any> };
type ExactChildren<Out, C extends ChildrenFor<Out>> = Exclude<keyof C, keyof Out> extends never ? C : never;

type OutputsOfChildren<C> = { [K in keyof C]: C[K] extends Workflow<any, any, any> ? (C[K] extends Workflow<any, infer O, any> ? O : never) : never };
type ChildrenNames<C> = Extract<keyof C, string>;

// Sentinel type for type-dispatching the first .step() which give's a Workflow it's input
declare const __WF_UNSET__: unique symbol;
type Unset = typeof __WF_UNSET__;

export class Workflow<Input = Unset, Output = Unset, const Names extends string = never> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;
  public queue?: string;

  constructor(
    props: Pick<WorkflowProps, 'name' | 'version' | 'queue'>,
    public steps: Array<StepFn<unknown, unknown> | Workflow<unknown, unknown, any> | Record<string | number | symbol, Workflow<unknown, unknown, any>>>,
  ) {
    this.name = props.name;
    this.version = props.version;
    this.queue = props.queue;
    this.numSteps = steps.length;
  }

  static create<const Name extends string>(props: { readonly name: Name, version: number, queue?: string }): Workflow<Unset, Unset, Name> {
    return new Workflow<Unset, Unset, Name>(props, []);
  }

  step<NewOutput, StepInput>(this: Workflow<Unset, Unset, Names>, stepFn: StepFn<StepInput, NewOutput>): Workflow<StepInput, NewOutput, Names>;
  step<NewOutput, StepInput>(this: Workflow<Input, StepInput, Names>, stepFn: StepFn<StepInput, NewOutput>): Workflow<Input, NewOutput, Names>;
  step<NewOutput, StepInput>(this: Workflow<any, any, Names>, stepFn: StepFn<StepInput, NewOutput>): Workflow<any, any, Names> {
    return new Workflow<unknown, NewOutput, Names>(
      { name: this.name, version: this.version, queue: this.queue },
      [ ...this.steps, stepFn as StepFn<unknown, unknown> ]
    );
  }

  // childStep(child1)
  childStep<ChildOutput, const ChildNames extends string>(child: Workflow<Output, ChildOutput, ChildNames>): Workflow<Input, ChildOutput, Names | ChildNames>;
  // childStep({ child1, child2 })
  childStep<const C extends ChildrenFor<Output>>(childrenMap: ExactChildren<Output, C>): Workflow<Input, OutputsOfChildren<C>, Names | ChildrenNames<C>>;
  childStep(childOrChildrenMap: unknown): any {
    return new Workflow<Input, any, any>(
      { name: this.name, version: this.version, queue: this.queue },
      [ ...this.steps, childOrChildrenMap as Workflow<unknown, unknown, any> ]
    );
  }
}
