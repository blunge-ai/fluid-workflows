
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

// Helper types for named child steps
export type WorkflowNames<W> = W extends Workflow<any, any, infer N> ? N : never;

type ChildrenFor<Out> = { [K in keyof Out]: Workflow<Out[K], any, any> };
type ExactChildren<Out, C extends ChildrenFor<Out>> = Exclude<keyof C, keyof Out> extends never ? C : never;

type OutputsOfChildren<C> = { [K in keyof C]: C[K] extends Workflow<any, any, any> ? (C[K] extends Workflow<any, infer O, any> ? O : never) : never };
type ChildrenNames<C> = { [K in keyof C]: C[K] extends Workflow<any, any, infer N> ? N : never }[keyof C];

export class Workflow<Input, Output, Names extends string = never> implements WorkflowProps {
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

  static create<Input>(props: { name: string, version: number, queue?: string }): Workflow<Input, Input, string>;
  static create<Input, Name extends string>(props: { name: Name, version: number, queue?: string }): Workflow<Input, Input, Name>;
  static create<Input, Name extends string>(props: { name: Name, version: number, queue?: string }) {
    return new Workflow<Input, Input, Name>(props, []);
  }

  step<NewOutput>(stepFn: StepFn<Output, NewOutput>): Workflow<Input, NewOutput, Names> {
    return new Workflow(
      { name: this.name, version: this.version, queue: this.queue },
      [ ...this.steps, stepFn as StepFn<unknown, unknown> ]
    );
  }

  // single child
  childStep<ChildOutput, ChildNames extends string>(child: Workflow<Output, ChildOutput, ChildNames>): Workflow<Input, ChildOutput, Names | ChildNames>;
  // named children
  childStep<C extends ChildrenFor<Output>>(children: ExactChildren<Output, C>): Workflow<Input, OutputsOfChildren<C>, Names | ChildrenNames<C>>;
  childStep(childOrChildren: unknown): any {
    return new Workflow(
      { name: this.name, version: this.version, queue: this.queue },
      [ ...this.steps, childOrChildren as Workflow<unknown, unknown, any> ]
    );
  }
}
