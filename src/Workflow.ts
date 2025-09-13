
export type WorkflowProps = {
  name: string,
  version: number,
  numSteps: number,
};

export type WorkflowRunOptions = {
  progress: ProgressFn,
};

export type ProgressFn = (phase: string, progress: number) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output> = (input: Input, runOpts: WorkflowRunOptions) => Promise<Output>;

// Helper types for named child steps
type ChildrenFor<Out> = { [K in keyof Out]: Workflow<Out[K], any> };
type ExactChildren<Out, C extends ChildrenFor<Out>> = Exclude<keyof C, keyof Out> extends never ? C : never;

type OutputsOfChildren<C> = { [K in keyof C]: C[K] extends Workflow<any, infer O> ? O : never };

export class Workflow<Input, Output> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;

  constructor(
    props: Pick<WorkflowProps, 'name' | 'version'>,
    public steps: Array<StepFn<unknown, unknown> | Workflow<unknown, unknown> | Record<string | number | symbol, Workflow<unknown, unknown>>>,
  ) {
    this.name = props.name;
    this.version = props.version;
    this.numSteps = steps.length;
  }

  static create<Input>(props: Pick<WorkflowProps, 'name' | 'version'>) {
    return new Workflow<Input, Input>(props, []);
  }

  step<NewOutput>(stepFn: StepFn<Output, NewOutput>): Workflow<Input, NewOutput> {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, stepFn as StepFn<unknown, unknown> ]
    );
  }

  // single child
  childStep<ChildOutput>(child: Workflow<Output, ChildOutput>): Workflow<Input, ChildOutput>;
  // named children
  childStep<C extends ChildrenFor<Output>>(children: ExactChildren<Output, C>): Workflow<Input, OutputsOfChildren<C>>;
  childStep(childOrChildren: unknown): any {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, childOrChildren as Workflow<unknown, unknown> ]
    );
  }
}
