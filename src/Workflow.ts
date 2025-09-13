
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

export class Workflow<Input, Output> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;

  constructor(
    props: Pick<WorkflowProps, 'name' | 'version'>,
    public steps: Array<StepFn<unknown, unknown> | Workflow<unknown, unknown>>,
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

  childStep<ChildOutput>(child: Workflow<Output, ChildOutput>): Workflow<Input, ChildOutput> {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, child as Workflow<unknown, unknown> ]
    );
  }
}
