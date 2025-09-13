
export type WorkflowProps = {
  name: string,
  version: number,
  numSteps: number,
};

export type WorkflowRunOptions = {
  progress: ProgressFn,
  dispatch: DispatchFn,
};

export type DispatchOpts = { jobId?: string, meta?: string };

export type DispatchFn = <Input, Output>(
  props: Workflow<Input, Output>,
  input: Input,
  opts?: DispatchOpts
) => DispatchableWorkflow<Input, Output>;
export type ProgressFn = (phase: string, progress: number) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output> = (input: Input, runOpts: WorkflowRunOptions) => Promise<Output>;

export class DispatchableWorkflow<Input, Output> {
  constructor(
    public workflow: Workflow<Input, Output>,
    public input: Input,
    public opts?: DispatchOpts,
  ) {}
}

export class Workflow<Input, Output> implements WorkflowProps {
  public name: string;
  public version: number;
  public numSteps: number;

  constructor(
    props: Pick<WorkflowProps, 'name' | 'version'>,
    public steps: StepFn<unknown, unknown>[],
  ) {
    this.name = props.name;
    this.version = props.version;
    this.numSteps = steps.length;
  }

  static create<Input>(props: Pick<WorkflowProps, 'name' | 'version'>) {
    return new Workflow<Input, Input>(props, []);
  }

  step<NewOutput>(stepFn: StepFn<Output, NewOutput | DispatchableWorkflow<unknown, NewOutput>>): Workflow<Input, NewOutput> {
    return new Workflow(
      { name: this.name, version: this.version },
      [ ...this.steps, stepFn as StepFn<unknown, unknown> ]
    );
  }
}
