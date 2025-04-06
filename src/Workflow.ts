
export type WorkflowOptions = {
  name: string,
  version: number,
};

export type WorkflowRunOptions = {
  progress: ProgressFn,
};

export type ProgressFn = (phase: string, progress: number) => Promise<{ interrupt: boolean }>;
export type StepFn<Input, Output> = (input: Input, runOpts: WorkflowRunOptions) => Promise<Output>;

export class RunnableWorkflow<Input, Output> {
  constructor(
    public workflow: Workflow<Input, Output>,
    public input: Input,
  ) {}
}

export class Workflow<Input, Output> {

  constructor(
    public opts: WorkflowOptions,
    public steps: StepFn<unknown, unknown>[],
  ) {}

  static create<Input>(opts: WorkflowOptions) {
    return new Workflow<Input, Input>(opts, []);
  }

  withInput(input: Input) {
    return new RunnableWorkflow<Input, Output>(this, input);
  }

  step<NewOutput>(stepFn: StepFn<Output, NewOutput | RunnableWorkflow<unknown, NewOutput>>): Workflow<Input, NewOutput> {
    return new Workflow(this.opts, [...this.steps, stepFn as StepFn<unknown, unknown>]);
  }
}
