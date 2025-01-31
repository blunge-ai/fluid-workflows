
export type WorkflowOptions = {
  name: string,
};

export type WorkflowRunOptions = {
  workflowName: string,
  progress: ProgressFn,
};

export type ProgressFn = (phase: string, progress: number) => { interrupted: boolean };
export type StepFn<Input, Output> = (input: Input, runOpts: WorkflowRunOptions) => Promise<Output>;

export class Workflow<Input, Output> {

  constructor(
    public opts: WorkflowOptions,
    public steps: StepFn<unknown, unknown>[],
    public inputProvided: boolean,
    public inputValue?: Input,
  ) {}

  static create<Input>(opts: WorkflowOptions) {
    return new Workflow<Input, Input>(opts, [], false);
  }

  input<Input>(input: Input) {
    return new Workflow<Input, Output>(this.opts, this.steps, true, input);
  }

  run<NewOutput>(stepFn: StepFn<Output, NewOutput | Workflow<unknown, NewOutput>>): Workflow<Input, NewOutput> {
    return new Workflow(this.opts, [...this.steps, stepFn as StepFn<unknown, unknown>], this.inputProvided);
  }
}
