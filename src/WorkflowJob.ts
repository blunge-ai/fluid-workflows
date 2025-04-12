
export type WorkflowJobData<Input = unknown> = {
  name: string,
  version: number,
  totalSteps: number,
  step: number,
  input: Input,
};

export type WorkflowProgressInfo = {
  phase: string,
  progress: number,
};
