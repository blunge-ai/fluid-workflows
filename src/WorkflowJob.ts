import { WorkflowProps } from "./Workflow"

// The job data (or state) used by the JobQueueWorkflowRunner to run a workflow

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

export function makeWorkflowJobData<Input = unknown>({ props, input }: { props: WorkflowProps, input: Input }) {
  return {
    name: props.name,
    version: props.version,
    totalSteps: props.numSteps,
    step: 0,
    input: input,
  } satisfies WorkflowJobData<Input>;
}
