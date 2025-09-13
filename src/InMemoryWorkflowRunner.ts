import { Workflow, WorkflowRunOptions, StepFn } from './Workflow';

export class InMemoryWorkflowRunner {
  async run<Input, Output>(workflow: Workflow<Input, Output>, input: Input) {
    let result: unknown = input;
    const runOptions = {
      progress: async (phase: string, progress: number) => {
        console.log(`phase: ${phase}, progress: ${progress}`);
        return { interrupt: false };
      },
    } satisfies WorkflowRunOptions;
    for (const step of workflow.steps) {
      if (step instanceof Workflow) {
        result = await this.run(step as Workflow<unknown, unknown>, result);
      } else {
        result = await (step as StepFn<unknown, unknown>)(result, runOptions);
      }
    }
    return result as Output;
  }
}
