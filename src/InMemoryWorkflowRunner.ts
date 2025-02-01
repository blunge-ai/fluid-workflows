import { RunnableWorkflow, WorkflowRunOptions } from './Workflow';

export class InMemoryWorkflowRunner {
  async run<Input, Output>(runnableWorkflow: RunnableWorkflow<Input, Output>) {
    const workflow = runnableWorkflow.workflow;
    let result: unknown = runnableWorkflow.input;
    const runOptions = {
      progress: async (phase: string, progress: number) => {
        console.log(`phase: ${phase}, progress: ${progress}`);
        return { interrupt: false };
      }
    } satisfies WorkflowRunOptions;
    for (const step of workflow.steps) {
      result = await step(result, runOptions);
      if (result instanceof RunnableWorkflow) {
        result = await this.run(result);
      }
    }
    return result as Output;
  }
}
