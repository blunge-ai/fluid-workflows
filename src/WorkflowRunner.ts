import { Workflow, WorkflowRunOptions } from './Workflow';

export interface WorkflowRunner {
  start<Input, Output>(workflow: Workflow<Input, Output>): Promise<Output>;
};

export class InMemoryWorkflowRunner implements WorkflowRunner {
  async start<Input, Output>(workflow: Workflow<Input, Output>) {
    if (!workflow.inputProvided) {
      throw Error('workflow input must be provided');
    }
    let result: unknown = workflow.inputValue;
    const runOptions = {
      workflowName: workflow.opts.name,
      progress: (phase: string, progress: number) => {
        console.log(`phase: ${phase}, progress: ${progress}`);
        return { interrupted: false };
      }
    } satisfies WorkflowRunOptions;
    for (const step of workflow.steps) {
      result = await step(result, runOptions);
      if (result instanceof Workflow) {
        result = await this.start(result);
      }
    }
    return result as Output;
  }
}
