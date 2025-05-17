import { Workflow, DispatchableWorkflow, DispatchOpts, WorkflowRunOptions } from './Workflow';

export class InMemoryWorkflowRunner {
  async run<Input, Output>(workflow: Workflow<Input, Output>, input: Input) {
    let result: unknown = input;
    const runOptions = {
      progress: async (phase: string, progress: number) => {
        console.log(`phase: ${phase}, progress: ${progress}`);
        return { interrupt: false };
      },
      dispatch: <Input, Output>(
        props: Workflow<Input, Output>,
        input: Input,
        opts?: DispatchOpts
      ) => new DispatchableWorkflow(props, input, opts),
    } satisfies WorkflowRunOptions;
    for (const step of workflow.steps) {
      result = await step(result, runOptions);
      if (result instanceof DispatchableWorkflow) {
        result = await this.run(result.workflow, result.input);
      }
    }
    return result as Output;
  }
}
