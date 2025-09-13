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
      } else if (typeof step === 'function') {
        result = await step(result, runOptions);
      } else {
        const children = step as Record<string | number | symbol, Workflow<unknown, unknown>>;
        const inputRecord = result as Record<string | number | symbol, unknown>;
        const entries = Object.entries(children);
        const outputs = await Promise.all(entries.map(([key, child]) => this.run(child, inputRecord[key])));
        result = Object.fromEntries(entries.map(([key], i) => [key, outputs[i]]));
      }
    }
    return result as Output;
  }
}
