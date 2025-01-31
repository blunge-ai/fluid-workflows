import { Workflow } from './Workflow';
;
export class InMemoryWorkflowRunner {
    async start(workflow) {
        if (!workflow.inputProvided) {
            throw Error('workflow input must be provided');
        }
        let result = workflow.inputValue;
        const runOptions = {
            workflowName: workflow.opts.name,
            progress: (phase, progress) => {
                console.log(`phase: ${phase}, progress: ${progress}`);
                return { interrupted: false };
            }
        };
        for (const step of workflow.steps.slice(1)) {
            result = await step(result, runOptions);
            if (result instanceof Workflow) {
                result = await this.start(result);
            }
        }
        return result;
    }
}
