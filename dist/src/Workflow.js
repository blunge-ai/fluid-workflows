export class RunnableWorkflow {
    workflow;
    input;
    constructor(workflow, input) {
        this.workflow = workflow;
        this.input = input;
    }
}
export class Workflow {
    opts;
    steps;
    constructor(opts, steps) {
        this.opts = opts;
        this.steps = steps;
    }
    static create(opts) {
        return new Workflow(opts, []);
    }
    input(input) {
        return new RunnableWorkflow(this, input);
    }
    run(stepFn) {
        return new Workflow(this.opts, [...this.steps, stepFn]);
    }
}
