export class Workflow {
    opts;
    steps;
    inputProvided;
    inputValue;
    constructor(opts, steps, inputProvided, inputValue) {
        this.opts = opts;
        this.steps = steps;
        this.inputProvided = inputProvided;
        this.inputValue = inputValue;
    }
    static create(opts) {
        return new Workflow(opts, [], false);
    }
    input(input) {
        return new Workflow(this.opts, this.steps, true, input);
    }
    run(stepFn) {
        return new Workflow(this.opts, [...this.steps, stepFn], this.inputProvided);
    }
}
