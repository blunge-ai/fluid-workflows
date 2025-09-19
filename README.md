# fluid-workflows

Type-safe workflow composition and job-queue execution with adapters.

The main problem this project attempts to solve is the separation of workflow definition and
execution.

## Install

```bash
npm install @blunge/fluid-workflows bullmq ioredis zod
```

## Quick Start (queueless for testing)

This defines a simple workflow with a single step that adds two numbers togther.

```ts
import * as fwf from '@blunge/fluid-workflows';

const wf = fwf.workflow({ name: 'add', version: 1 })
  .step(async ({ a, b }: { a: number; b: number }) => ({ sum: a + b }));

// Runs the workflow in the current process to completion, useful for testing
const out = await fwf.runQueueless(wf, { a: 2, b: 3 });
console.log(out.sum); // 5
```

## BullMQ Adapter

To use the `BullMQAdapter` you need to have redis running and the `REDIS_URL` env variable defined.

For example, to run tests:

```bash
REDIS_URL=redis://127.0.0.1:6379 npm run test
```

## Using job queues

```ts
import * as fwf from '@blunge/fluid-workflows';

// Workflow definitions

const child = fwf.workflow({ name: 'child', version: 1 })
  .step(async ({ s }: { s: string }) => ({ s2: `child(${s})` }));

const parent = fwf.workflow({ name: 'parent', version: 1 })
  .step(async ({ n }: { n: number }) => ({ s: `n=${n}` }))
  .childStep(child)
  .step(async ({ s2 }) => ({ out: s2 }));

// Configuration

const { runner, dispatcher } = fwf.config({
  engine: new fwf.BullMqAdapter({ attempts: 1, lockTimeoutMs: 8000 }),
  workflows: [parent],
  queues: { parent: 'parent-queue, child: 'child-queue' } ,
});

// On a worker machine, run parent workflows
const stopParent = runner.run(['parent-queue']);

// Possibly on a separate machine, run child workflows
const stopChild = runner.run(['child-queue']);

// Alternatively, run all workflows on the same machine
// const stopAll = runner.run('all');

// On the app server, dispatch workflows and optionally await the output
const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
console.log(result.out); // 'child(n=5)'

await stopParent();
await stopChild();
```

## Advanced features

### Parallel execution

You can pass a map of `{[key]: Workflow}` to `childStep()` to run multiple children in parallel. The
keys can be arbitrary but must match the output map of the previous step and the child results will be
mapped to the same keys as the input to the next step.

```ts
const child1 = fwf.workflow({ name: 'child2', version: 1 })
  .step(async (input: string) => 'child1-output');

const child2 = fwf.workflow({ name: 'child2', version: 1 })
  .step(async (input: string) => 'child2-output');

const parent = fwf.workflow({ name: 'parent', version: 1 })
  .step(async ({ n }: { n: number }) => ({ 
    child1: 'child1-input',
    child2: 'child2-input',
  }))
  .childStep({ 
    child1, 
    child2
  })
  .step(async ({
    child1 // 'child1-output'
    child2 // 'child2 output'
  }) => { ... });
```

### Zod schema support

You may have noticed that we need to specify the type of the input to the first step explicitly. You
can pass a **zod** schema to automatically infer the input type to the first step. The inputs to the
workflow will also be validated against the schema during workflow execution.

```ts
const inputSchema = z.object({ a: z.number(), b: z.number() });

const parent = fwf.workflow({ name: 'parent', version: 1, inputSchema })
  .step(async ({ a, b }) => ({ 
     ...
  }));
```

### Control flow

* `restart` - allows a workflow to be restarted with new input data
* `complete` - allows the workflow to be completed early with the given result

The control flow functions only wrap the argument and return it, and the wrapped result has to be
returned by the step to indicate to the workflow runner what shoud happen next.

```ts
const parent = fwf.workflow({ name: 'parent', version: 1, inputSchema })
  .step(async ({ iterations }, { restart, complete }) => ({
     if (iterations < 10) {
       return restart({ iterations: iterations + 1 });
     }
     // will skip any following steps
     return complete({ iterations });
  }))
  .step(async () => {
     // will never reach here
  });

const out = await fwf.runQueueless(wf, { iterations: 0 });
console.log(out.iterations); // 10
```

### progress and update

__this is work in progress__

The `progress` function allows progress to be reported. Progress events can be subscribed to through
the `engine`. If the `progress` function returns a true value for `cancelled`, the step should return
or throw an `Error`. Cancelling a workflow that is in progress is informational only, and the
workflow can run to successful completion even after it has been cancelled.

The `update` function allows the step's input data to be updated, such that if the step does not run
to completion, it will be retried with the updated input data. Progress can also be reported through
the update function by passing an optional second argument.

```ts
const parent = fwf.workflow({ name: 'parent', version: 1, inputSchema })
  .step(async ({ iterations, maxIterations }, { progress, update }) => ({
     for (; iterations > 0; iterations--) {

       // by convention we pass a value from 0 to 1 to report the current progress
       const progress = Math.min(1, iterations / maxIterations);

       // calling update here updates the input data; the optional second argument reports the current progress
       const { cancelled } = await update({ iterations }, { progress });

       // or just report progress, without updating the step's input data
       // const { cancelled } = await progress({ progress });

       if (cancelled) {
         throw Error('cancelled');
       }
     }
  }));
```


## Implementation details

* `Workflow` a builder pattern API to define the individual steps of a workflow,
* `JobQueueEngine` - interface that allows adapters for multiple queueing engines to be implemented,
* `JobQueueWorkflowRunner` - runs/executes workflows using a `JobQueueEngine`, usually in a worker process,
* `JobQueueWorkflowDispatcher` - dispatches/submits workflows to be run using a `JobQueueEngine`, usually on the app server,
* `BullMqAdapter` - an implementation of `JobQueueEngine` for **BullMq**.

## License

MIT © Blunge
