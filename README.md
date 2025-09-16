# fluid-workflows

Type-safe workflow composition and job-queue execution with adapters.

The main problem this project attempts to solve is the separation of the workflow implementation and
execution.

The implementation part is provided by the `Workflow` class, which provides a DSL or builder
pattern to define the individual steps of a workflow.

The execution part is currently provided by either the `runQueueless()` function, which is a simple
in-process loop that calls the individual step functions, or the `JobQueue*` classes which provide a
way to run workflows in different processes by scheduling jobs via a queue engine.

* `JobQueueEngine` - interface that allows adapters for multiple queueing engines to be implemented,
* `JobQueueWorkflowRunner` - runs/executes workflows using a `JobQueueEngine`, usually in a worker process,
* `JobQueueWorkflowDispatcher` - dispatches/submits workflows to be run using a `JobQueueEngine`, usually on the app server,
* `BullMqAdapter` - an implementation of `JobQueueEngine` for **BullMq**.

## Install

```bash
npm install @blunge/fluid-workflows
```

## Quick Start (queueless for testing)

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

For example to run tests:

```
export REDIS_URL=redis://127.0.0.1:6379 npm run test
```

## Using job queues

```ts
import * as fwf from '@blunge/fluid-workflows';

// Workflows definitions

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

// Alternatively, run all on the same machine
// const stop = runner.run('all');

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

```
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

```
const schema = z.object({ a: z.number(), b: z.number() });

const parent = fwf.workflow({ name: 'parent', version: 1, schema })
  .step(async ({ a, b }) => ({ 
     ...
  }));
```

## License

MIT © blunge-ai
