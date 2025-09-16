# fluid-workflows

Type-safe workflow composition and job-queue execution with adapters.

The main problem this project attempts to solve is the separation of the workflow implementation and
execution.

The implementation part is provided by the Workflow class, which provides a DSL or builder
pattern to define the individual steps of a workflow.

The execution part is currently provided by either the `runQueueless()` function, which is a simple
in-process loop that calls the individual step functions, or the `JobQueue*` classes which provide a
way to run workflows in different processes by scheduling jobs via a queue engine.

* `JobQueueEngine` - interface that allows adapters for multiple queueing engines to be implemented,
* `JobQueueWorkflowRunner` - runs/executes workflows using a `JobQueueEngine`, usually in a worker process,
* `JobQueueWorkflowDispatcher` - dispatches/submits workflows to be run using a `JobQueueEngine`,
* `BullMqAdapter` - an implementation of `JobQueueEngine` for **BullMq**.

## Install

```bash
npm install @blunge/fluid-workflows
```

## Quick Start (Queueless for testing)

```ts
import * as fwf from '@blunge/fluid-workflows';

const wf = fwf.workflow({ name: 'add', version: 1 })
  .step(async ({ a, b }: { a: number; b: number }) => ({ sum: a + b }));

// Runs the workflow in the current process to completion, useful for testing
const out = await runQueueless(wf, { a: 2, b: 3 });
console.log(out.sum); // 5
```

## BullMQ (in production)

```ts
import * as fwf from '@blunge/fluid-workflows';

// Define workflows
const child = fwf.workflow({ name: 'child', version: 1 })
  .step(async ({ s }: { s: string }) => ({ s2: `child(${s})` }));

const parent = fwf.workflow({ name: 'parent', version: 1 })
  .step(async ({ n }: { n: number }) => ({ childInput: { s: `n=${n}` } }))
  .childStep({ childInput: child })
  .step(async ({ childInput }) => ({ out: childInput.s2 }));

// Engine and wiring

const { runner, dispatcher } = fwf.config({
  engine: new BullMqAdapter({ attempts: 1, lockTimeoutMs: 8000 }),
  workflows: [parent],
  queues: { parent: 'parent-queue, child: 'child-queue' } as const,
});

// On a worker machine, run parent workflows
const stopParent = runner.run(['parent-queue']);

// Possibly on a separate machine, run child workflows
const stopChild = runner.run([child-queue']);

// Alternatively, run all on the same machine
// const stop = runner.run('all');

// On the app server, dispatch workflows and optionally await the output
const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
console.log(result.out);

await stopParent();
await stopChild();
```

## License

MIT © blunge-ai
