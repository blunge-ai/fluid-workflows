# fluid-workflows

Type-safe durable workflows with fluid builder syntax and optional job-queue execution with adapters.

The main problem this project attempts to solve is the separation of workflow definition from execution. Execution can involve persisting intermediary steps through a storage adapter (e.g. redis, or cloudflare durable objects) or using queues with a queueing adapater (e.g. BullMq, or cloudflare queues).

## Install

```bash
npm install @blunge/fluid-workflows bullmq ioredis zod
```

## Quick Start

This defines a simple workflow with a single step that adds two numbers together.

```ts
import fwf from '@blunge/fluid-workflows';

const wf = fwf.create({ name: 'add', version: 1 })
  .step(async ({ a, b }: { a: number; b: number }) => ({ sum: a + b }));

// Run the workflow directly (uses in-memory storage by default)
const out = await wf.run({ a: 2, b: 3 });
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
import fwf from '@blunge/fluid-workflows';

// Workflow definitions

const child = fwf.create({ name: 'child', version: 1 })
  .step(async ({ s }: { s: string }) => ({ s2: `child(${s})` }));

const parent = fwf.create({ name: 'parent', version: 1 })
  .step(async ({ n }: { n: number }) => ({ s: `n=${n}` }))
  .step(child)
  .step(async ({ s2 }) => ({ out: s2 }));

// Configuration

const { worker, dispatcher } = fwf.jobQueueConfig({
  engine: new fwf.BullMqAdapter({ attempts: 1, lockTimeoutMs: 8000 }),
  workflows: [parent],
  queues: { parent: 'parent-queue', child: 'child-queue' },
  jobTimeoutMs: 60000, // Maximum time a job is expected to run (also used for state TTL)
});

// On a worker machine, run parent workflows
const stopParent = worker.run(['parent-queue']);

// Possibly on a separate machine, run child workflows
const stopChild = worker.run(['child-queue']);

// Alternatively, run all workflows on the same machine
// const stopAll = worker.run('all');

// On the app server, dispatch workflows and optionally await the output
const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
console.log(result.out); // 'child(n=5)'

await stopParent();
await stopChild();
```

## Advanced features

### Parallel execution

Use `.parallel()` to run multiple child workflows in parallel. Each child receives the full 
accumulated state, and their outputs are merged into the result with the specified keys.

```ts
const child1 = fwf.create({ name: 'child1', version: 1 })
  .step(async ({ n }: { n: number }) => `child1(${n})`);

const child2 = fwf.create({ name: 'child2', version: 1 })
  .step(async ({ n }: { n: number }) => `child2(${n})`);

const parent = fwf.create({ name: 'parent', version: 1 })
  .step(async ({ n }: { n: number }) => ({ n2: n * 2 }))
  .parallel({ c1: child1, c2: child2 })
  .step(async ({ n, n2, c1, c2 }) => ({ 
    out: `${c1}, ${c2}` // 'child1(5), child2(5)'
  }));
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
const wf = fwf.workflow({ name: 'iterate', version: 1, inputSchema })
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

const out = await wf.run({ iterations: 0 });
console.log(out.iterations); // 10
```

### update

The `update` function allows the step's input data to be updated, such that if the step does not run
to completion, it will be retried with the updated input data. Additional information, such as the
current workflow progress, can be reported to job status listeners by passing an `info` property in
the options object.

```ts
const parent = fwf.workflow({ name: 'parent', version: 1, inputSchema })
  .step(async ({ iterations, maxIterations }, { update }) => ({
     for (; iterations > 0; iterations--) {

       // by convention we pass a value from 0 to 1 to report the current progress
       const progress = Math.min(1, iterations / maxIterations);

       // calling update here updates the input data and reports the current progress
       const { interrupt } = await update({ input: { iterations }, info: { phase: 'iterating', progress } });

       // or just report progress, without updating the step's input data
       // const { interrupt } = await update({ info: { phase: 'iterating', progress } });

        if (interrupt) {
          throw Error('interrupted');
        }
      }
   }));
```

### subscribe

The `subscribe` method allows job status listeners to receive real-time updates when a job's state changes (e.g., progress updates from `update()` calls or job completion).

```ts
const unsubscribe = await wf.subscribe(jobId, (status) => {
  console.log('Job status update:', status);
});

// Later, stop listening
unsubscribe();
```

Note: This example uses in-memory storage by default. To use Redis pub/sub for status updates across processes, pass a `RedisStorage` instance when creating the workflow.

## License

MIT © Blunge
