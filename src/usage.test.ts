import { expect, test } from 'vitest';
import * as fwf from '~/index';

test('workflow.run() example', async () => {
  const wf = fwf.create({ name: 'add', version: 1 })
    .step(async ({ a, b }: { a: number; b: number }) => ({ sum: a + b }));

  const out = await wf.run({ a: 2, b: 3 });
  expect(out.sum).toBe(5);
});

test('job queues example', async () => {

  const child = fwf.create({ name: 'child', version: 1 })
    .step(async ({ s }: { s: string }) => ({ s2: `child(${s})` }));

  const parent = fwf.create({ name: 'parent', version: 1 })
    .step(async ({ n }: { n: number }) => ({ s: `n=${n}` }))
    .step(child)
    .step(async ({ s2 }) => ({ out: s2 }));

  const { worker, dispatcher } = fwf.jobQueueConfig({
    engine: new fwf.BullMqAdapter({
      attempts: 1,
      lockTimeoutMs: 8000,
      blockingTimeoutSecs: 0.1,
    }),
    workflows: [parent],
    queues: { parent: 'parent-queue', child: 'child-queue' },
  });

  const stop = worker.run('all');

  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await stop();

  expect(result.out).toBe('child(n=5)');
});
