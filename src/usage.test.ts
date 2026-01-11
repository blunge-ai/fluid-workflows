import { expect, test } from 'vitest';
import * as fwf from '~/index';

test('runQueueless example', async () => {
  const wf = fwf.workflow({ name: 'add', version: 1 })
    .step(async ({ a, b }: { a: number; b: number }) => ({ sum: a + b }));

  const out = await fwf.runQueueless(wf, { a: 2, b: 3 });
  expect(out.sum).toBe(5);
});

test('job queues example', async () => {

  const child = fwf.workflow({ name: 'child', version: 1 })
    .step(async ({ s }: { s: string }) => ({ s2: `child(${s})` }));

  const parent = fwf.workflow({ name: 'parent', version: 1 })
    .step(async ({ n }: { n: number }) => ({ s: `n=${n}` }))
    .step(child)
    .step(async ({ s2 }) => ({ out: s2 }));

  const { runner, dispatcher } = fwf.config({
    engine: new fwf.BullMqAdapter({
      attempts: 1,
      lockTimeoutMs: 8000,
      blockingTimeoutSecs: 0.1,
    }),
    workflows: [parent],
    queues: { parent: 'parent-queue', child: 'child-queue' },
  });

  const stop = runner.run('all');

  const result = await dispatcher.dispatchAwaitingOutput(parent, { n: 5 });
  await stop();

  expect(result.out).toBe('child(n=5)');
});
