import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { workflow } from '~/index';
import {
  CloudflareWorkflowRunner,
  type CloudflareWorkflowEvent,
  type CloudflareWorkflowStep,
  type CloudflareWorkflowRunnerOptions,
} from '~/cloudflare/CloudflareWorkflowRunner';
import { CloudflareWorkflowDispatcher } from '~/cloudflare/CloudflareWorkflowDispatcher';

const toRunnerWorkflow = (
  wf: unknown,
): CloudflareWorkflowRunnerOptions['workflows'][number] => wf as CloudflareWorkflowRunnerOptions['workflows'][number];

const createDispatcher = (workflows: CloudflareWorkflowRunnerOptions['workflows']) => {
  return new CloudflareWorkflowDispatcher<any, any>({
    accountId: 'acct-1',
    apiToken: 'token-1',
    workflows: workflows as any,
    fetchImpl: vi.fn(),
    pollIntervalMs: 0,
  });
};

class StepStub implements CloudflareWorkflowStep {
  public readonly calls: string[] = [];

  async do<T>(name: string, ...args: [() => Promise<T> | T] | [unknown, () => Promise<T> | T]): Promise<T> {
    const callback = (args.length === 1 ? args[0] : args[1]);
    if (typeof callback !== 'function') {
      throw new Error('step callback missing');
    }
    this.calls.push(name);
    return await Promise.resolve((callback as () => Promise<T> | T)());
  }
}

describe('CloudflareWorkflowRunner', () => {
  let step: StepStub;
  const eventBase = (): CloudflareWorkflowEvent<any> => ({ payload: undefined, timestamp: new Date(), instanceId: 'inst-1' });

  beforeEach(() => {
    step = new StepStub();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('runs sequential steps via step.do', async () => {
    const adder = workflow({ name: 'adder', version: 1 })
      .step(async ({ a, b }: { a: number; b: number }) => ({ sum: a + b }))
      .step(async ({ sum }) => ({ sum: sum + 1 }));

    const workflows = [toRunnerWorkflow(adder)];
    const dispatcher = createDispatcher(workflows);
    const runner = new CloudflareWorkflowRunner({ workflows, dispatcher });
    const dispatchSpy = vi.spyOn(dispatcher, 'dispatchAwaitingOutput');

    const event: CloudflareWorkflowEvent<{ a: number; b: number }> = {
      ...eventBase(),
      payload: { a: 2, b: 3 },
    };

    const result = await runner.run(adder, event, step);

    expect(result).toEqual({ sum: 6 });
    expect(step.calls).toEqual([
      'adder-v1.step.001',
      'adder-v1.step.002',
    ]);
    expect(dispatchSpy).not.toHaveBeenCalled();
  });

  test('handles restart and complete wrappers', async () => {
    const restartable = workflow({ name: 'restartable', version: 1 })
      .step(async ({ count }: { count: number }, { restart }) => {
        if (count < 2) {
          return restart({ count: count + 1 });
        }
        return { count };
      })
      .step(async ({ count }, { complete }) => complete(count));

    const workflows = [toRunnerWorkflow(restartable)];
    const dispatcher = createDispatcher(workflows);
    const runner = new CloudflareWorkflowRunner({ workflows, dispatcher });
    const dispatchSpy = vi.spyOn(dispatcher, 'dispatchAwaitingOutput');

    const event: CloudflareWorkflowEvent<{ count: number }> = {
      ...eventBase(),
      payload: { count: 0 },
    };

    const result = await runner.run(restartable as any, event, step);

    expect(result).toEqual(2);
    expect(step.calls).toEqual([
      'restartable-v1.step.001',
      'restartable-v1.step.001',
      'restartable-v1.step.001',
      'restartable-v1.step.002',
    ]);
    expect(dispatchSpy).not.toHaveBeenCalled();
  });

  test('runs child workflows by dispatching with prefixed step names', async () => {
    const child = workflow({ name: 'double', version: 3 })
      .step(async (value: number) => value * 2);

    const parent = workflow({ name: 'parent', version: 1 })
      .step(async ({ value }: { value: number }) => value)
      .childStep(child)
      .step(async (value: number) => ({ result: value }));

    const workflows = [toRunnerWorkflow(parent), toRunnerWorkflow(child)];
    const dispatcher = createDispatcher(workflows);
    const dispatchSpy = vi.spyOn(dispatcher, 'dispatchAwaitingOutput');
    dispatchSpy.mockImplementation(async (wf, input) => {
      expect((wf as any).name).toBe('double');
      expect(input).toBe(4);
      return 8;
    });

    const runner = new CloudflareWorkflowRunner({ workflows, dispatcher });

    const event: CloudflareWorkflowEvent<{ value: number }> = {
      ...eventBase(),
      payload: { value: 4 },
    };

    const result = await runner.run(parent as any, event as any, step);

    expect(result).toEqual({ result: 8 });
    expect(dispatchSpy).toHaveBeenCalledTimes(1);
    expect(dispatchSpy).toHaveBeenCalledWith(child as any, 4);
    expect(step.calls).toEqual([
      'parent-v1.step.001',
      'parent-v1.double-v3.dispatch',
      'parent-v1.step.003',
    ]);
  });
});
