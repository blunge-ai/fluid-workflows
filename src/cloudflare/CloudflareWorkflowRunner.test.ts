import { beforeEach, describe, expect, test, vi } from 'vitest';
import { workflow } from '~/index';
import { CloudflareWorkflowRunner } from '~/cloudflare/CloudflareWorkflowRunner';
import type { CloudflareWorkflowRunnerOptions } from '~/cloudflare/CloudflareWorkflowRunner';

const logger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

describe('CloudflareWorkflowRunner', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const wf = workflow({ name: 'remote-workflow', version: 1 })
    .step(async (input: { foo: string }) => ({ foo: input.foo }));

  function createRunner(fetchMock: ReturnType<typeof vi.fn>, extra?: Partial<CloudflareWorkflowRunnerOptions<'remote-workflow', [typeof wf]>>) {
    return new CloudflareWorkflowRunner<'remote-workflow', [typeof wf]>({
      accountId: 'acct-1',
      apiToken: 'token-1',
      workflows: [wf],
      logger,
      pollIntervalMs: 0,
      fetchImpl: fetchMock as unknown as typeof fetch,
      ...extra,
    });
  }

  test('run dispatches and waits for completion', async () => {
    const fetchMock = vi.fn();
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-55',
          status: 'queued',
          version_id: 'v1',
          workflow_id: 'wf1',
        },
      }))
    );
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-55',
          status: 'running',
        },
      }))
    );
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-55',
          status: 'complete',
          success: true,
          output: { foo: 'done' },
        },
      }))
    );

    const runner = createRunner(fetchMock);

    const { instanceId, output } = await runner.run(wf, { foo: 'bar' });

    expect(instanceId).toBe('inst-55');
    expect(output).toEqual({ foo: 'done' });
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  test('waitForCompletion polls existing instance', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-99',
          status: 'complete',
          success: true,
          output: { foo: 'finished' },
        },
      }))
    );

    const runner = createRunner(fetchMock);

    const output = await runner.waitForCompletion(wf, 'inst-99');
    expect(output).toEqual({ foo: 'finished' });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url] = fetchMock.mock.calls[0] as [string];
    expect(url).toBe('https://api.cloudflare.com/client/v4/accounts/acct-1/workflows/remote-workflow/instances/inst-99');
  });

  test('sendEvent posts payload to events endpoint', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({
        success: true,
        result: {},
      }))
    );

    const runner = createRunner(fetchMock);

    await runner.sendEvent(wf, 'inst-1', 'my-event', { value: 42 });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('https://api.cloudflare.com/client/v4/accounts/acct-1/workflows/remote-workflow/instances/inst-1/events/my-event');
    expect(init?.method).toBe('POST');
    expect(init?.body).toBe(JSON.stringify({ payload: { value: 42 } }));
  });
});
