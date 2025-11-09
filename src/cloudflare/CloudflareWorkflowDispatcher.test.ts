import { beforeEach, describe, expect, test, vi } from 'vitest';
import { workflow } from '~/index';
import { CloudflareWorkflowDispatcher } from '~/cloudflare/CloudflareWorkflowDispatcher';
import type { CloudflareWorkflowDispatcherOptions } from '~/cloudflare/CloudflareWorkflowDispatcher';

const logger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

describe('CloudflareWorkflowDispatcher', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });
  const wf = workflow({ name: 'remote-workflow', version: 1 })
    .step(async (input: { foo: string }) => ({ foo: input.foo }));

  function createDispatcher(fetchMock: ReturnType<typeof vi.fn>, extra?: Partial<CloudflareWorkflowDispatcherOptions<'remote-workflow', [typeof wf]>>) {
    return new CloudflareWorkflowDispatcher<'remote-workflow', [typeof wf]>({
      accountId: 'acct-1',
      apiToken: 'token-1',
      workflows: [wf],
      logger,
      fetchImpl: fetchMock as unknown as typeof fetch,
      pollIntervalMs: 0,
      ...extra,
    });
  }

  test('dispatch issues create instance request with input payload', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-1',
          status: 'queued',
          version_id: 'v1',
          workflow_id: 'wf1',
        },
      }))
    );
    const dispatcher = createDispatcher(fetchMock);

    await dispatcher.dispatch(wf, { foo: 'bar' });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('https://api.cloudflare.com/client/v4/accounts/acct-1/workflows/remote-workflow/instances');
    expect(init?.method).toBe('POST');
    expect(init?.headers).toMatchObject({
      Authorization: 'Bearer token-1',
      'Content-Type': 'application/json',
    });
    expect(init?.body).toBe(JSON.stringify({ params: { foo: 'bar' } }));
  });

  test('dispatchAwaitingOutput polls until completion and returns output', async () => {
    const fetchMock = vi.fn();
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-42',
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
          id: 'inst-42',
          status: 'running',
        },
      }))
    );
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-42',
          status: 'complete',
          success: true,
          output: { foo: 'done' },
        },
      }))
    );

    const dispatcher = createDispatcher(fetchMock);

    const result = await dispatcher.dispatchAwaitingOutput(wf, { foo: 'bar' });

    expect(result).toEqual({ foo: 'done' });
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  test('toParams customiser receives meta and input', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({
        success: true,
        result: {
          id: 'inst-100',
          status: 'queued',
          version_id: 'v1',
          workflow_id: 'wf1',
        },
      }))
    );
    const dispatcher = createDispatcher(fetchMock, {
      toParams: ({ input, meta }) => ({ data: input, meta }),
    });

    await dispatcher.dispatch(wf, { foo: 'baz' }, { meta: { requestId: 'r-1' } });

    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(init?.body).toBe(JSON.stringify({ params: { data: { foo: 'baz' }, meta: { requestId: 'r-1' } } }));
  });
});
