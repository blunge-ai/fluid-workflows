import { Workflow, collectWorkflows, findWorkflow, type WorkflowProps } from '../Workflow';
import type { NamesOfWfs, WfArray } from '../typeHelpers';
import { defaultLogger, Logger, timeout } from '../utils';

export type InstanceStatus =
  | 'queued'
  | 'running'
  | 'paused'
  | 'errored'
  | 'terminated'
  | 'complete'
  | 'waiting'
  | 'waitingForPause'
  | 'unknown';

export type InstanceDetails = {
  id: string,
  status: InstanceStatus,
  success?: boolean,
  output?: unknown,
  error?: { message?: string, name?: string },
};

export type CloudflareParamsTransformer = (args: {
  workflow: Workflow<any, any>,
  input: unknown,
  meta: unknown,
}) => unknown;

export type CloudflareWorkflowClientOptions<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> = {
  accountId: string,
  apiToken: string,
  workflows: Wfs,
  workflowNameMap?: Partial<Record<Names, string>>,
  baseUrl?: string,
  pollIntervalMs?: number,
  maxWaitTimeMs?: number,
  logger?: Logger,
  fetchImpl?: typeof fetch,
  toParams?: CloudflareParamsTransformer,
};

type CloudflareError = {
  code?: number,
  message?: string,
};

type CloudflareEnvelope<T> = {
  success: boolean,
  errors?: CloudflareError[],
  messages?: unknown,
  result: T,
};

type InstanceCreateResult = {
  id: string,
  status: string,
  version_id: string,
  workflow_id: string,
};

function ensureFetch(fetchImpl?: typeof fetch): typeof fetch {
  const impl = fetchImpl ?? globalThis.fetch;
  if (!impl) {
    throw new Error('fetch is not available in this environment');
  }
  return impl.bind(globalThis);
}

function trimTrailingSlash(url: string) {
  return url.replace(/\/+$/, '');
}

export class CloudflareWorkflowClient<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> {
  public readonly accountId: string;
  public readonly apiToken: string;
  public readonly workflows: Wfs;
  protected readonly allWorkflows: Workflow<unknown, unknown>[];
  protected readonly workflowNameMap: Record<string, string>;
  public readonly baseUrl: string;
  public readonly pollIntervalMs: number;
  public readonly maxWaitTimeMs: number;
  protected readonly fetchImpl: typeof fetch;
  public readonly logger: Logger;
  protected readonly toParams: CloudflareParamsTransformer;

  constructor(opts: CloudflareWorkflowClientOptions<Names, Wfs>) {
    this.accountId = opts.accountId;
    this.apiToken = opts.apiToken;
    this.workflows = opts.workflows;
    this.allWorkflows = collectWorkflows(this.workflows as unknown as Workflow<unknown, unknown>[]);
    this.workflowNameMap = {};
    for (const workflow of this.allWorkflows) {
      this.workflowNameMap[workflow.name] = opts.workflowNameMap?.[workflow.name as Names] ?? workflow.name;
    }
    this.baseUrl = trimTrailingSlash(opts.baseUrl ?? 'https://api.cloudflare.com/client/v4');
    this.pollIntervalMs = Math.max(0, opts.pollIntervalMs ?? 2000);
    this.maxWaitTimeMs = Math.max(this.pollIntervalMs, opts.maxWaitTimeMs ?? 10 * 60 * 1000);
    this.fetchImpl = ensureFetch(opts.fetchImpl);
    this.logger = opts.logger ?? defaultLogger;
    this.toParams = opts.toParams ?? (({ input }) => input);
  }

  public ensureWorkflowRegistered(props: Pick<WorkflowProps, 'name' | 'version'>): void {
    findWorkflow(this.allWorkflows, props);
  }

  public prepareParams(workflow: Workflow<any, any>, input: unknown, meta: unknown) {
    const validatedInput = workflow.inputSchema ? workflow.inputSchema.parse(input) : input;
    return this.toParams({ workflow, input: validatedInput, meta });
  }

  public async createInstance(
    workflow: Workflow<any, any>,
    params: unknown,
    opts?: { jobId?: string }
  ): Promise<InstanceCreateResult> {
    const body: Record<string, unknown> = {};
    if (opts?.jobId) {
      body.id = opts.jobId;
    }
    if (params !== undefined) {
      body.params = params;
    }
    const workflowName = this.remoteNameFor(workflow);
    const path = this.instancesPath(workflowName);
    return await this.apiRequest<InstanceCreateResult>('POST', path, body);
  }

  public async waitForCompletion<Output>(workflow: Workflow<any, any>, instanceId: string): Promise<Output> {
    const workflowName = this.remoteNameFor(workflow);
    const path = `${this.instancesPath(workflowName)}/${encodeURIComponent(instanceId)}`;
    const start = Date.now();
    while (true) {
      const details = await this.apiRequest<InstanceDetails>('GET', path);
      const status = details.status;
      if (status === 'complete') {
        if (details.success === false) {
          const reason = details.error?.message ?? `Cloudflare workflow instance ${instanceId} completed without success`;
          throw new Error(reason);
        }
        return details.output as Output;
      }
      if (status === 'errored' || status === 'terminated') {
        const reason = details.error?.message ?? `Cloudflare workflow instance ${instanceId} ended with status ${status}`;
        throw new Error(reason);
      }
      if (Date.now() - start >= this.maxWaitTimeMs) {
        throw new Error(`timed out waiting for Cloudflare workflow instance ${instanceId} to complete`);
      }
      await timeout(this.pollIntervalMs);
    }
  }

  public async fetchInstanceDetails(workflow: Workflow<any, any>, instanceId: string) {
    const workflowName = this.remoteNameFor(workflow);
    const path = `${this.instancesPath(workflowName)}/${encodeURIComponent(instanceId)}`;
    return await this.apiRequest<InstanceDetails>('GET', path);
  }

  public async sendEvent(
    workflow: Workflow<any, any>,
    instanceId: string,
    eventType: string,
    payload?: unknown,
  ): Promise<void> {
    const workflowName = this.remoteNameFor(workflow);
    const path = `${this.instancesPath(workflowName)}/${encodeURIComponent(instanceId)}/events/${encodeURIComponent(eventType)}`;
    await this.apiRequest('POST', path, payload === undefined ? {} : { payload });
  }

  protected remoteNameFor(workflow: Workflow<any, any>) {
    const remoteName = this.workflowNameMap[workflow.name];
    if (!remoteName) {
      throw new Error(`no Cloudflare workflow mapping configured for '${workflow.name}'`);
    }
    return remoteName;
  }

  protected instancesPath(workflowName: string) {
    return `/accounts/${encodeURIComponent(this.accountId)}/workflows/${encodeURIComponent(workflowName)}/instances`;
  }

  public async apiRequest<T>(method: 'GET' | 'POST' | 'PATCH', path: string, body?: unknown): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      'Authorization': `Bearer ${this.apiToken}`,
      'Content-Type': 'application/json',
    };
    const response = await this.fetchImpl(url, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });

    const text = await response.text();
    let payload: CloudflareEnvelope<T> | undefined;
    if (text) {
      try {
        payload = JSON.parse(text) as CloudflareEnvelope<T>;
      } catch (err) {
        throw new Error(`Cloudflare API response from ${path} is not valid JSON`);
      }
    }
    if (!response.ok) {
      const message = this.errorMessage(path, response.status, payload?.errors);
      throw new Error(message);
    }
    if (!payload) {
      throw new Error(`Cloudflare API response from ${path} is empty`);
    }
    if (!payload.success) {
      throw new Error(this.errorMessage(path, response.status, payload.errors));
    }
    if (payload.result === undefined) {
      throw new Error(`Cloudflare API response from ${path} is missing result`);
    }
    return payload.result;
  }

  private errorMessage(path: string, status: number, errors?: CloudflareError[]) {
    if (!errors || errors.length === 0) {
      return `Cloudflare API request to ${path} failed with status ${status}`;
    }
    const messages = errors.map((error) => {
      const codePart = error.code !== undefined ? `${error.code}: ` : '';
      return `${codePart}${error.message ?? 'Unknown error'}`;
    }).join('; ');
    return `Cloudflare API request to ${path} failed with status ${status}: ${messages}`;
  }
}
