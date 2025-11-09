import type { Workflow } from '../Workflow';
import type { MatchingWorkflow, NamesOfWfs, WfArray } from '../typeHelpers';
import { CloudflareWorkflowClient, type CloudflareWorkflowClientOptions, type InstanceDetails } from './CloudflareWorkflowClient';
import type { DispatchOptions } from '../WorkflowDispatcher';
import type { Logger } from '../utils';

export type CloudflareWorkflowRunnerOptions<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> = CloudflareWorkflowClientOptions<Names, Wfs>;

export class CloudflareWorkflowRunner<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> {
  private readonly client: CloudflareWorkflowClient<Names, Wfs>;
  private readonly logger: Logger;

  constructor(opts: CloudflareWorkflowRunnerOptions<Names, Wfs>) {
    this.client = new CloudflareWorkflowClient(opts);
    this.logger = this.client.logger;
  }

  async start<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<{ instanceId: string, status: string }> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const workflow = props as Workflow<Input, Output, N, No, Co>;
    const cfWorkflow = workflow as unknown as Workflow<any, any>;
    const params = this.client.prepareParams(cfWorkflow, input, opts?.meta);
    const result = await this.client.createInstance(cfWorkflow, params, { jobId: opts?.jobId });
    this.logger.info({ workflow: workflow.name, instanceId: result.id, status: result.status }, 'cloudflare: workflow started');
    return { instanceId: result.id, status: result.status };
  }

  async run<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<{ instanceId: string, output: Output }> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const workflow = props as Workflow<Input, Output, N, No, Co>;
    const cfWorkflow = workflow as unknown as Workflow<any, any>;
    const params = this.client.prepareParams(cfWorkflow, input, opts?.meta);
    const creation = await this.client.createInstance(cfWorkflow, params, { jobId: opts?.jobId });
    this.logger.info({ workflow: workflow.name, instanceId: creation.id, status: creation.status }, 'cloudflare: workflow started');
    try {
      const output = await this.client.waitForCompletion<Output>(cfWorkflow, creation.id);
      this.logger.info({ workflow: workflow.name, instanceId: creation.id }, 'cloudflare: workflow completed');
      return { instanceId: creation.id, output };
    } catch (err) {
      this.logger.error({ workflow: workflow.name, instanceId: creation.id, err }, 'cloudflare: workflow failed');
      throw err;
    }
  }

  async waitForCompletion<N extends string, Input, Output, No, Co>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    instanceId: string,
  ): Promise<Output> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const cfWorkflow = props as unknown as Workflow<any, any>;
    return await this.client.waitForCompletion<Output>(cfWorkflow, instanceId);
  }

  async getInstanceDetails<N extends string, Input, Output, No, Co>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    instanceId: string,
  ): Promise<InstanceDetails> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const cfWorkflow = props as unknown as Workflow<any, any>;
    return await this.client.fetchInstanceDetails(cfWorkflow, instanceId);
  }

  async sendEvent<N extends string, Input, Output, No, Co>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    instanceId: string,
    eventType: string,
    payload?: unknown,
  ): Promise<void> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const cfWorkflow = props as unknown as Workflow<any, any>;
    await this.client.sendEvent(cfWorkflow, instanceId, eventType, payload);
    const workflow = props as Workflow<Input, Output, N, No, Co>;
    this.logger.info({ workflow: workflow.name, instanceId, eventType }, 'cloudflare: event sent');
  }
}
