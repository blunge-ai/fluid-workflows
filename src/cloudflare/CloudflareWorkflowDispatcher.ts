import type { Workflow } from '../Workflow';
import type { WorkflowDispatcher, DispatchOptions } from '../WorkflowDispatcher';
import type { MatchingWorkflow, NamesOfWfs, WfArray } from '../typeHelpers';
import { CloudflareWorkflowClient, type CloudflareWorkflowClientOptions } from './CloudflareWorkflowClient';
import type { Logger } from '../utils';

export type CloudflareWorkflowDispatcherOptions<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> = CloudflareWorkflowClientOptions<Names, Wfs>;

export class CloudflareWorkflowDispatcher<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> implements WorkflowDispatcher<Names, Wfs> {
  private readonly client: CloudflareWorkflowClient<Names, Wfs>;
  private readonly logger: Logger;

  constructor(opts: CloudflareWorkflowDispatcherOptions<Names, Wfs>) {
    this.client = new CloudflareWorkflowClient(opts);
    this.logger = this.client.logger;
  }

  async dispatch<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<void> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const workflow = props as Workflow<Input, Output, N, No, Co>;
    const cfWorkflow = workflow as unknown as Workflow<any, any>;
    const params = this.client.prepareParams(cfWorkflow, input, opts?.meta);
    const creation = await this.client.createInstance(cfWorkflow, params, { jobId: opts?.jobId });
    this.logger.info({ workflow: workflow.name, instanceId: creation.id, status: creation.status }, 'cloudflare: workflow dispatched');
  }

  async dispatchAwaitingOutput<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<Output> {
    this.client.ensureWorkflowRegistered({ name: props.name, version: props.version });
    const workflow = props as Workflow<Input, Output, N, No, Co>;
    const cfWorkflow = workflow as unknown as Workflow<any, any>;
    const params = this.client.prepareParams(cfWorkflow, input, opts?.meta);
    const creation = await this.client.createInstance(cfWorkflow, params, { jobId: opts?.jobId });
    this.logger.info({ workflow: workflow.name, instanceId: creation.id, status: creation.status }, 'cloudflare: workflow dispatched');
    try {
      const output = await this.client.waitForCompletion<Output>(cfWorkflow, creation.id);
      this.logger.info({ workflow: workflow.name, instanceId: creation.id }, 'cloudflare: workflow completed');
      return output;
    } catch (err) {
      this.logger.error({ workflow: workflow.name, instanceId: creation.id, err }, 'cloudflare: workflow failed');
      throw err;
    }
  }
}
