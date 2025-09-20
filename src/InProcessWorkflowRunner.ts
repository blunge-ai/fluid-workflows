import { v4 as uuidv4 } from 'uuid';
import type { WfArray, NamesOfWfs, MatchingWorkflow } from './typeHelpers';
import { Workflow, findWorkflow, isRestartWrapper, isCompleteWrapper, withRestartWrapper, withCompleteWrapper, collectWorkflows } from './Workflow';
import type { JobStatus } from './JobQueueEngine';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';
import { defaultRedisConnection, defaultLogger } from './utils';
import type { Logger } from './utils';
import { pack } from './packer';
import { JobResult } from './JobQueueEngine';
import Redis from 'ioredis';

type RunOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
  statusHandler?: (status: JobStatus<Meta, WorkflowProgressInfo>) => void
};

export class InProcessWorkflowRunner<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
> {
  private readonly redis;
  private readonly allWorkflows: Workflow<unknown, unknown>[];
  private readonly logger: Logger;  

  constructor(
    opts: {
      workflows: Wfs;
      logger?: Logger,
      redisConnection?: () => Redis,
    }
  ) {
    this.redis = (opts.redisConnection ?? defaultRedisConnection)(); 
    this.logger = opts.logger ?? defaultLogger;
    this.allWorkflows = collectWorkflows(opts.workflows as unknown as Workflow<unknown, unknown>[]);
  }

  private async publishStatus<Meta>(jobId: string, status: JobStatus<Meta, WorkflowProgressInfo>, opts?: RunOptions<Meta>) {
    opts?.statusHandler?.(status);
    await this.redis.publish(`jobs:status:${jobId}`, pack(status));
  }

  private async updateState<Meta>(jobId: string, state: WorkflowJobData<unknown>, status?: JobStatus<Meta, WorkflowProgressInfo>, opts?: RunOptions<Meta>) {
    // TODO redis multi
    await this.redis.set(jobId, pack(state));
    if (status) {
      this.publishStatus(jobId, status, opts)
    }
  }

  private async persistResult<Output>(jobId: string, result: JobResult<Output>) {
    await this.redis.set(`jobs:result:${jobId}`, pack(result));
  }

  async run<const N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: RunOptions<Meta>,
  ): Promise<Output> {
    const workflow = findWorkflow(this.allWorkflows, props) as Workflow<Input, Output>;

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;

    let result: unknown = input;
    if (workflow.inputSchema) {
      result = workflow.inputSchema.parse(result);
    }

    const jobData = makeWorkflowJobData<Input>({ props: workflow, input }) as WorkflowJobData<unknown>;
    await this.updateState(jobId, jobData, undefined, opts);

    this.logger.info({ name: props.name, version: props.version, meta: opts?.meta, jobId }, 'starting workflow');

    let currentStep = 0;

    const runOptions = {
      progress: async (progressInfo: WorkflowProgressInfo) => {
        const status = { type: 'active', jobId, meta: opts?.meta as Meta, info: progressInfo } as const;
        await this.publishStatus(jobId, status, opts);
        return { interrupt: false } as const;
      },
      update: async (stepInput: unknown, progressInfo?: WorkflowProgressInfo) => {
        const status = (
          progressInfo
            ? { type: 'active', jobId, meta: opts?.meta as Meta, info: progressInfo } as const
            : undefined
        );
        await this.updateState(jobId, { ...jobData, input: stepInput, currentStep }, status, opts);
        return { interrupt: false } as const;
      },
      restart: withRestartWrapper,
      complete: withCompleteWrapper,
    };

    try {
      while (currentStep < workflow.steps.length) {
        const step = workflow.steps[currentStep];
        if (typeof step !== 'function') {
          throw Error('child workflows not yet implemented');
        }
        const out = await (step as any)(result, runOptions);
        if (isRestartWrapper(out)) {
          result = (out as any).input as Input;
          if (workflow.inputSchema) {
            result = workflow.inputSchema.parse(result);
          }
          currentStep = 0;
          await this.updateState(jobId, { ...jobData, input: result, currentStep }, undefined, opts);
          continue;
        }
        if (isCompleteWrapper(out)) {
          const output = (out as any).output as Output;
          await this.persistResult(jobId, { type: 'success', output });
          await this.publishStatus(jobId, { type: 'success', jobId, meta: opts?.meta as Meta, resultKey: jobId });
          return output;
        }
        result = out;

        currentStep += 1;
        if (currentStep !== workflow.steps.length) {
          await this.updateState(jobId, { ...jobData, input: result, currentStep }, undefined, opts);
        }
      }

      this.logger.info({ name: props.name, version: props.version, meta: opts?.meta, jobId }, 'finished workflow');
      const output = result as Output;
      this.persistResult(jobId, { type: 'success', output })
      await this.publishStatus(jobId, { type: 'success', jobId, meta: opts?.meta as Meta, resultKey: jobId });
      return output;
    } catch (err) {
      const reason = `exception when running workflow: ${String(err)}`;
      this.logger.error({ name: props.name, version: props.version, meta: opts?.meta, jobId, reason, err }, 'workflow error');
      await this.persistResult(jobId, { type: 'error', reason });
      await this.publishStatus(jobId, { type: 'error', jobId, meta: opts?.meta as Meta, reason });
      throw err;
    }
  }
}
