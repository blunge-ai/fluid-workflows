import { v4 as uuidv4 } from 'uuid';
import { Workflow, findWorkflow } from './Workflow';
import { makeWorkflowJobData } from './WorkflowJob';
import type { JobQueueEngine, JobResultStatus } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { Logger, defaultLogger, assert } from './utils';
import { WfArray, NamesOfWfs } from './typeHelpers';
import { Config } from './Config';

export type Opts = {
  logger: Logger,
};

type MatchingWorkflow<Wf, Names extends string>
  = Wf extends Workflow<any, any, infer N> ? (Exclude<N, Names> extends never ? Wf : never) : never;

export class JobQueueWorkflowDispatcher<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
> {
  private opts: Opts;
  private queuesMap: Record<string, string>;
  private allWorkflows: Workflow<any, any, any>[];
  private engine: JobQueueEngine;

  constructor(config: Config<Names, Wfs, Qs>) {
    this.engine = config.engine;
    this.opts = { logger: config.logger ?? defaultLogger };
    this.queuesMap = config.queues as unknown as Record<string, string>;
    this.allWorkflows = config.allWorkflows;
  }

  async dispatch<N extends string, Input, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, any, N>, Names>,
    input: Input,
    opts?: { jobId?: string, meta?: Meta },
  ) {
    // ensure the correct workflow/version was passed to the constructor
    findWorkflow(this.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = this.queuesMap[props.name];
    assert(queue, 'queue not found');

    const workflowInput = makeWorkflowJobData({ props, input });
    return await this.engine.submitJob({
      data: { id: jobId, input: workflowInput, meta: opts?.meta },
      queue,
    });
  }
  
  async dispatchAwaitingOutput<const N extends string, Input, Output, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N>, Names>,
    input: Input,
    opts?: { jobId?: string, meta?: Meta },
  ) {
    // ensure the correct workflow/version was passed to the constructor
    findWorkflow(this.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = this.queuesMap[props.name];
    assert(queue, 'queue not found');

    const workflowInput = makeWorkflowJobData({ props, input });

    let submitPromise: Promise<unknown> | undefined;
    const resultStatusPromise = new Promise<JobResultStatus<unknown>>((resolve) => {
      submitPromise = this.engine.submitJob({
        data: { id: jobId, input: workflowInput, meta: opts?.meta },
        queue,
        statusHandler: (status) => {
          if (isResultStatus(status)) {
            resolve(status);
          }
        }
      });
    });
    await submitPromise;
    const resultStatus = await resultStatusPromise;
    if (resultStatus.type !== 'success') {
      this.opts.logger.error({ resulType: resultStatus.type, queue }, 'run: job unsuccessful');
      throw Error('job unsuccessful');
    }
    const result = await this.engine.getJobResult({ resultKey: resultStatus.resultKey, delete: true });
    assert(result.type === 'success');
    return result.output as Output;
  }
}
