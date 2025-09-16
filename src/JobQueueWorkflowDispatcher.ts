import { v4 as uuidv4 } from 'uuid';
import { Workflow, findWorkflow } from './Workflow';
import { makeWorkflowJobData } from './WorkflowJob';
import type { JobResultStatus } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { Logger, assert } from './utils';
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
  constructor(public readonly config: Config<Names, Wfs, Qs>) {}

  async dispatch<N extends string, Input, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, any, N>, Names>,
    input: Input,
    opts?: { jobId?: string, meta?: Meta },
  ) {
    // ensure the correct workflow/version was passed to the constructor
    findWorkflow(this.config.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = (this.config.queues as unknown as Record<string, string>)[props.name];
    assert(queue, 'queue not found');

    const workflowInput = makeWorkflowJobData({ props, input });
    return await this.config.engine.submitJob({
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
    findWorkflow(this.config.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = (this.config.queues as unknown as Record<string, string>)[props.name];
    assert(queue, 'queue not found');

    const workflowInput = makeWorkflowJobData({ props, input });

    let submitPromise: Promise<unknown> | undefined;
    const resultStatusPromise = new Promise<JobResultStatus<unknown>>((resolve) => {
      submitPromise = this.config.engine.submitJob({
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
      this.config.logger.error({ resulType: resultStatus.type, queue }, 'run: job unsuccessful');
      throw Error('job unsuccessful');
    }
    const result = await this.config.engine.getJobResult({ resultKey: resultStatus.resultKey, delete: true });
    assert(result.type === 'success');
    return result.output as Output;
  }
}
