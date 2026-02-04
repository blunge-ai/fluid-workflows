import { v4 as uuidv4 } from 'uuid';
import { Workflow, findWorkflow } from '../WfBuilder';
import { makeWorkflowJobData } from '../types';
import type { JobResultStatus } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { Logger, assert } from '../utils';
import { WfArray, NamesOfWfs, MatchingWorkflow } from '../typeHelpers';
import { JobQueueConfig } from './JobQueueConfig';
import type { WfDispatcher, DispatchOptions, WfMeta } from '../types';

export type Opts = {
  logger: Logger,
};

export interface JobQueueWorkflowDispatcherInterface<
  Wfs extends WfArray<string>,
  Qs extends Record<NamesOfWfs<Wfs>, string>
> extends WfDispatcher<Wfs> {
  readonly config: JobQueueConfig<Wfs, Qs>;
}

export class JobQueueWorkflowDispatcher<
  const Wfs extends WfArray<string>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
> implements JobQueueWorkflowDispatcherInterface<Wfs, Qs> {
  constructor(public readonly config: JobQueueConfig<Wfs, Qs>) {}

  async dispatch<N extends string, Input, Output, No, Co, Meta extends WfMeta = WfMeta>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, NamesOfWfs<Wfs>, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ) {
    // ensure the correct workflow/version was passed to the constructor
    findWorkflow(this.config.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = this.config.queueFor(props.name);

    const workflowInput = makeWorkflowJobData({ props, input });
    return await this.config.engine.submitJob({
      data: { id: jobId, input: workflowInput, meta: opts?.meta },
      queue,
    });
  }
  
  async dispatchAwaitingOutput<const N extends string, Input, Output, No, Co, Meta extends WfMeta = WfMeta>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, NamesOfWfs<Wfs>, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ) {
    // ensure the correct workflow/version was passed to the constructor
    findWorkflow(this.config.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = this.config.queueFor(props.name);

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
