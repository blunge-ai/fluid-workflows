import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowProps } from './Workflow';
import { WorkflowJobData, makeWorkflowJobData } from './WorkflowJob';
import type { JobQueueEngine, JobStatus, JobResultStatus } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { Logger, defaultLogger, assert } from './utils';

export type Opts = {
  logger: Logger,
  queue: string,
};

export type ConstructorOpts
 = Partial<Opts>
 & Pick<Opts, 'queue'>;

export class WorkflowDispatcher {
  private opts: Opts;

  constructor(
    private engine: JobQueueEngine,
    opts: ConstructorOpts,
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
  }

  async dispatch<Input, Meta = unknown>(
    props: WorkflowProps,
    input: Input,
    opts?: { jobId?: string, queue?: string, meta?: Meta },
  ) {
    const jobId = opts?.jobId ?? uuidv4();
    const queue = opts?.queue ?? this.opts.queue;
    const workflowInput = {
      name: props.name,
      version: props.version,
      totalSteps: props.numSteps,
      step: 0,
      input,
    } satisfies WorkflowJobData<Input>;
    return await this.engine.submitJob({
      data: { id: jobId, input: workflowInput, meta: opts?.meta },
      queue,
    });
  }
  
  async dispatchAwaitingOutput<Input, Output, Meta>(
    props: Workflow<Input, Output>,
    input: Input,
    opts?: { jobId?: string, queue?: string, meta?: Meta },
  ) {
    const jobId = opts?.jobId ?? uuidv4();
    const queue = opts?.queue ?? this.opts.queue;
    const workflowInput = makeWorkflowJobData({ props, input });

    const resultStatusPromise = new Promise<JobResultStatus<unknown>>((resolve) => {
      this.engine.submitJob({
        data: { id: jobId, input: workflowInput, meta: opts?.meta },
        queue,
        statusHandler: (status) => {
          if (isResultStatus(status)) {
            resolve(status);
          }
        }
      });
    });
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
