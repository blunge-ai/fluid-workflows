import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowProps } from './Workflow';
import { WorkflowJobData } from './WorkflowJob';
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
    private engine: JobQueueEngine<WorkflowJobData<unknown>, unknown, unknown, unknown>,
    opts: ConstructorOpts,
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
  }

  async dispatch<Input>(
    props: WorkflowProps,
    input: Input,
    opts?: { jobId?: string, queue?: string },
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
      data: { id: jobId, input: workflowInput, meta: undefined },
      queue,
    });
  }
  
  async dispatchAwaitingOutput<Input, Output>(
    props: Workflow<Input, Output>,
    input: Input,
    opts?: { jobId?: string, queue?: string },
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

    const resultStatusPromise = new Promise<JobResultStatus<unknown>>((resolve, reject) => {
      this.engine.submitJob({
        data: { id: jobId, input: workflowInput, meta: undefined },
        queue,
        statusHandler: (status) => {
          if (isResultStatus(status.type)) {
            if (status.type === 'success') {
              resolve(status);
            } else {
              reject(status);
            }
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
