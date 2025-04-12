import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowProps } from './Workflow';
import { WorkflowJobData } from './WorkflowJob';
import type { JobQueue, JobStatus, JobResultStatus } from './JobQueue';
import { Logger, defaultLogger } from './utils';

export type Opts = {
  logger: Logger,
};

export type ConstructorOpts = Partial<Opts>;

export class JobQueueWorkflowDispatcher {
  private opts: Opts;

  constructor(
    private dispatcher: <Input, Output>(workflow: WorkflowProps)  => JobQueue<WorkflowJobData<Input>, Output, unknown, unknown>,
    opts?: ConstructorOpts,
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
  }

  async dispatch<Input>(
    props: WorkflowProps,
    input: Input,
    opts?: { uniqueId?: string },
  ) {
    const uniqueId = opts?.uniqueId ?? uuidv4();
    const jobData = {
      name: props.name,
      version: props.version,
      totalSteps: props.numSteps,
      step: 0,
      input,
    } satisfies WorkflowJobData<Input>;
    const queue = this.dispatcher(props);
    return await queue.enqueueJob({
      uniqueId,
      meta: undefined,
      input: jobData,
    });
  }
  
  async dispatchAwaitingOutput<Input, Output>(
    props: Workflow<Input, Output>,
    input: Input,
    opts?: { uniqueId?: string },
  ) {
    const uniqueId = opts?.uniqueId ?? uuidv4();
    const queue = this.dispatcher(props);
    const resultStatusPromise = new Promise<[Promise<() => Promise<void>>, JobResultStatus<unknown>]>((resolve) => {
      const unsubscribe = queue.subscribe(uniqueId, (status: JobStatus<unknown, unknown>) => {
        if (status.type === 'success' || status.type === 'cancelled' || status.type === 'error') {
          resolve([unsubscribe, status]);
        }
      });
    });
    const { outputKey } = await this.dispatch(props, input, { uniqueId });
    const [ unsubscribe, resultStatus ] = await resultStatusPromise;
    await (await unsubscribe)()
    if (resultStatus.type !== 'success') {
      this.opts.logger.error({ resulType: resultStatus.type, queue: queue.name }, 'run: job unsuccessful');
      throw Error('job unsuccessful');
    }
    const result = await queue.getResult({ uniqueId, outputKey });
    return result.output as Output;
  }

}
