import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowProps, WorkflowNames } from './Workflow';
import { WorkflowJobData, makeWorkflowJobData } from './WorkflowJob';
import type { JobQueueEngine, JobResultStatus } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { Logger, defaultLogger, assert } from './utils';

export type Opts = {
  logger: Logger,
};

type WorkflowsArray = ReadonlyArray<Workflow<any, any, any>>;
type NamesOf<A extends WorkflowsArray> = WorkflowNames<A[number]>;

type RequireExactKeys<TObj, K extends PropertyKey> = Exclude<keyof TObj, K> extends never
  ? (Exclude<K, keyof TObj> extends never ? TObj : never)
  : never;

export type ConstructorOpts<A extends WorkflowsArray>
 = Partial<Opts>
 & { queues: RequireExactKeys<Record<NamesOf<A>, string>, NamesOf<A>> };

export class JobQueueWorkflowDispatcher<A extends WorkflowsArray> {
  private opts: Opts;
  private queues: Record<string, string>;

  constructor(
    private engine: JobQueueEngine,
    private workflows: A,
    opts: ConstructorOpts<A>,
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
    this.queues = opts.queues;
  }

  private findWorkflowByProps(props: WorkflowProps): Workflow<any, any, any> | undefined {
    return this.workflows.find(w => w.name === props.name && w.version === props.version && w.steps.length === props.numSteps);
  }

  async dispatch<Input, Meta = unknown>(
    props: WorkflowProps,
    input: Input,
    opts?: { jobId?: string, meta?: Meta },
  ) {
    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const wf = this.findWorkflowByProps(props);
    if (!wf) {
      throw Error(`dispatcher: no workflow found for '${props.name}' version ${props.version}`);
    }
    const queue = this.queues[props.name];
    assert(queue, 'queue not found');
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
    props: Workflow<Input, Output, any>,
    input: Input,
    opts?: { jobId?: string, meta?: Meta },
  ) {
    const wfProps: WorkflowProps = { name: props.name, version: props.version, numSteps: props.numSteps };
    const wf = this.findWorkflowByProps(wfProps);
    if (!wf) throw Error(`dispatcher: no workflow found for '${props.name}' version ${props.version}`);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = this.queues[wfProps.name];
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
