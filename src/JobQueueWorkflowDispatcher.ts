import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowProps, findWorkflow, collectWorkflows } from './Workflow';
import { WorkflowJobData, makeWorkflowJobData } from './WorkflowJob';
import type { JobQueueEngine, JobResultStatus } from './JobQueueEngine';
import { isResultStatus } from './JobQueueEngine';
import { Logger, defaultLogger, assert } from './utils';
import { WorkflowsArray, NamesOf, QueuesOption } from './typeHelpers';

export type Opts = {
  logger: Logger,
};

type ConstructorOpts<Wfs extends WorkflowsArray<Names>, Names extends string, Qs extends Record<NamesOf<Wfs, Names>, string>>
  = Partial<Opts>
  & QueuesOption<Wfs, Names, Qs>;

export class JobQueueWorkflowDispatcher<
  const Names extends string,
  const Wfs extends WorkflowsArray<Names>,
  const Qs extends Record<NamesOf<Wfs, Names>, string>
> {
  private opts: Opts;
  private queuesMap: Record<string, string>;
  private allWorkflows: Workflow<any, any, any>[];

  constructor(
    private engine: JobQueueEngine,
    workflows: Wfs,
    opts: ConstructorOpts<Wfs, Names, Qs>,
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
    this.queuesMap = opts.queues;
    this.allWorkflows = collectWorkflows(workflows);
    for (const workflow of this.allWorkflows) {
      if (!this.queuesMap[workflow.name]) {
        throw Error(`no queue found workflow ${workflow.name}`);
      }
    }
  }

   async dispatch<Input, Meta = unknown>(
    props: WorkflowProps,
    input: Input,
    opts?: { jobId?: string, meta?: Meta },
  ) {
    // ensure the correct workflow/version was passed to the constructor
    findWorkflow(this.allWorkflows, props);

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;
    const queue = this.queuesMap[props.name];
    assert(queue, 'queue not found');

    const workflowInput = {
      name: props.name,
      version: props.version,
      totalSteps: props.numSteps,
      currentStep: 0,
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
