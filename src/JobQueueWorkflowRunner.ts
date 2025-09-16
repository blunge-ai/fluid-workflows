import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowRunOptions, findWorkflow, validateWorkflowSteps } from './Workflow';
import type { JobQueueEngine, JobData, JobResult } from './JobQueueEngine';
import { timeout, assertNever, assert, Logger, defaultLogger } from './utils';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';
import type { WorkflowRunner } from './WorkflowRunner';
import { WfArray, NamesOfWfs, QueuesOption, ValueOf } from './typeHelpers';
import { Config } from './Config';

export type Opts = {
  logger: Logger,
};

type ConstructorOpts<Wfs extends WfArray<Names>, Names extends string, Qs extends Record<NamesOfWfs<Wfs>, string>>
  = Partial<Opts>
  & QueuesOption<Wfs, Names, Qs>;

export class JobQueueWorkflowRunner<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
> implements WorkflowRunner<ValueOf<Qs>> {
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

  async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    job: JobData<WorkflowJobData<Input>>,
    queue: string,
    token: string,
    childResults?: Record<string, JobResult<unknown>>,
  ): Promise<[Output | undefined, 'suspended' | 'success']>{
    let stepIndex = job.input.currentStep;
    const steps = workflow.steps.slice(stepIndex);
    let result: unknown = job.input.input;

    if (stepIndex === 0 && workflow.inputSchema) {
      result = workflow.inputSchema.parse(result);
    }

    const runOptions = {
      progress: async (phase: string, progress: number) => {
        this.opts.logger.info({
          workflowName: workflow.name,
          workflowVersion: workflow.version,
          phase,
          progress,
          queue,
          jobId: job.id,
        }, 'workflow runner: progress');
        return await this.engine.updateJob({
          queue,
          token,
          jobId: job.id,
          progressInfo: { phase, progress } satisfies WorkflowProgressInfo
        })
      },
    } satisfies WorkflowRunOptions;

    for (const step of steps) {
      if (typeof step === 'function' && !(step instanceof Workflow)) {
        // handle step function
        result = await step(result, runOptions);
      } else {
        // handle child or childMap workflows
        const entries = (
          step instanceof Workflow
            ? [['', step]] satisfies [string, Workflow<unknown, unknown>][]
            : Object.entries(step as Record<string, Workflow<unknown, unknown>>)
        );
        const childrenPayload = entries.map(([key, childWorkflow]) => {
          const childQueue = this.queuesMap[childWorkflow.name];
          assert(childQueue, 'child queue not found');
          return {
            data: {
              id: `${job.id}:step:${stepIndex}:child:${key}`,
              input: makeWorkflowJobData({
                props: childWorkflow,
                input: key === '' ? result : (result as Record<string, unknown>)[key]
              }),
              meta: undefined,
            },
            queue: childQueue
          };
        });
        if (!childResults) {
          const maybeResults = await this.engine.submitChildrenSuspendParent<unknown>({
            token,
            children: childrenPayload,
            parentId: job.id,
            parentQueue: queue,
          });
          if (!maybeResults) {
            return [undefined, 'suspended'];
          }
          childResults = maybeResults;
        }
        // unwrap results
        const outputs = Object.fromEntries(entries.map(([key]) => {
          assert(childResults);
          const childResult = childResults[`${job.id}:step:${stepIndex}:child:${key}`];
          assert(childResult);
          if (childResult.type !== 'success') {
            throw Error('error running child job');
          }
          return [key, childResult.output];
        }));
        childResults = undefined; // only valid for the first iteration
        result = step instanceof Workflow ? outputs[''] : outputs;
      }

      stepIndex += 1;
      // the result of the last step will be used to complete the job and doesn't need to be persisted
      if (stepIndex !== workflow.steps.length) {
        const newInput = { ...job.input, input: result as Input, currentStep: stepIndex } satisfies WorkflowJobData<Input>;
        await this.engine.updateJob({ queue, token, jobId: job.id, input: newInput });
      }
    }
    return [result as Output, 'success'];
  }

  run(queues: 'all' | ValueOf<Qs>[]) {
    const token = uuidv4();
    let stop = false;
    const allQueues = new Set(Object.values(this.queuesMap));
    const queueSet = queues === 'all' ? allQueues : new Set(queues);
    for (const q of queueSet) {
      if (!allQueues.has(q)) {
        throw Error('run must be called with a queue that was used to initialise the runner');
      }
    }
    const loops = Array.from(queueSet).map(async (queue) => {
      this.opts.logger.info({ queue }, 'workflow runner: started');
      while (!stop) {
        try {
          const { data: job, childResults } = await this.engine.acquireJob<WorkflowJobData, unknown, unknown>({ queue, token, block: true });
          if (job) {
            const jobId = job.id;
            try {
              const workflow = findWorkflow(this.allWorkflows, job.input);
              validateWorkflowSteps(workflow, job.input);
              const [output, status] = await this.runSteps(workflow, job, queue, token, childResults);
              if (status === 'suspended') {
                continue;
              } else if (status === 'success') {
                await this.engine.completeJob({ queue, token, jobId, result: { type: 'success', output } });
              } else {
                assertNever(status);
              }
            } catch (err) {
              this.opts.logger.error({ err, queue }, 'workflow runner: exception occured when running workflow');
              const reason = `exception when running workflow: ${new String(err)}`;
              await this.engine.completeJob({ queue, token, jobId, result: { type: 'error', reason } });
            }
          }
        } catch (err) {
          this.opts.logger.error({ err, queue }, 'workflow runner: exception occured while running worker loop; sleeping 1s before continuing');
          await timeout(1000);
        }
      }
      this.opts.logger.info({ queue }, 'workflow runner: stopped');
    });

    process.on('SIGTERM', () => {
      this.opts.logger.warn({}, 'workflow runner: sigterm received; trying to stop gracefully');
      stop = true;
    });

    return async () => {
      stop = true;
      await Promise.all(loops);
    };
  }
}
