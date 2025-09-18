import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowRunOptions, findWorkflow, validateWorkflowSteps, isRestartWrapper, withRestartWrapper, isCompleteWrapper, withCompleteWrapper } from './Workflow';
import type { StepFn } from './Workflow';
import type { JobData, JobResult } from './JobQueueEngine';
import { timeout, assertNever, assert } from './utils';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';
import type { WorkflowRunner } from './WorkflowRunner';
import { WfArray, NamesOfWfs, ValueOf } from './typeHelpers';
import { Config } from './Config';

export class JobQueueWorkflowRunner<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
> implements WorkflowRunner<ValueOf<Qs>> {
  constructor(public readonly config: Config<Names, Wfs, Qs>) {}

  async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    job: JobData<WorkflowJobData<Input>>,
    queue: string,
    token: string,
    childResults?: Record<string, JobResult<unknown>>,
  ): Promise<[Output | undefined, 'suspended' | 'success']>{

    const runOptions: WorkflowRunOptions<Input, Output, unknown> = {
      progress: async (progressInfo: WorkflowProgressInfo) => {
        this.config.logger.info({
          workflowName: workflow.name,
          workflowVersion: workflow.version,
          progressInfo,
          queue,
          jobId: job.id,
        }, 'workflow runner: progress');
        return await this.config.engine.updateJob({ queue, token, jobId: job.id, progressInfo })
      },
      update: async (stepInput: unknown, progressInfo?: WorkflowProgressInfo) => {
        const input = { ...job.input, input: stepInput as any, currentStep } satisfies WorkflowJobData<Input>;
        return await this.config.engine.updateJob({ queue, token, jobId: job.id, input, progressInfo });
      },
      restart: withRestartWrapper,
      complete: withCompleteWrapper,
    };

    let currentStep = job.input.currentStep;
    let result: unknown = job.input.input;

    while (currentStep < workflow.steps.length) {
      if (currentStep === 0 && workflow.inputSchema) {
        result = workflow.inputSchema.parse(result);
      }

      const step = workflow.steps[currentStep];
      if (typeof step === 'function' && !(step instanceof Workflow)) {
        // handle step function
        if (childResults) {
          throw Error('encountered child results when running a step function');
        }
        const stepFn = step as StepFn<unknown, unknown, Input, Output>;
        const out = await stepFn(result, runOptions);
        if (isRestartWrapper(out)) {
          result = out.input as Input;
          currentStep = 0;
          await runOptions.update(result);
          continue;
        }
        if (isCompleteWrapper(out)) {
          return [out.output as Output, 'success'];
        }
        result = out;
      } else {
        // handle child or childMap steps
        const entries = (
          step instanceof Workflow
            ? [['', step]] as unknown as [string, Workflow<unknown, unknown>][]
            : Object.entries(step as unknown as Record<string, Workflow<unknown, unknown>>)
        );
        const childrenPayload = entries.map(([key, childWorkflow]) => {
          const childQueue = this.config.queueFor(childWorkflow.name as any);
          return {
            data: {
              id: `${job.id}:step:${currentStep}:child:${key}`,
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
          const maybeResults = await this.config.engine.submitChildrenSuspendParent<unknown>({
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
          const childResult = childResults[`${job.id}:step:${currentStep}:child:${key}`];
          assert(childResult);
          if (childResult.type !== 'success') {
            throw Error('error running child job');
          }
          return [key, childResult.output];
        }));
        childResults = undefined; // only valid for the first iteration
        result = step instanceof Workflow ? outputs[''] : outputs;
      }

      currentStep += 1;
      // the result of the last step will be used to complete the job and doesn't need to be persisted
      if (currentStep !== workflow.steps.length) {
        await runOptions.update(result);
      }
    }
    return [result as Output, 'success'];
  }

  run(queues: 'all' | ValueOf<Qs>[]) {
    const token = uuidv4();
    let stop = false;
    const allQueues = new Set(Object.values(this.config.queues as Record<string, string>));
    const queueSet = queues === 'all' ? allQueues : new Set(queues);
    for (const q of queueSet) {
      if (!allQueues.has(q)) {
        throw Error('run must be called with a queue that was used to initialise the runner');
      }
    }
    const loops = Array.from(queueSet).map(async (queue) => {
      this.config.logger.info({ queue }, 'workflow runner: started');
      while (!stop) {
        try {
          const { data: job, childResults } = await this.config.engine.acquireJob<WorkflowJobData, unknown, unknown>({ queue, token, block: true });
          if (job) {
            const jobId = job.id;
            try {
              const workflow = findWorkflow(this.config.allWorkflows, job.input);
              validateWorkflowSteps(workflow, job.input);
              const [output, status] = await this.runSteps(workflow, job, queue, token, childResults);
              if (status === 'suspended') {
                continue;
              } else if (status === 'success') {
                await this.config.engine.completeJob({ queue, token, jobId, result: { type: 'success', output } });
              } else {
                assertNever(status);
              }
            } catch (err) {
              this.config.logger.error({ err, queue }, 'workflow runner: exception occured when running workflow');
              const reason = `exception when running workflow: ${new String(err)}`;
              await this.config.engine.completeJob({ queue, token, jobId, result: { type: 'error', reason } });
            }
          }
        } catch (err) {
          this.config.logger.error({ err, queue }, 'workflow runner: exception occured while running worker loop; sleeping 1s before continuing');
          await timeout(1000);
        }
      }
      this.config.logger.info({ queue }, 'workflow runner: stopped');
    });

    process.on('SIGTERM', () => {
      this.config.logger.warn({}, 'workflow runner: sigterm received; trying to stop gracefully');
      stop = true;
    });

    return async () => {
      stop = true;
      await Promise.all(loops);
    };
  }
}
