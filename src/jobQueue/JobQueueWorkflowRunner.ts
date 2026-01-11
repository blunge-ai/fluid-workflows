import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowRunOptions, findWorkflow, validateWorkflowSteps, isRestartWrapper, withRestartWrapper, isCompleteWrapper, withCompleteWrapper, isStepsChildren } from '../Workflow';
import type { StepFn } from '../Workflow';
import type { JobData, JobResult, JobStatusType } from './JobQueueEngine';
import { timeout, assertNever, assert } from '../utils';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from '../WorkflowJob';
import { WfArray, NamesOfWfs, ValueOf } from '../typeHelpers';
import { Config } from '../Config';

export class JobQueueWorkflowRunner<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
>{
  constructor(public readonly config: Config<Names, Wfs, Qs>) {}

  async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    job: JobData<WorkflowJobData<Input>>,
    queue: string,
    token: string,
    childResults?: Record<string, JobResult<unknown>>,
  ): Promise<{ status: Extract<JobStatusType, 'suspended' | 'success'>, output?: Output }>{

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

    while (currentStep < workflow.stepFns.length) {
      if (currentStep === 0 && workflow.inputSchema) {
        result = workflow.inputSchema.parse(result);
      }

      const step = workflow.stepFns[currentStep];
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
          return { status: 'success', output: out.output as Output };
        }
        // Last step's output is the workflow output (no merge)
        if (currentStep === workflow.stepFns.length - 1) {
          result = out;
        } else {
          // Merge step output with accumulated state
          result = { ...(result as Record<string, unknown>), ...(out as Record<string, unknown>) };
        }
      } else if (isStepsChildren(step)) {
        // handle .steps() - pass full accumulated input to each child, merge outputs
        const entries = Object.entries(step.children as Record<string, Workflow<unknown, unknown>>);
        const childrenPayload = entries.map(([key, childWorkflow]) => {
          const childQueue = this.config.queueFor(childWorkflow.name as any);
          return {
            data: {
              id: `${job.id}:step:${currentStep}:child:${key}`,
              input: makeWorkflowJobData({
                props: childWorkflow,
                input: result  // pass full accumulated input
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
            return { status: 'suspended' };
          }
          childResults = maybeResults;
        }
        // unwrap results and merge with existing result
        const outputs = Object.fromEntries(entries.map(([key]) => {
          assert(childResults);
          const childResult = childResults[`${job.id}:step:${currentStep}:child:${key}`];
          assert(childResult);
          if (childResult.type !== 'success') {
            throw Error('error running child job');
          }
          return [key, childResult.output];
        }));
        childResults = undefined;
        result = { ...(result as Record<string, unknown>), ...outputs };
      } else {
        // handle .step(workflow) - single child workflow
        const childWorkflow = step as unknown as Workflow<unknown, unknown>;
        const childQueue = this.config.queueFor(childWorkflow.name as any);
        const childId = `${job.id}:step:${currentStep}:child:`;
        if (!childResults) {
          const maybeResults = await this.config.engine.submitChildrenSuspendParent<unknown>({
            token,
            children: [{
              data: {
                id: childId,
                input: makeWorkflowJobData({
                  props: childWorkflow,
                  input: result
                }),
                meta: undefined,
              },
              queue: childQueue
            }],
            parentId: job.id,
            parentQueue: queue,
          });
          if (!maybeResults) {
            return { status: 'suspended' };
          }
          childResults = maybeResults;
        }
        // unwrap result
        assert(childResults);
        const childResult = childResults[childId];
        assert(childResult);
        if (childResult.type !== 'success') {
          throw Error('error running child job');
        }
        childResults = undefined;
        // Last step's output is the workflow output (no merge)
        if (currentStep === workflow.stepFns.length - 1) {
          result = childResult.output;
        } else {
          // Merge child output with accumulated state
          result = { ...(result as Record<string, unknown>), ...(childResult.output as Record<string, unknown>) };
        }
      }

      currentStep += 1;
      // the result of the last step will be used to complete the job and doesn't need to be persisted
      if (currentStep !== workflow.stepFns.length) {
        await runOptions.update(result);
      }
    }
    return { status: 'success', output: result as Output };
  }

  async runJob<Input, Output>(job: JobData<WorkflowJobData<Input>, unknown>, { childResults, queue, token }: { childResults?: Record<string, JobResult<unknown>>, queue: string, token: string }): Promise<{ status: Extract<JobStatusType, 'suspended' | 'success' | 'error'>, output?: Output }> {
    const jobId = job.id;
    try {
      const workflow = findWorkflow(this.config.allWorkflows, job.input);
      validateWorkflowSteps(workflow, job.input);
      const { output, status } = await this.runSteps<Input, Output>(workflow as Workflow<Input, Output>, job, queue, token, childResults);
      if (status === 'suspended') {
        // nothing to do
      } else if (status === 'success') {
        await this.config.engine.completeJob({ queue, token, jobId, result: { type: 'success', output } });
      } else {
        assertNever(status);
      }
      return { output, status };
    } catch (err) {
      this.config.logger.error({ err, queue }, 'workflow runner: exception occured when running workflow');
      const reason = `exception when running workflow: ${new String(err)}`;
      await this.config.engine.completeJob({ queue, token, jobId, result: { type: 'error', reason } });
      return { status: 'error' };
    }
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
            await this.runJob(job, { childResults, queue, token });
          }
        } catch (err) {
          this.config.logger.error({ err, queue }, 'workflow runner: exception occured while running worker loop; sleeping 10s before continuing');
          await timeout(10000);
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
