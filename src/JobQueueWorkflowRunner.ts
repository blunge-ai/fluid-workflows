import { v4 as uuidv4 } from 'uuid';
import { Workflow, DispatchableWorkflow, DispatchOpts, WorkflowRunOptions } from './Workflow';
import type { JobQueue, Job } from './JobQueue';
import { timeout, assertNever, Logger, defaultLogger } from './utils';
import { WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';

export type Opts = {
  logger: Logger,
};

export type ConstructorOpts = Partial<Opts>;

function findWorkflow(workflows: Workflow <unknown, unknown > [], jobData: WorkflowJobData) {
  const { name, version, totalSteps, step } = jobData;
  if (step >= totalSteps) {
    throw Error(`inconsistent jobData: ${JSON.stringify(jobData)}`);
  }
  const workflow = workflows.find((w) => w.name === name && w.version === version);
  if (!workflow) {
    throw Error(`no workflow found for '${name}' version ${version}`);
  }
  if (workflow.steps.length !== totalSteps) {
    throw Error(`job totalSteps mismatch: expected ${workflow.steps.length}, received ${totalSteps}`);
  }
  return workflow;
}

export class JobQueueWorkflowRunner {
  private opts: Opts;

  constructor(
    private queue: JobQueue<WorkflowJobData<unknown>, unknown, unknown, unknown>,
    opts?: ConstructorOpts
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
  }

  async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    job: Job<WorkflowJobData<Input>>,
    token: string,
  ): Promise<[Output | undefined, 'sleeping' | 'success']>{
    let stepIndex = job.input.step;
    const steps = workflow.steps.slice(stepIndex);
    let result: unknown = job.input.input;

    const runOptions = {
      progress: async (phase: string, progress: number) => {
        this.opts.logger.info({
          workflowName: workflow.name,
          workflowVersion: workflow.version,
          phase,
          progress
        }, 'run steps: progress');
        return await this.queue.updateStatus({
          token,
          status: {
            type: 'active' as const,
            uniqueId: job.uniqueId,
            info: { phase, progress } satisfies WorkflowProgressInfo
          }
        })
      },
      dispatch: <Input, Output>(
        props: Workflow<Input, Output>,
        input: Input,
        opts?: DispatchOpts
      ) => new DispatchableWorkflow(props, input, opts),
    } satisfies WorkflowRunOptions;

    for (const step of steps) {
      result = await step(result, runOptions);
      if (result instanceof DispatchableWorkflow) {
        const childJob = {
          uniqueId: result.opts?.uniqueId ?? uuidv4(),
          meta: undefined, //TODO
          input: result.input,
        };
        //TODO dispatcher
        this.queue.enqueueChildren([childJob], job.uniqueId);
        return [undefined, 'sleeping'];
      }
      stepIndex += 1;
      if (stepIndex !== workflow.steps.length) {
        const newInput = { ...job.input, input: result as Input, step: stepIndex } satisfies WorkflowJobData<Input>;
        await this.queue.updateJob({ uniqueId: job.uniqueId, input: newInput });
      }
    }
    return [result as Output, 'success'];
  }

  run(workflows: Workflow<unknown, unknown>[]) {
    let stop = false;
    const token = uuidv4();
    const workerPromise = (async () => {
      process.on('SIGTERM', () => {
        this.opts.logger.warn({
          queue: this.queue.name,
        }, 'jobs worker: sigterm received; trying to stop gracefully');
        stop = true;
      });
      this.opts.logger.info({ queue: this.queue.name }, `jobs worker: started`);
      while (!stop) {
        try {
          const job = await this.queue.getNextJob({ token, block: true });
          if (job) {
            const { uniqueId } = job;
            try {
              const workflow = findWorkflow(workflows, job.input);
              const [output, status] = await this.runSteps(workflow, job, token);
              if (status === 'sleeping') {
                continue;
              } else if (status === 'success') {
                await this.queue.completeJob({ token, uniqueId, result: { type: 'success', output } });
              } else {
                assertNever(status);
              }
            } catch (err) {
              this.opts.logger.error({
                err,
                queue: this.queue.name
              }, 'jobs worker: exception occured when running workflow');
              await this.queue.completeJob({ token, uniqueId, result: { type: 'error' } });
            }
          }
        } catch (err) {
          this.opts.logger.error({
            err,
            queue: this.queue.name
          }, 'jobs worker: exception occured while running worker loop; sleeping 1s before continuing');
          await timeout(1000);
        }
      }
      this.opts.logger.info({ queue: this.queue.name }, `jobs worker: stopped`);
    })();
    return async () => {
      stop = true;
      await workerPromise;
    };
  }
}
