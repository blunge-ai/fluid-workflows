import { v4 as uuidv4 } from 'uuid';
import { Workflow, RunnableWorkflow, WorkflowRunOptions } from './Workflow';
import type { JobQueue, Job } from './JobQueue';
import { timeout, Logger, defaultLogger } from './utils';
import { WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';

export type Opts = {
  logger: Logger,
};

export type ConstructorOpts = Partial<Opts>;

export class JobQueueWorkflowRunner {
  private opts: Opts;

  constructor(
    private queue: JobQueue<WorkflowJobData<unknown>, unknown, unknown, unknown>,
    private workflows: Workflow<unknown, unknown>[],
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
  ) {
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
      }
    } satisfies WorkflowRunOptions;

    for (const step of steps) {
      result = await step(result, runOptions);
      if (result instanceof RunnableWorkflow) {
        //TODO
      }
      stepIndex += 1;
      if (stepIndex !== workflow.steps.length) {
        const newInput = { ...job.input, input: result as Input, step: stepIndex } satisfies WorkflowJobData<Input>;
        await this.queue.updateJob({ uniqueId: job.uniqueId, input: newInput });
      }
    }
    return result;
  }


  findWorkflow(jobData: WorkflowJobData) {
    const { name, version, totalSteps, step } = jobData;
    if (step >= totalSteps) {
      throw Error(`inconsistent jobData: ${JSON.stringify(jobData)}`);
    }
    const workflow = this.workflows.find((w) => w.name === name && w.version === version);
    if (!workflow) {
      throw Error(`no workflow found for '${name}' version ${version}`);
    }
    if (workflow.steps.length !== totalSteps) {
      throw Error(`job totalSteps mismatch: expected ${workflow.steps.length}, received ${totalSteps}`);
    }
    return workflow;
  }

  run() {
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
            let output;
            try {
              const workflow = this.findWorkflow(job.input);
              output = await this.runSteps(workflow, job, token);
            } catch (err) {
              this.opts.logger.error({
                err,
                queue: this.queue.name
              }, 'jobs worker: exception occured when running workflow');
              await this.queue.completeJob({ token, uniqueId, result: { type: 'error' } });
              continue;
            }
            await this.queue.completeJob({ token, uniqueId, result: { type: 'success', output: output } });
          }
        } catch (err) {
          this.opts.logger.error({
            err,
            queue: this.queue.name
          }, 'jobs worker: exception occured while running worker loop; sleeping 1s before continuing');
          await timeout(1000);
          continue;
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
