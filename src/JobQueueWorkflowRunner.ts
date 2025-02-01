import { v4 as uuidv4 } from 'uuid';
import { Workflow, RunnableWorkflow, WorkflowRunOptions } from './Workflow';
import type { JobQueue, Job } from './JobQueue';
import { timeout, Logger, defaultLogger } from './utils';

export type Opts = {
  logger: Logger,
};

export type ConstructorOpts = Partial<Opts>;

type WorkflowJobData<Input> = {
  name: string,
  version: number,
  totalSteps: number,
  step: number,
  input: Input,
};

type WorkflowProgressInfo = {
  phase: string,
  progress: number,
};

function findWorkflow<Input, Output>(workflows: Workflow<Input, Output>[], jobData: WorkflowJobData<Input>) {
  const { name, version, totalSteps, step } = jobData;
  if (step >= totalSteps) {
    throw Error(`inconsistent jobData: ${JSON.stringify(jobData)}`);
  }
  const workflow = workflows.find((w) => w.opts.name === name && w.opts.version === version);
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
    opts: ConstructorOpts
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
    let result: unknown = job.input;

    const runOptions = {
      progress: async (phase: string, progress: number) => {
        this.opts.logger.info({
          workflowName: workflow.opts.name,
          workflowVersion: workflow.opts.version,
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
      stepIndex += 1;
      if (stepIndex !== workflow.steps.length) {
        const newInput = { ...job.input, input: result as Input, step: stepIndex } satisfies WorkflowJobData<Input>;
        await this.queue.updateJob({ uniqueId: job.uniqueId, input: newInput });
      }
    }
    return result;
  }

  async startWorkerProcess<Input, Output>(workflows: Workflow<Input, Output>[]) {
    const token = uuidv4();
    let stop = false;
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
            const workflow = findWorkflow(workflows, job.input);
            output = await this.runSteps(workflow, job, token);
          } catch (err) { 
            this.opts.logger.error({
              err,
              queue: this.queue.name
            }, 'jobs worker: error in worker function');
            await this.queue.completeJob({ token, uniqueId, result: { type: 'error' } });
            continue;
          }
          await this.queue.completeJob({ token, uniqueId, result: { type: 'success', output: output } });
        }
      } catch (err) {
        this.opts.logger.error({
          err,
          queue: this.queue.name
        }, 'jobs worker: error while running worker loop; sleeping 1s and retrying');
        await timeout(1000);
        continue;
      }
    }
    this.opts.logger.info({ queue: this.queue.name }, `jobs worker: stopped`);
  }

  async run<Input, Output>(
    runnableWorkflow: RunnableWorkflow<Input, Output>,
    { uniqueId }: { uniqueId?: string},
  ) {
    uniqueId ??= uuidv4();
    const input = {
      name: runnableWorkflow.workflow.opts.name,
      version: runnableWorkflow.workflow.opts.version,
      totalSteps: runnableWorkflow.workflow.steps.length,
      step: 0,
      input: runnableWorkflow.input,
    } satisfies WorkflowJobData<Input>;
    return await this.queue.processJob({
      uniqueId,
      meta: undefined,
      input,
    });
  }
}
