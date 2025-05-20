import { v4 as uuidv4 } from 'uuid';
import { Workflow, DispatchableWorkflow, DispatchOpts, WorkflowRunOptions } from './Workflow';
import type { JobQueueEngine, JobData } from './JobQueueEngine';
import { timeout, assertNever, Logger, defaultLogger } from './utils';
import { WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';

export type Opts = {
  logger: Logger,
};

export type ConstructorOpts = Partial<Opts>;

function findWorkflow(workflows: Workflow <unknown, unknown > [], jobData: WorkflowJobData) {
  const { name, version, totalSteps, step } = jobData;
  if (step >= totalSteps) {
    throw Error(`inconsistent jobData: ${step} >= ${totalSteps}`);
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
    private engine: JobQueueEngine<WorkflowJobData<unknown>, unknown, unknown, unknown>,
    opts?: ConstructorOpts
  ) {
    this.opts = {
      logger: defaultLogger,
      ...opts,
    };
  }

  async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    job: JobData<WorkflowJobData<Input>>,
    queue: string,
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
          progress,
          queue,
          jobId: job.id,
        }, 'run steps: progress');
        return await this.engine.updateJob({
          queue,
          token,
          jobId: job.id,
          progressInfo: { phase, progress } satisfies WorkflowProgressInfo
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
          id: result.opts?.jobId ?? uuidv4(),
          meta: undefined, //TODO
          input: result.input,
        };
        const childResults = await this.engine.submitChildrenSuspendParent({
          token,
          children: [{ data: childJob , queue }],
          parentId: job.id,
          parentQueue: queue
        });
        if (!childResults) {
          return [undefined, 'sleeping'];
        }
        result = childResults[childJob.id];
      }
      stepIndex += 1;
      if (stepIndex !== workflow.steps.length) {
        const newInput = { ...job.input, input: result as Input, step: stepIndex } satisfies WorkflowJobData<Input>;
        await this.engine.updateJob({ queue, token, jobId: job.id, input: newInput });
      }
    }
    return [result as Output, 'success'];
  }

  run(queue: string, workflows: Workflow<unknown, unknown>[]) {
    let stop = false;
    const token = uuidv4();
    const workerPromise = (async () => {
      process.on('SIGTERM', () => {
        this.opts.logger.warn({
          queue,
        }, 'jobs worker: sigterm received; trying to stop gracefully');
        stop = true;
      });
      this.opts.logger.info({ queue }, `jobs worker: started`);
      while (!stop) {
        try {
          const job = await this.engine.acquireJob({ queue, token, block: true });
          if (job) {
            const jobId = job.id;
            try {
              const workflow = findWorkflow(workflows, job.input);
              const [output, status] = await this.runSteps(workflow, job, queue, token);
              if (status === 'sleeping') {
                continue;
              } else if (status === 'success') {
                await this.engine.completeJob({ queue, token, jobId, result: { type: 'success', output } });
              } else {
                assertNever(status);
              }
            } catch (err) {
              this.opts.logger.error({
                err, queue
              }, 'jobs worker: exception occured when running workflow');
              const reason = `exception when running workflow: ${new String(err)}`;
              await this.engine.completeJob({ queue, token, jobId, result: { type: 'error', reason } });
            }
          }
        } catch (err) {
          this.opts.logger.error({
            err, queue
          }, 'jobs worker: exception occured while running worker loop; sleeping 1s before continuing');
          await timeout(1000);
        }
      }
      this.opts.logger.info({ queue }, `jobs worker: stopped`);
    })();
    return async () => {
      stop = true;
      await workerPromise;
    };
  }
}
