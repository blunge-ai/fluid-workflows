import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowRunOptions } from './Workflow';
import type { JobQueueEngine, JobData, JobResult } from './JobQueueEngine';
import { timeout, assertNever, assert, Logger, defaultLogger } from './utils';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';

export type Opts = {
  logger: Logger,
};

export type ConstructorOpts = Partial<Opts>;

function findWorkflow(workflows: Workflow<any, any>[], jobData: WorkflowJobData) {
  const { name, version, totalSteps, step } = jobData;
  if (step >= totalSteps) {
    throw Error(`inconsistent jobData: current step is ${step} but expected value smaller than ${totalSteps}`);
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
    private engine: JobQueueEngine,
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
    childResults?: Record<string, JobResult<unknown>>,
  ): Promise<[Output | undefined, 'suspended' | 'success']>{
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
        // step function
        result = await step(result, runOptions);
      } else {
        const entries = (
          step instanceof Workflow
            ? [['', step]] satisfies [string, Workflow<unknown, unknown>][]
            : Object.entries(step as Record<string, Workflow<unknown, unknown>>)
        );
        const childJobs = entries.map(([key, childWorkflow]) => ({
          id: `${job.id}:step:${stepIndex}:child:${key}`,
          input: makeWorkflowJobData({
            props: childWorkflow,
            input: key === '' ? result : (result as Record<string, unknown>)[key]
          }),
          meta: undefined,
        }));
        if (!childResults) {
          const maybeResults = await this.engine.submitChildrenSuspendParent<unknown>({
            token,
            children: childJobs.map((data) => ({ data, queue })),
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
        const newInput = { ...job.input, input: result as Input, step: stepIndex } satisfies WorkflowJobData<Input>;
        await this.engine.updateJob({ queue, token, jobId: job.id, input: newInput });
      }
    }
    return [result as Output, 'success'];
  }

  run(queue: string, workflows: Workflow<any, any>[]) {
    let stop = false;
    const token = uuidv4();
    const workerPromise = (async () => {
      process.on('SIGTERM', () => {
        this.opts.logger.warn({
          queue,
        }, 'workflow runner: sigterm received; trying to stop gracefully');
        stop = true;
      });
      this.opts.logger.info({ queue }, `workflow runner: started`);
      while (!stop) {
        try {
          const {
            data: job,
            childResults
          } = await this.engine.acquireJob<WorkflowJobData, unknown, unknown>({ queue, token, block: true });
          if (job) {
            const jobId = job.id;
            try {
              const workflow = findWorkflow(workflows, job.input);
              const [output, status] = await this.runSteps(workflow, job, queue, token, childResults);
              if (status === 'suspended') {
                continue;
              } else if (status === 'success') {
                await this.engine.completeJob({ queue, token, jobId, result: { type: 'success', output } });
              } else {
                assertNever(status);
              }
            } catch (err) {
              this.opts.logger.error({
                err, queue
              }, 'workflow runner: exception occured when running workflow');
              const reason = `exception when running workflow: ${new String(err)}`;
              await this.engine.completeJob({ queue, token, jobId, result: { type: 'error', reason } });
            }
          }
        } catch (err) {
          this.opts.logger.error({
            err, queue
          }, 'workflow runner: exception occured while running worker loop; sleeping 1s before continuing');
          await timeout(1000);
        }
      }
      this.opts.logger.info({ queue }, `workflow runner: stopped`);
    })();
    return async () => {
      stop = true;
      await workerPromise;
    };
  }
}
