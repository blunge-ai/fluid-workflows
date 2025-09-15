import { v4 as uuidv4 } from 'uuid';
import { Workflow, WorkflowRunOptions, findWorkflow, validateWorkflowSteps, collectWorkflows } from './Workflow';
import type { JobQueueEngine, JobData, JobResult } from './JobQueueEngine';
import { timeout, assertNever, assert, Logger, defaultLogger } from './utils';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';
import type { WorkflowRunner } from './WorkflowRunner';

export type Opts = {
  logger: Logger,
};

type WorkflowsArray<Names extends string> = Workflow<any, any, Names>[];

type RequireExactKeys<TObj, K extends PropertyKey> = Exclude<keyof TObj, K> extends never
  ? (Exclude<K, keyof TObj> extends never ? TObj : never)
  : never;

type NamesOf<A extends WorkflowsArray<Names>, Names extends string> = A[number] extends infer U
  ? U extends Workflow<any, any, infer N> ? N : never
  : never;

export type ConstructorOpts<A extends WorkflowsArray<Names>, Names extends string, Q extends string>
  = Partial<Opts>
  & { queues: RequireExactKeys<Record<NamesOf<A, Names>, Q>, NamesOf<A, Names>> };

export class JobQueueWorkflowRunner<const Names extends string, A extends WorkflowsArray<Names>, const Q extends string> implements WorkflowRunner<Q> {
  private opts: Opts;
  private queuesMap: Record<string, Q>;
  private allWorkflows: Workflow<any, any, any>[];

  constructor(
    private engine: JobQueueEngine,
    workflows: A,
    opts: ConstructorOpts<A, Names, Q>
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

  async runSteps<Input, Output>(
    workflow: Workflow<Input, Output>,
    job: JobData<WorkflowJobData<Input>>,
    queue: Q,
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
        // step function
        result = await step(result, runOptions);
      } else {
        const entries = (
          step instanceof Workflow
            ? [['', step]] satisfies [string, Workflow<unknown, unknown>][]
            : Object.entries(step as Record<string, Workflow<unknown, unknown>>)
        );
        const childrenPayload = entries.map(([key, childWorkflow]) => {
          const childQueue = this.queuesMap[childWorkflow.name as string];
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

  run(queues: 'all' | Q[]) {
    const token = uuidv4();
    let stop = false;
    const allQueues = new Set(Object.values(this.queuesMap));
    let queueSet;
    if (queues === 'all') {
      queueSet = allQueues;
    } else {
      queueSet = new Set<Q>();
      for (const q of queues) {
        if (!allQueues.has(q)) {
          throw Error('must initialise runner with all queues that are to be run');
        }
        queueSet.add(q);
      }
    }
    const queueList = Array.from(queueSet) as Q[];
    const loops = queueList.map((queue: Q) => (async () => {
      this.opts.logger.info({ queue }, 'workflow runner: started');
      while (!stop) {
        try {
          const { data: job, childResults } = await this.engine.acquireJob<WorkflowJobData, unknown, unknown>({ queue, token, block: true });
          if (job) {
            const jobId = job.id;
            try {
              const workflow = findWorkflow(this.allWorkflows, job.input);
              validateWorkflowSteps(workflow, job.input);
              const [output, status] = await this.runSteps(workflow, job as JobData<WorkflowJobData<any>>, queue, token, childResults);
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
    })());

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
