import { v4 as uuidv4 } from 'uuid';
import type { WfJobData, Workflow, WfDispatcher, DispatchOptions } from '../types';
import { makeWorkflowJobData } from '../types';
import { timeout } from '../utils';
import { ValueOf, WfArray, NamesOfWfs } from '../typeHelpers';
import { Config } from '../Config';
import { WfRunner, SuspendExecutionException } from '../WfRunner';
import type { JobResult, JobData } from './JobQueueEngine';

type PendingChild = {
  workflow: Workflow<unknown, unknown>,
  input: unknown,
  jobId: string,
};

/**
 * Worker that pulls jobs from queues and runs them using WfRunner.
 * Uses an inline dispatcher to intercept child workflows and queue them.
 */
export class WfJobQueueWorker<
  const Wfs extends WfArray<string>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
> {
  constructor(
    public readonly config: Config<Wfs, Qs>,
  ) {}

  private async runJob<Input, Output>(
    job: JobData<WfJobData<Input>, unknown>,
    opts: { childResults?: Record<string, JobResult<unknown>>, queue: string, token: string },
  ): Promise<{ status: 'suspended' | 'success' | 'error', output?: Output }> {
    const { childResults, queue, token } = opts;
    const jobId = job.id;

    // Pending children collected by the dispatcher
    const pendingChildren: PendingChild[] = [];

    // Create inline dispatcher that collects children and throws SuspendExecutionException
    const dispatcher: WfDispatcher<Wfs> = {
      dispatch: async () => {
        throw new Error('dispatch() not supported in job queue worker');
      },
      dispatchAwaitingOutput: async <N extends string, I, O, No, Co, Meta>(
        workflow: Workflow<I, O, N, No, Co>,
        input: I,
        dispatchOpts?: DispatchOptions<Meta>,
      ): Promise<O> => {
        const childJobId = dispatchOpts?.jobId ?? `${jobId}:child:${uuidv4()}`;
        
        // Check if we have a cached result
        if (childResults) {
          const result = childResults[childJobId];
          if (result) {
            if (result.type !== 'success') {
              throw new Error(`Child workflow failed: ${(result as any).reason}`);
            }
            return result.output as O;
          }
        }
        
        // No result available - add to pending and throw to suspend
        pendingChildren.push({
          workflow: workflow as Workflow<unknown, unknown>,
          input,
          jobId: childJobId,
        });
        throw new SuspendExecutionException(`Waiting for child workflow: ${workflow.name}`);
      },
    };

    // Create WfRunner with the inline dispatcher
    const runner = new WfRunner({
      workflows: this.config.workflows,
      lockTimeoutMs: 60000, // TODO: make configurable
      logger: this.config.logger,
      dispatcher,
    });

    try {
      // Run with the job data provided by the queue
      const output = await runner.resume<Output>(jobId, { jobData: job.input });

      // Success - complete the job
      await this.config.engine.completeJob({
        queue,
        token,
        jobId,
        result: { type: 'success', output },
      });
      return { status: 'success', output };

    } catch (err) {
      if (err instanceof SuspendExecutionException) {
        // Child workflows need to be queued
        if (pendingChildren.length > 0) {
          const childrenPayload = pendingChildren.map((child) => {
            const childQueue = this.config.queueFor(child.workflow.name as any);
            return {
              data: {
                id: child.jobId,
                input: makeWorkflowJobData({
                  props: child.workflow,
                  input: child.input,
                }),
                meta: undefined,
              },
              queue: childQueue,
            };
          });

          const maybeResults = await this.config.engine.submitChildrenSuspendParent<unknown>({
            token,
            children: childrenPayload,
            parentId: jobId,
            parentQueue: queue,
          });

          if (maybeResults) {
            // Children already completed - re-run with results
            pendingChildren.length = 0; // Clear pending
            return this.runJob(job, { childResults: maybeResults, queue, token });
          }
        }
        return { status: 'suspended' };
      }

      // Real error - complete the job with error
      this.config.logger.error({ err, queue, jobId }, 'workflow worker: exception running workflow');
      const reason = `exception when running workflow: ${String(err)}`;
      await this.config.engine.completeJob({
        queue,
        token,
        jobId,
        result: { type: 'error', reason },
      });
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
      this.config.logger.info({ queue }, 'workflow worker: started');
      while (!stop) {
        try {
          const { data: job, childResults } = await this.config.engine.acquireJob<WfJobData, unknown, unknown>({
            queue,
            token,
            block: true,
          });
          if (job) {
            await this.runJob(job, { childResults, queue, token });
          }
        } catch (err) {
          this.config.logger.error({ err, queue }, 'workflow worker: exception in worker loop; sleeping 10s');
          await timeout(10000);
        }
      }
      this.config.logger.info({ queue }, 'workflow worker: stopped');
    });

    process.on('SIGTERM', () => {
      this.config.logger.warn({}, 'workflow worker: sigterm received; stopping gracefully');
      stop = true;
    });

    return async () => {
      stop = true;
      await Promise.all(loops);
    };
  }
}
