import { v4 as uuidv4 } from 'uuid';
import type { WfArray, NamesOfWfs, MatchingWorkflow } from './typeHelpers';
import { Workflow, findWorkflow, isRestartWrapper, isCompleteWrapper, withRestartWrapper, withCompleteWrapper, collectWorkflows } from './Workflow';
import type { JobStatus } from './jobQueue/JobQueueEngine';
import { makeWorkflowJobData, WorkflowJobData, WorkflowProgressInfo } from './WorkflowJob';
import { defaultLogger } from './utils';
import type { Logger } from './utils';
import { JobResult } from './jobQueue/JobQueueEngine';
import type { Storage } from './Storage';
import { MemoryStorage } from './MemoryStorage';

type RunOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
};

export class WorkflowRunner<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
> {
  private readonly storage: Storage;
  private readonly allWorkflows: Workflow<unknown, unknown>[];
  private readonly logger: Logger;  
  private readonly lockTimeoutMs: number;

  constructor(
    opts: {
      workflows: Wfs,
      lockTimeoutMs: number,
      logger?: Logger,
      storage?: Storage,
    }
  ) {
    this.storage = opts.storage ?? new MemoryStorage();
    this.logger = opts.logger ?? defaultLogger;
    this.allWorkflows = collectWorkflows(opts.workflows as unknown as Workflow<unknown, unknown>[]);
    this.lockTimeoutMs = opts.lockTimeoutMs;
  }

  async run<const N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: RunOptions<Meta>,
  ) {
    const workflow = findWorkflow(this.allWorkflows, props) as Workflow<Input, Output>;

    const jobId = opts?.jobId ?? `${props.name}-${uuidv4()}`;

    let result: unknown = input;
    if (workflow.inputSchema) {
      result = workflow.inputSchema.parse(result);
    }

    const jobData = makeWorkflowJobData<Input>({ props: workflow, input }) as WorkflowJobData<unknown>;
    await this.storage.updateState(jobId, { state: jobData, ttlMs: this.lockTimeoutMs });

    this.logger.info({ name: props.name, version: props.version, meta: opts?.meta, jobId }, 'starting workflow');

    let currentStep = 0;

    const runOptions = {
      progress: async (progressInfo: WorkflowProgressInfo) => {
        const status = { type: 'active', jobId, meta: opts?.meta as Meta, info: progressInfo } as const;
        await this.storage.updateState(jobId, { status, ttlMs: this.lockTimeoutMs });
        return { interrupt: false } as const;
      },
      update: async (stepInput: unknown, progressInfo?: WorkflowProgressInfo) => {
        const status = (
          progressInfo
            ? { type: 'active', jobId, meta: opts?.meta as Meta, info: progressInfo } as const
            : undefined
        );
        await this.storage.updateState(jobId, {
          state: { ...jobData, input: stepInput, currentStep },
          status,
          ttlMs: this.lockTimeoutMs,
        });
        return { interrupt: false } as const;
      },
      restart: withRestartWrapper,
      complete: withCompleteWrapper,
    };

    try {
      while (currentStep < workflow.stepFns.length) {
        const step = workflow.stepFns[currentStep];
        if (typeof step !== 'function') {
          throw Error('child workflows not yet implemented');
        }
        const out = await (step as any)(result, runOptions);
        if (isRestartWrapper(out)) {
          result = (out as any).input as Input;
          if (workflow.inputSchema) {
            result = workflow.inputSchema.parse(result);
          }
          currentStep = 0;
          await this.storage.updateState(jobId, {
            state: { ...jobData, input: result, currentStep },
            ttlMs: this.lockTimeoutMs,
          });
          continue;
        }
        if (isCompleteWrapper(out)) {
          const output = (out as any).output as Output;
          const status = { type: 'success', jobId, meta: opts?.meta as Meta, resultKey: jobId } as const;
          await this.storage.setResult(jobId, { type: 'success', output } as JobResult<Output>, status);
          return output;
        }
        // Last step's output is the workflow output (no merge)
        if (currentStep === workflow.stepFns.length - 1) {
          result = out;
        } else {
          // Merge step output with accumulated state
          result = { ...(result as Record<string, unknown>), ...(out as Record<string, unknown>) };
        }

        currentStep += 1;
        if (currentStep !== workflow.stepFns.length) {
          await this.storage.updateState(jobId, {
            state: { ...jobData, input: result, currentStep },
            ttlMs: this.lockTimeoutMs,
          });
        }
      }

      this.logger.info({ name: props.name, version: props.version, meta: opts?.meta, jobId }, 'finished workflow');
      const output = result as Output;
      const status = { type: 'success', jobId, meta: opts?.meta as Meta, resultKey: jobId } as const;
      await this.storage.setResult(jobId, { type: 'success', output } as JobResult<Output>, status);
      return output;
    } catch (err) {
      const reason = `exception when running workflow: ${String(err)}`;
      this.logger.error({ name: props.name, version: props.version, meta: opts?.meta, jobId, reason, err }, 'workflow error');
      const status = { type: 'error', jobId, meta: opts?.meta as Meta, reason } as const;
      await this.storage.setResult(jobId, { type: 'error', reason } as JobResult<Output>, status);
      throw err;
    }
  }
}
