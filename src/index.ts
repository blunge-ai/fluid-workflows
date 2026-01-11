export {
 WfBuilder,
} from './WfBuilder';
export type {
 Workflow,
 WorkflowProps,
 WorkflowRunOptions,
 ProgressFn,
 StepFn,
} from './WfBuilder';

export {
  withRestartWrapper,
  withCompleteWrapper,
  isRestartWrapper,
  isCompleteWrapper,
} from './WfBuilder';
export { WfJobQueueWorker } from './jobQueue/WfJobQueueWorker';
export {
 JobQueueWorkflowDispatcher
} from './jobQueue/JobQueueWorkflowDispatcher';
export type { JobQueueWorkflowDispatcherInterface } from './jobQueue/JobQueueWorkflowDispatcher';
export { Config } from './Config';
export { BullMqAdapter } from './jobQueue/BullMqAdapter';
export { WfRunner, SuspendExecutionException } from './WfRunner';
export type { Storage } from './storage/Storage';
export { RedisStorage } from './storage/RedisStorage';
export { MemoryStorage } from './storage/MemoryStorage';
export { InMemoryJobQueueAdapter } from './jobQueue/InMemoryJobQueueAdapter';
export { HttpJobQueueEngineClient } from './jobQueue/HttpJobQueueEngineClient';
export { HttpJobQueueEngineServer } from './jobQueue/HttpJobQueueEngineServer';
export type { WfDispatcher, DispatchOptions, Runner, RunOptions } from './types';

import { Config } from './Config';
import { WfJobQueueWorker } from './jobQueue/WfJobQueueWorker';
import { JobQueueWorkflowDispatcher } from './jobQueue/JobQueueWorkflowDispatcher';
import { WfBuilder } from './WfBuilder';
import type { JobQueueEngine } from './jobQueue/JobQueueEngine';
import type { Logger } from './utils';
import type { WfArray, NamesOfWfs, RequireKeys } from './typeHelpers';

export function config<const Wfs extends WfArray<string>, const Qs extends Record<NamesOfWfs<Wfs>, string>>(args: {
  engine: JobQueueEngine,
  workflows: Wfs,
  queues: RequireKeys<Qs, NamesOfWfs<Wfs>>,
  logger?: Logger,
}) {
  const cfg = new Config<Wfs, Qs>(args);
  const worker = new WfJobQueueWorker(cfg);
  const dispatcher = new JobQueueWorkflowDispatcher(cfg);
  return { worker, dispatcher } as const;
}

export const workflow: typeof WfBuilder.create = WfBuilder.create;
