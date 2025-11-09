export {
 Workflow,
 runQueueless,
} from './Workflow';
export type {
 WorkflowProps,
 WorkflowRunOptions,
 ProgressFn,
 StepFn,
} from './Workflow';

export {
  withRestartWrapper,
  withCompleteWrapper,
  isRestartWrapper,
  isCompleteWrapper,
} from './Workflow';
export {
 JobQueueWorkflowRunner
} from './jobQueue/JobQueueWorkflowRunner';
export {
 JobQueueWorkflowDispatcher
} from './jobQueue/JobQueueWorkflowDispatcher';
export type { JobQueueWorkflowDispatcherInterface } from './jobQueue/JobQueueWorkflowDispatcher';
export { Config } from './Config';
export { BullMqAdapter } from './jobQueue/BullMqAdapter';
export { InProcessWorkflowRunner } from './InProcessWorkflowRunner';
export { InMemoryJobQueueAdapter } from './jobQueue/InMemoryJobQueueAdapter';
export { HttpJobQueueEngineClient } from './jobQueue/HttpJobQueueEngineClient';
export { HttpJobQueueEngineServer } from './jobQueue/HttpJobQueueEngineServer';
export type { WorkflowDispatcher, DispatchOptions } from './WorkflowDispatcher';

export { CloudflareWorkflowDispatcher } from './cloudflare/CloudflareWorkflowDispatcher';
export type { CloudflareWorkflowDispatcherOptions } from './cloudflare/CloudflareWorkflowDispatcher';
export { CloudflareWorkflowRunner } from './cloudflare/CloudflareWorkflowRunner';
export type { CloudflareWorkflowRunnerOptions } from './cloudflare/CloudflareWorkflowRunner';
export type { InstanceStatus, InstanceDetails } from './cloudflare/CloudflareWorkflowClient';

import { Config } from './Config';
import { JobQueueWorkflowRunner } from './jobQueue/JobQueueWorkflowRunner';
import { JobQueueWorkflowDispatcher } from './jobQueue/JobQueueWorkflowDispatcher';
import { Workflow } from './Workflow';
import type { JobQueueEngine } from './jobQueue/JobQueueEngine';
import type { Logger } from './utils';
import type { WfArray, NamesOfWfs, RequireKeys } from './typeHelpers';

export function config<const Wfs extends WfArray<string>, const Qs extends Record<NamesOfWfs<Wfs>, string>>(args: {
  engine: JobQueueEngine,
  workflows: Wfs,
  queues: RequireKeys<Qs, NamesOfWfs<Wfs>>,
  logger?: Logger,
}) {
  const cfg = new Config<NamesOfWfs<Wfs>, Wfs, Qs>(args);
  const runner = new JobQueueWorkflowRunner(cfg);
  const dispatcher = new JobQueueWorkflowDispatcher(cfg);
  return { runner, dispatcher } as const;
}

export const workflow: typeof Workflow.create = Workflow.create;
