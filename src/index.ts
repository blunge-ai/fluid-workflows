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
} from './JobQueueWorkflowRunner';
export {
 JobQueueWorkflowDispatcher
} from './JobQueueWorkflowDispatcher';
export type { JobQueueWorkflowDispatcherInterface } from './JobQueueWorkflowDispatcher';
export { Config } from './Config';
export { BullMqAdapter } from './BullMqAdapter';
export { InProcessWorkflowRunner } from './InProcessWorkflowRunner';
export { InMemoryJobQueueAdapter } from './InMemoryJobQueueAdapter';
export { HttpJobQueueEngineClient } from './HttpJobQueueEngineClient';
export { HttpJobQueueEngineServer } from './HttpJobQueueEngineServer';
export type { WorkflowDispatcher, DispatchOptions } from './WorkflowDispatcher';

export { CloudflareWorkflowDispatcher } from './cloudflare/CloudflareWorkflowDispatcher';
export type { CloudflareWorkflowDispatcherOptions } from './cloudflare/CloudflareWorkflowDispatcher';
export { CloudflareWorkflowRunner } from './cloudflare/CloudflareWorkflowRunner';
export type { CloudflareWorkflowRunnerOptions } from './cloudflare/CloudflareWorkflowRunner';
export type { InstanceStatus, InstanceDetails } from './cloudflare/CloudflareWorkflowClient';

import { Config } from './Config';
import { JobQueueWorkflowRunner } from './JobQueueWorkflowRunner';
import { JobQueueWorkflowDispatcher } from './JobQueueWorkflowDispatcher';
import { Workflow } from './Workflow';
import type { JobQueueEngine } from './JobQueueEngine';
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
