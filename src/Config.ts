import type { JobQueueEngine } from './JobQueueEngine';
import type { Logger } from './utils';
import { defaultLogger } from './utils';
import type { WfArray, NamesOfWfs, RequireKeys } from './typeHelpers';
import type { Workflow } from './Workflow';
import { collectWorkflows } from './Workflow';

export class Config<
  const Names extends NamesOfWfs<Wfs>,
  const Wfs extends WfArray<Names>,
  const Qs extends Record<NamesOfWfs<Wfs>, string>
> {
  public readonly engine: JobQueueEngine;
  public readonly workflows: Wfs;
  public readonly queues: RequireKeys<Qs, NamesOfWfs<Wfs>>;
  public readonly logger: Logger;
  public readonly allWorkflows: Workflow<unknown, unknown>[];

  constructor(args: {
    engine: JobQueueEngine,
    workflows: Wfs,
    queues: RequireKeys<Qs, NamesOfWfs<Wfs>>,
    logger?: Logger,
  }) {
    this.engine = args.engine;
    this.workflows = args.workflows;
    this.queues = args.queues;
    this.logger = args.logger ?? defaultLogger;
    this.allWorkflows = collectWorkflows(this.workflows as unknown as Workflow<unknown, unknown>[]);

    // Ensure there is a queue for every workflow that can be dispatched or run
    const queuesMap = this.queues as unknown as Record<string, string>;
    for (const workflow of this.allWorkflows) {
      if (!queuesMap[workflow.name]) {
        throw Error(`no queue found workflow ${workflow.name}`);
      }
    }
  }

  queueFor(name: string): string {
    const q = (this.queues as Record<string, string>)[name];
    if (!q) {
      throw Error(`queue not found for workflow ${name as string}`);
    }
    return q;
  }
}
