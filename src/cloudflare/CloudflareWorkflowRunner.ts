import {
  Workflow,
  isCompleteWrapper,
  isRestartWrapper,
  withCompleteWrapper,
  withRestartWrapper,
  type WorkflowRunOptions,
} from '../Workflow';
import { defaultLogger, type Logger } from '../utils';
import { CloudflareWorkflowDispatcher } from './CloudflareWorkflowDispatcher';

type AnyWorkflow = Workflow<any, any, any, any, any>;

type AnyDispatcher = CloudflareWorkflowDispatcher<any, any>;


export interface CloudflareWorkflowStep {
  do<T>(name: string, callback: () => Promise<T> | T): Promise<T>;
  do<T>(name: string, options: unknown, callback: () => Promise<T> | T): Promise<T>;
}

export type CloudflareWorkflowEvent<Payload> = {
  payload: Payload;
  timestamp: Date;
  instanceId: string;
};

export type CloudflareWorkflowRunnerOptions = {
  workflows: AnyWorkflow[];
  dispatcher: AnyDispatcher;
  logger?: Logger;
  stepNamePrefix?: string;
};

export class CloudflareWorkflowRunner {
  private readonly logger: Logger;
  private readonly rootPrefix?: string;
  private readonly dispatcher: AnyDispatcher;

  constructor(options: CloudflareWorkflowRunnerOptions) {
    const { workflows, dispatcher, logger, stepNamePrefix } = options;
    if (!workflows || workflows.length === 0) {
      throw new Error('CloudflareWorkflowRunner requires at least one workflow');
    }
    if (!dispatcher) {
      throw new Error('CloudflareWorkflowRunner requires a CloudflareWorkflowDispatcher instance');
    }
    this.logger = logger ?? defaultLogger;
    this.rootPrefix = stepNamePrefix ? sanitizeSegment(stepNamePrefix) : undefined;
    this.dispatcher = dispatcher;
  }

  async run<Input, Output>(
    workflow: Workflow<Input, Output, any, any, any>,
    event: CloudflareWorkflowEvent<Input>,
    step: CloudflareWorkflowStep,
  ): Promise<Output> {
    const wf = workflow as AnyWorkflow;
    const prefix = this.workflowPrefix(wf, this.rootPrefix);
    const parsedInput = this.validateInput(wf, event.payload);
    const output = await this.executeWorkflow(wf, parsedInput, step, prefix);
    this.logger.info({ workflow: wf.name, instanceId: event.instanceId }, 'cloudflare: workflow run complete');
    return output as Output;
  }

  async runWithInput<Input, Output>(
    workflow: Workflow<Input, Output, any, any, any>,
    input: Input,
    step: CloudflareWorkflowStep,
  ): Promise<Output> {
    const wf = workflow as AnyWorkflow;
    const prefix = this.workflowPrefix(wf, this.rootPrefix);
    const parsedInput = this.validateInput(wf, input);
    const output = await this.executeWorkflow(wf, parsedInput, step, prefix);
    return output as Output;
  }

  private async executeWorkflow(
    workflow: AnyWorkflow,
    initialInput: unknown,
    stepApi: CloudflareWorkflowStep,
    prefix: string,
  ): Promise<unknown> {
    let current: unknown = initialInput;
    let index = 0;

    while (index < workflow.steps.length) {
      const wfStep = workflow.steps[index];

      if (typeof wfStep === 'function') {
        const stepName = this.stepName(prefix, index + 1);
        this.logger.info({ workflow: workflow.name, step: stepName }, 'cloudflare: step start');
        const runOptions = this.runOptions();
        const result = await stepApi.do(stepName, async () => {
          return await wfStep(current, runOptions);
        });
        if (isRestartWrapper(result)) {
          current = this.validateInput(workflow, result.input);
          index = 0;
          this.logger.info({ workflow: workflow.name, step: stepName }, 'cloudflare: restart requested');
          continue;
        }
        if (isCompleteWrapper(result)) {
          this.logger.info({ workflow: workflow.name, step: stepName }, 'cloudflare: complete requested');
          return result.output;
        }
        current = result;
        this.logger.info({ workflow: workflow.name, step: stepName }, 'cloudflare: step complete');
        index += 1;
        continue;
      }

      if (wfStep instanceof Workflow) {
        const childWorkflow = wfStep as AnyWorkflow;
        const childPrefix = this.workflowPrefix(childWorkflow, prefix);
        current = await this.runChildWorkflow({
          workflow: childWorkflow,
          input: current,
          stepApi,
          prefix: childPrefix,
        });
        index += 1;
        continue;
      }

      const children = wfStep as Record<string, AnyWorkflow>;
      if (typeof current !== 'object' || current === null) {
        throw new Error(`workflow '${workflow.name}' expected object input for childStep`);
      }
      const inputRecord = current as Record<string, unknown>;
      const outputs: Record<string, unknown> = {};
      for (const [key, childWorkflow] of Object.entries(children)) {
        const childPrefix = this.workflowPrefix(childWorkflow, prefix, key);
        outputs[key] = await this.runChildWorkflow({
          workflow: childWorkflow,
          input: inputRecord[key],
          stepApi,
          prefix: childPrefix,
        });
      }
      current = outputs;
      index += 1;
    }

    return current;
  }

  private async runChildWorkflow(args: {
    workflow: AnyWorkflow;
    input: unknown;
    stepApi: CloudflareWorkflowStep;
    prefix: string;
  }): Promise<unknown> {
    const { workflow, input, stepApi, prefix } = args;
    return await stepApi.do(`${prefix}.dispatch`, async () => {
      return await this.dispatcher.dispatchAwaitingOutput(workflow as any, input as any);
    });
  }

  private runOptions(): WorkflowRunOptions<any, any, any> {
    return {
      progress: async () => ({ interrupt: false }),
      update: async () => ({ interrupt: false }),
      restart: withRestartWrapper,
      complete: withCompleteWrapper,
    };
  }

  private validateInput(workflow: AnyWorkflow, input: unknown): unknown {
    if (workflow.inputSchema) {
      return workflow.inputSchema.parse(input);
    }
    return input;
  }

  private workflowPrefix(workflow: AnyWorkflow, parentPrefix?: string, alias?: string) {
    const workflowSegment = `${sanitizeSegment(workflow.name)}-v${workflow.version}`;
    const aliasSegment = alias ? sanitizeSegment(alias) : undefined;
    const segments = [parentPrefix, workflowSegment, aliasSegment].filter(Boolean) as string[];
    return segments.join('.');
  }

  private stepName(prefix: string, stepIndex: number) {
    const index = stepIndex.toString().padStart(3, '0');
    return `${prefix}.step.${index}`;
  }
}

function sanitizeSegment(value: string): string {
  const cleaned = value
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .toLowerCase();
  return cleaned || 'segment';
}
