export interface WorkflowRunner {
  run(): () => Promise<void>;
}
