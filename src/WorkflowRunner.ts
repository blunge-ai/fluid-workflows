export interface WorkflowRunner<Q extends string> {
  run(queues: 'all' | Q[]): () => Promise<void>;
}
