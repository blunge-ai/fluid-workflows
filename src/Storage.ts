/**
 * Storage interface for workflow state persistence and pub/sub.
 */
export interface Storage {
  /**
   * Update state and/or publish status. At least one of state or status must be provided.
   * If state is provided, it is persisted with the given TTL.
   * If status is provided, it is published to subscribers.
   */
  updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
  }): Promise<void>;

  /**
   * Set a result and optionally publish a status.
   */
  setResult(jobId: string, result: unknown, status?: unknown): Promise<void>;

  /**
   * Close the storage connection.
   */
  close(): Promise<void>;
}
