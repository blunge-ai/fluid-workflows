/**
 * Stored job state with metadata.
 */
export type StoredJobState<T = unknown> = {
  state: T,
  lastUpdatedAt: number,
};

/**
 * Storage interface for workflow state persistence and pub/sub.
 */
export interface Storage {
  /**
   * Update state and/or publish status. At least one of state or status must be provided.
   * If state is provided, it is persisted with the given TTL and lastUpdatedAt timestamp.
   * If status is provided, it is published to subscribers.
   */
  updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
  }): Promise<void>;

  /**
   * Get the stored state for a job, or undefined if not found.
   */
  getState<T = unknown>(jobId: string): Promise<StoredJobState<T> | undefined>;

  /**
   * Get all active jobs sorted by lastUpdatedAt (oldest first).
   */
  getActiveJobs<T = unknown>(): Promise<Array<{ jobId: string } & StoredJobState<T>>>;

  /**
   * Set a result and optionally publish a status.
   */
  setResult(jobId: string, result: unknown, status?: unknown): Promise<void>;

  /**
   * Close the storage connection.
   */
  close(): Promise<void>;
}
