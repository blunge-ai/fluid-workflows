/**
 * Stored job state with metadata.
 */
export type StoredJobState<T = unknown> = {
  state: T,
  lastUpdatedAt: number,
};

/**
 * Result of attempting to acquire a lock.
 */
export type LockResult = {
  acquired: true,
  token: string,
} | {
  acquired: false,
};

/**
 * Error thrown when a lock cannot be acquired for a job.
 */
export class LockAcquisitionError extends Error {
  constructor(jobId: string) {
    super(`Failed to acquire lock for job: ${jobId}`);
    this.name = 'LockAcquisitionError';
  }
}

/**
 * Logger interface for withLock.
 */
export type LockLogger = {
  warn: (ctx: Record<string, unknown>, msg: string) => void;
  error: (ctx: Record<string, unknown>, msg: string) => void;
};

/**
 * Options for withLock helper.
 */
export type WithLockOptions = {
  storage: Storage;
  jobId: string;
  lockTimeoutMs: number;
  lockRefreshIntervalMs: number;
  logger?: LockLogger;
  /**
   * If true, automatically refresh the lock in a background loop.
   * If false (default), the lock will not be refreshed and may expire if the operation
   * takes longer than lockTimeoutMs.
   */
  autoRefreshLock?: boolean;
};

/**
 * Result of withLock execution.
 */
export type WithLockResult<T> =
  | { type: 'success'; result: T }
  | { type: 'lock-not-acquired' };

/**
 * Context passed to the callback function when a lock is held.
 */
export type LockContext = {
  token: string;
  timeoutMs: number;
};

/**
 * Execute a function with distributed locking.
 * Acquires lock, starts refresh loop, executes fn, then releases lock.
 * The callback receives a LockContext with the token and timeout for use with refreshLock operations.
 */
export async function withLock<T>(
  opts: WithLockOptions,
  fn: (lockCtx: LockContext) => Promise<T>,
): Promise<WithLockResult<T>> {
  const { storage, jobId, lockTimeoutMs, lockRefreshIntervalMs, logger } = opts;

  const lockResult = await storage.lock(jobId, lockTimeoutMs);
  if (!lockResult.acquired) {
    return { type: 'lock-not-acquired' };
  }

  const lockCtx: LockContext = { token: lockResult.token, timeoutMs: lockTimeoutMs };
  const abortController = new AbortController();

  // Start lock refresh loop only if autoRefreshLock is enabled
  if (opts.autoRefreshLock) {
    const refreshLoop = async () => {
      while (!abortController.signal.aborted) {
        await new Promise(resolve => setTimeout(resolve, lockRefreshIntervalMs));
        if (abortController.signal.aborted) break;
        const refreshed = await storage.refreshLock(jobId, lockResult.token, lockTimeoutMs);
        if (!refreshed) {
          logger?.warn({ jobId }, 'lost lock during refresh');
          break;
        }
      }
    };
    refreshLoop().catch(err => {
      if (!abortController.signal.aborted) {
        logger?.error({ jobId, err }, 'lock refresh loop error');
      }
    });
  }

  try {
    return { type: 'success', result: await fn(lockCtx) };
  } finally {
    abortController.abort();
    await storage.unlock(jobId, lockResult.token);
  }
}

/**
 * Callback for status subscription events.
 */
export type StatusListener = (status: unknown) => void;

/**
 * Storage interface for workflow state persistence and pub/sub.
 */
export interface Storage {
  /**
   * Attempt to acquire a lock for a job.
   * Returns a token if acquired, which must be passed to unlock.
   * @param jobId - The job ID to lock
   * @param ttlMs - Lock expiration time in milliseconds
   */
  lock(jobId: string, ttlMs: number): Promise<LockResult>;

  /**
   * Release a lock for a job.
   * Only releases if the token matches the one used to acquire.
   * @param jobId - The job ID to unlock
   * @param token - The token returned from lock()
   * @returns true if the lock was released, false if token didn't match or lock expired
   */
  unlock(jobId: string, token: string): Promise<boolean>;

  /**
   * Refresh (extend) a lock's TTL.
   * Only refreshes if the token matches the current lock holder.
   * @param jobId - The job ID whose lock to refresh
   * @param token - The token returned from lock()
   * @param ttlMs - New TTL in milliseconds
   * @returns true if the lock was refreshed, false if token didn't match or lock expired
   */
  refreshLock(jobId: string, token: string, ttlMs: number): Promise<boolean>;
  /**
   * Update state and/or publish status. At least one of state or status must be provided.
   * If state is provided, it is persisted with the given TTL and lastUpdatedAt timestamp.
   * If status is provided, it is published to subscribers.
   * If refreshLock is provided, atomically refresh the lock as part of the same operation.
   */
  updateState(jobId: string, opts: {
    state?: unknown,
    status?: unknown,
    ttlMs: number,
    refreshLock?: { token: string, timeoutMs: number },
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
   * @param jobId - The job ID
   * @param result - The result to store
   * @param opts.ttlMs - Result expiration time in milliseconds
   * @param opts.status - Optional status to publish
   */
  setResult(jobId: string, result: unknown, opts: { ttlMs: number, status?: unknown }): Promise<void>;

  /**
   * Get the result for a job, or undefined if not found.
   */
  getResult<T = unknown>(jobId: string): Promise<T | undefined>;

  /**
   * Wait for a lock to be released.
   * @param jobId - The job ID to wait for
   * @param timeoutMs - Maximum time to wait before giving up
   * @returns true if the lock was released, false if timed out
   */
  waitForLock(jobId: string, timeoutMs: number): Promise<boolean>;

  /**
   * Clean up expired results and stale data.
   * Should be called periodically by the application.
   * @returns Number of items cleaned up
   */
  cleanup(): Promise<number>;

  /**
   * Subscribe to status updates for a job.
   * @param jobId - The job ID to subscribe to
   * @param listener - Callback invoked when status is published
   * @returns Unsubscribe function
   */
  subscribe(jobId: string, listener: StatusListener): () => void;

  /**
   * Close the storage connection.
   */
  close(): Promise<void>;
}
