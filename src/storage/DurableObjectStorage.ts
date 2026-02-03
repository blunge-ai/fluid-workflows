import type { Storage, StoredJobState, LockResult } from './Storage';

/**
 * SQLite cursor interface for iterating query results.
 */
export interface SqlStorageCursor<T = Record<string, unknown>> extends Iterable<T> {
  next(): IteratorResult<T>;
  toArray(): T[];
  one(): T;
  raw(): Iterator<unknown[]>;
  readonly columnNames: string[];
  readonly rowsRead: number;
  readonly rowsWritten: number;
}

/**
 * SQLite storage API interface for Durable Objects.
 */
export interface SqlStorage {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlStorageCursor<T>;
  readonly databaseSize: number;
}

/**
 * Durable Object storage API interface with SQL support.
 */
export interface DurableObjectStorageAPI {
  sql: SqlStorage;
}

/**
 * Cloudflare Durable Object state interface.
 */
export interface DurableObjectState {
  storage: DurableObjectStorageAPI;
}

type JobStateRow = {
  job_id: string;
  state: string;
  last_updated_at: number;
};

type JobResultRow = {
  job_id: string;
  result: string;
  expires_at: number;
};



/**
 * Storage implementation for Cloudflare Durable Objects using SQLite.
 * Uses the SQL API (ctx.storage.sql) for state persistence.
 * 
 * Note: Pub/sub is not supported in Durable Objects - status updates are ignored.
 * For real-time updates, consider using WebSocket hibernation API or polling.
 */
export class DurableObjectStorage implements Storage {
  private readonly sql: SqlStorage;
  private initialized = false;

  constructor(ctx: DurableObjectState) {
    this.sql = ctx.storage.sql;
  }

  private ensureInitialized(): void {
    if (this.initialized) return;
    
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS job_states (
        job_id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        last_updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS job_results (
        job_id TEXT PRIMARY KEY,
        result TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_job_states_last_updated 
        ON job_states(last_updated_at);
    `);
    
    this.initialized = true;
  }

  async updateState(jobId: string, opts: {
    state?: unknown;
    status?: unknown;
    ttlMs: number;
  }): Promise<void> {
    this.ensureInitialized();
    
    if (opts.state !== undefined) {
      const now = Date.now();
      const stateJson = JSON.stringify(opts.state);
      
      this.sql.exec(
        `INSERT INTO job_states (job_id, state, last_updated_at) 
         VALUES (?, ?, ?)
         ON CONFLICT(job_id) DO UPDATE SET 
           state = excluded.state,
           last_updated_at = excluded.last_updated_at`,
        jobId,
        stateJson,
        now
      );
    }
    // status is ignored - Durable Objects don't have pub/sub
    // Use WebSocket hibernation API or polling for real-time updates
  }

  async getState<T = unknown>(jobId: string): Promise<StoredJobState<T> | undefined> {
    this.ensureInitialized();
    
    const rows = this.sql.exec<JobStateRow>(
      'SELECT state, last_updated_at FROM job_states WHERE job_id = ?',
      jobId
    ).toArray();
    
    const row = rows[0];
    if (!row) return undefined;
    
    return {
      state: JSON.parse(row.state) as T,
      lastUpdatedAt: row.last_updated_at,
    };
  }

  async getActiveJobs<T = unknown>(): Promise<Array<{ jobId: string } & StoredJobState<T>>> {
    this.ensureInitialized();
    
    // Get all jobs ordered by last_updated_at ascending (oldest first)
    const rows = this.sql.exec<JobStateRow>(
      'SELECT job_id, state, last_updated_at FROM job_states ORDER BY last_updated_at ASC'
    ).toArray();
    
    return rows.map(row => ({
      jobId: row.job_id,
      state: JSON.parse(row.state) as T,
      lastUpdatedAt: row.last_updated_at,
    }));
  }

  async setResult(jobId: string, result: unknown, opts: { ttlMs: number, status?: unknown }): Promise<void> {
    this.ensureInitialized();
    
    const resultJson = JSON.stringify(result);
    const expiresAt = Date.now() + opts.ttlMs;
    
    // Store result with expiration and clean up job state atomically
    this.sql.exec(
      `INSERT INTO job_results (job_id, result, expires_at) 
       VALUES (?, ?, ?)
       ON CONFLICT(job_id) DO UPDATE SET result = excluded.result, expires_at = excluded.expires_at`,
      jobId,
      resultJson,
      expiresAt
    );
    
    this.sql.exec('DELETE FROM job_states WHERE job_id = ?', jobId);
    
    // status is ignored - no pub/sub support
  }

  /**
   * No-op lock for Durable Objects.
   * Durable Objects provide built-in serialization guarantees:
   * - Single active instance globally
   * - Input/output gates prevent concurrent request interleaving
   * Therefore, explicit locking is unnecessary.
   */
  async lock(_jobId: string, _ttlMs: number): Promise<LockResult> {
    return { acquired: true, token: 'durable-object-noop' };
  }

  async unlock(_jobId: string, _token: string): Promise<boolean> {
    return true;
  }

  async refreshLock(_jobId: string, _token: string, _ttlMs: number): Promise<boolean> {
    return true;
  }

  async getResult<T = unknown>(jobId: string): Promise<T | undefined> {
    this.ensureInitialized();
    
    const now = Date.now();
    const rows = this.sql.exec<JobResultRow>(
      'SELECT result, expires_at FROM job_results WHERE job_id = ?',
      jobId
    ).toArray();
    
    const row = rows[0];
    if (!row || row.expires_at <= now) return undefined;
    
    return JSON.parse(row.result) as T;
  }

  /**
   * No-op for Durable Objects - locks always succeed immediately.
   */
  async waitForLock(_jobId: string, _timeoutMs: number): Promise<boolean> {
    return true;
  }

  async cleanup(): Promise<number> {
    this.ensureInitialized();
    
    const now = Date.now();
    const cursor = this.sql.exec('DELETE FROM job_results WHERE expires_at <= ?', now);
    return cursor.rowsWritten;
  }

  async close(): Promise<void> {
    // Durable Objects don't need explicit cleanup
    // The storage is managed by the runtime
  }
}
