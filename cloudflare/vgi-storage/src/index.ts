/**
 * VGI Storage — Cloudflare Worker + Durable Object
 *
 * A single Durable Object instance backed by SQLite provides shared storage
 * for VGI distributed function execution. The Worker routes HTTP requests
 * to the DO, which executes SQL identical to FunctionStorageSqlite.
 *
 * Authentication: optional Bearer token via VGI_STORAGE_TOKEN secret.
 */

export interface Env {
  STORAGE: DurableObjectNamespace<VgiStorageDO>;
  VGI_STORAGE_TOKEN?: string;
}

// --- Worker (router) ---

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Auth check
    if (env.VGI_STORAGE_TOKEN) {
      const auth = request.headers.get("Authorization");
      if (auth !== `Bearer ${env.VGI_STORAGE_TOKEN}`) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }
    }

    if (request.method !== "POST") {
      return Response.json({ error: "method_not_allowed" }, { status: 405 });
    }

    // Route to the single "vgi" Durable Object
    try {
      const id = env.STORAGE.idFromName("vgi");
      const stub = env.STORAGE.get(id);
      return await stub.fetch(request);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: "internal", message: msg, stack: e instanceof Error ? e.stack : undefined }, { status: 500 });
    }
  },
};

// --- Durable Object ---

const CLEANUP_INTERVAL_MS = 3600_000; // 1 hour
const MAX_AGE_DAYS = 1.0;

export class VgiStorageDO implements DurableObject {
  private sql: SqlStorage;
  private initialized = false;

  constructor(
    private ctx: DurableObjectState,
    private env: Env,
  ) {
    this.sql = ctx.storage.sql;
  }

  private ensureTables(): void {
    if (this.initialized) return;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS worker_state (
        execution_id BLOB NOT NULL,
        process_id INTEGER NOT NULL,
        state_data BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (execution_id, process_id)
      );
      CREATE TABLE IF NOT EXISTS work_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id BLOB NOT NULL,
        work_item BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_work_queue_execution
        ON work_queue(execution_id);
      CREATE TABLE IF NOT EXISTS invocation_registry (
        execution_id BLOB PRIMARY KEY,
        created_at REAL DEFAULT (julianday('now'))
      );
      CREATE TABLE IF NOT EXISTS transaction_state (
        transaction_id BLOB NOT NULL,
        key BLOB NOT NULL,
        value BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (transaction_id, key)
      );
    `);
    this.initialized = true;
  }

  private scheduleCleanup(): void {
    this.ctx.storage.setAlarm(Date.now() + CLEANUP_INTERVAL_MS);
  }

  async alarm(): Promise<void> {
    this.ensureTables();
    const threshold = MAX_AGE_DAYS;
    this.sql.exec(
      `DELETE FROM worker_state WHERE julianday('now') - created_at > ?`,
      threshold,
    );
    this.sql.exec(
      `DELETE FROM work_queue WHERE julianday('now') - created_at > ?`,
      threshold,
    );
    this.sql.exec(
      `DELETE FROM invocation_registry WHERE julianday('now') - created_at > ?`,
      threshold,
    );
    this.sql.exec(
      `DELETE FROM transaction_state WHERE julianday('now') - created_at > ?`,
      threshold,
    );
    // Re-arm the alarm
    this.scheduleCleanup();
  }

  async fetch(request: Request): Promise<Response> {
    try {
      this.ensureTables();

      // Ensure cleanup alarm is set
      const currentAlarm = await this.ctx.storage.getAlarm();
      if (currentAlarm === null) {
        this.scheduleCleanup();
      }

      const url = new URL(request.url);
      const path = url.pathname.replace(/^\/+/, "");
      const body = (await request.json()) as Record<string, unknown>;

      switch (path) {
        case "worker_put":
          return this.workerPut(body);
        case "worker_collect":
          return this.workerCollect(body);
        case "worker_scan":
          return this.workerScan(body);
        case "queue_push":
          return this.queuePush(body);
        case "queue_pop":
          return this.queuePop(body);
        case "queue_clear":
          return this.queueClear(body);
        case "transaction_state_get":
          return this.transactionStateGet(body);
        case "transaction_state_put":
          return this.transactionStatePut(body);
        case "transaction_state_clear":
          return this.transactionStateClear(body);
        default:
          return Response.json({ error: "not_found" }, { status: 404 });
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: "do_internal", message: msg, stack: e instanceof Error ? e.stack : undefined }, { status: 500 });
    }
  }

  private b64ToBytes(b64: string): ArrayBuffer {
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }

  private bytesToB64(buf: ArrayBuffer | ArrayBufferLike): string {
    const bytes = new Uint8Array(buf);
    let binary = "";
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  // --- Worker State ---

  private workerPut(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);
    const workerId = body.worker_id as number;
    const state = this.b64ToBytes(body.state as string);

    this.sql.exec(
      `INSERT OR REPLACE INTO worker_state (execution_id, process_id, state_data, created_at)
       VALUES (?, ?, ?, julianday('now'))`,
      eid,
      workerId,
      state,
    );
    return Response.json({});
  }

  private workerCollect(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);

    const rows = this.sql
      .exec(`DELETE FROM worker_state WHERE execution_id = ? RETURNING state_data`, eid)
      .toArray();

    const states = rows.map((row) =>
      this.bytesToB64(row.state_data as ArrayBuffer),
    );
    return Response.json({ states });
  }

  // --- Work Queue ---

  private queuePush(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);
    const items = body.items as string[];

    // Register invocation
    this.sql.exec(
      `INSERT OR IGNORE INTO invocation_registry (execution_id) VALUES (?)`,
      eid,
    );

    // Insert work items
    for (const item of items) {
      this.sql.exec(
        `INSERT INTO work_queue (execution_id, work_item) VALUES (?, ?)`,
        eid,
        this.b64ToBytes(item),
      );
    }

    return Response.json({ count: items.length });
  }

  private queuePop(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);

    // Combined registry check + pop in a single pass.
    // If not registered, no row in invocation_registry → we detect below.
    // If registered but empty, DELETE matches nothing → item is null.
    const reg = this.sql
      .exec(`SELECT 1 FROM invocation_registry WHERE execution_id = ?`, eid)
      .toArray();
    if (reg.length === 0) {
      return Response.json({ error: "unknown_invocation" }, { status: 404 });
    }

    const rows = this.sql
      .exec(
        `DELETE FROM work_queue
         WHERE id = (SELECT id FROM work_queue WHERE execution_id = ? ORDER BY id ASC LIMIT 1)
         RETURNING work_item`,
        eid,
      )
      .toArray();

    if (rows.length === 0) {
      return Response.json({ item: null });
    }
    return Response.json({
      item: this.bytesToB64(rows[0].work_item as ArrayBuffer),
    });
  }

  private queueClear(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);

    const deleted = this.sql
      .exec(`DELETE FROM work_queue WHERE execution_id = ? RETURNING id`, eid)
      .toArray();

    this.sql.exec(
      `DELETE FROM invocation_registry WHERE execution_id = ?`,
      eid,
    );

    return Response.json({ cleared: deleted.length });
  }

  // --- Worker State (non-destructive) ---

  private workerScan(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);
    const rows = this.sql
      .exec(
        `SELECT process_id, state_data FROM worker_state WHERE execution_id = ?`,
        eid,
      )
      .toArray();
    const out = rows.map((row) => ({
      worker_id: row.process_id as number,
      state: this.bytesToB64(row.state_data as ArrayBuffer),
    }));
    return Response.json({ rows: out });
  }

  // --- Transaction State ---

  private transactionStateGet(body: Record<string, unknown>): Response {
    const txnId = this.b64ToBytes(body.transaction_id as string);
    const keysB64 = body.keys as string[];
    // Lookup is per-key so the response can return null for misses in
    // the same order the client sent — mirrors FunctionStorageSqlite's
    // contract where the result list is parallel to ``keys``.
    const values: (string | null)[] = [];
    for (const keyB64 of keysB64) {
      const key = this.b64ToBytes(keyB64);
      const rows = this.sql
        .exec(
          `SELECT value FROM transaction_state WHERE transaction_id = ? AND key = ?`,
          txnId,
          key,
        )
        .toArray();
      values.push(
        rows.length === 0
          ? null
          : this.bytesToB64(rows[0].value as ArrayBuffer),
      );
    }
    return Response.json({ values });
  }

  private transactionStatePut(body: Record<string, unknown>): Response {
    const txnId = this.b64ToBytes(body.transaction_id as string);
    const items = body.items as Array<{ key: string; value: string }>;
    for (const item of items) {
      this.sql.exec(
        `INSERT OR REPLACE INTO transaction_state
         (transaction_id, key, value, created_at)
         VALUES (?, ?, ?, julianday('now'))`,
        txnId,
        this.b64ToBytes(item.key),
        this.b64ToBytes(item.value),
      );
    }
    return Response.json({});
  }

  private transactionStateClear(body: Record<string, unknown>): Response {
    const txnId = this.b64ToBytes(body.transaction_id as string);
    const deleted = this.sql
      .exec(
        `DELETE FROM transaction_state WHERE transaction_id = ? RETURNING key`,
        txnId,
      )
      .toArray();
    return Response.json({ cleared: deleted.length });
  }
}
