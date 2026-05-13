/**
 * VGI Storage — Cloudflare Worker + Durable Object
 *
 * A single Durable Object instance backed by SQLite provides shared storage
 * for VGI distributed function execution. The Worker routes HTTP requests
 * to the DO, which executes SQL identical to FunctionStorageSqlite.
 *
 * Authentication: optional Bearer token via VGI_STORAGE_TOKEN secret.
 *
 * ── Workflow contract ─────────────────────────────────────────────────────
 *
 * Every ``execution_id`` (and ``transaction_id``) has a single linear
 * lifecycle, driven by exactly one coordinator:
 *
 *   create  → push/put repeatedly → terminal op → DONE
 *
 * Where the terminal op is ``queue_clear`` (for work queues),
 * ``worker_collect`` (for worker state), or ``transaction_state_clear``
 * (for transactions). After the terminal op, the id is never reused —
 * subsequent work allocates a fresh id.
 *
 * The Python client's ``_post`` retry loop is synchronous: all retries of
 * one logical call (carrying the same ``attempt_id``) complete or exhaust
 * before the caller can issue the next logical call. Combined with the
 * lifecycle above, this means **no two different attempts can write the
 * same row in interleaved order** — a retry of attempt A always lands
 * before any other attempt B can be in flight against the same id.
 *
 * That property is what makes the column-only replay model below sound.
 * If a future client breaks lockstep (e.g. multi-coordinator writes to one
 * execution_id, or async fire-and-forget retries that outlive ``_post``),
 * the "latest-attempt-only" replay checks here are no longer sufficient
 * and the resurrection / re-liven races flagged in code review become real.
 *
 * ── Idempotency mechanism ─────────────────────────────────────────────────
 *
 * Every destructive endpoint requires a client-generated ``attempt_id``
 * (32-char hex). The DO stores it alongside the affected row so that a
 * retried request — one whose response was lost between DO and client —
 * replays the original response instead of re-executing the write.
 * ``queue_pop`` and ``worker_collect`` use soft-delete tombstones for the
 * same reason: the row sticks around briefly so a retry can return the
 * same data. Tombstones are GC'd by the alarm 5 minutes after creation.
 */

import { DurableObject } from "cloudflare:workers";

export interface Env {
  STORAGE: DurableObjectNamespace<VgiStorageDO>;
  VGI_STORAGE_TOKEN?: string;
  VGI_STORAGE_DEBUG?: string;
}

// --- Worker (router) ---

function constantTimeEqual(a: string, b: string): boolean {
  const ea = new TextEncoder().encode(a);
  const eb = new TextEncoder().encode(b);
  // Always XOR the longer buffer in full so timing reflects max(|a|,|b|), not
  // min(|a|,|b|). Length mismatch is itself the failure signal but we hide
  // *where* the mismatch is.
  const len = Math.max(ea.byteLength, eb.byteLength);
  let diff = ea.byteLength ^ eb.byteLength;
  for (let i = 0; i < len; i++) {
    diff |= (ea[i] ?? 0) ^ (eb[i] ?? 0);
  }
  return diff === 0;
}

function debugEnabled(env: Env): boolean {
  return env.VGI_STORAGE_DEBUG === "1";
}

function internalError(env: Env, e: unknown, code: string): Response {
  const msg = e instanceof Error ? e.message : String(e);
  const body: Record<string, unknown> = { error: code };
  if (debugEnabled(env)) {
    body.message = msg;
    if (e instanceof Error && e.stack) body.stack = e.stack;
  }
  return Response.json(body, { status: 500 });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (env.VGI_STORAGE_TOKEN) {
      const auth = request.headers.get("Authorization") ?? "";
      const expected = `Bearer ${env.VGI_STORAGE_TOKEN}`;
      if (!constantTimeEqual(auth, expected)) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }
    }

    if (request.method !== "POST") {
      return Response.json({ error: "method_not_allowed" }, { status: 405 });
    }

    try {
      const id = env.STORAGE.idFromName("vgi");
      const stub = env.STORAGE.get(id);
      return await stub.fetch(request);
    } catch (e: unknown) {
      return internalError(env, e, "internal");
    }
  },
};

// --- Durable Object ---

const CLEANUP_INTERVAL_MS = 3600_000; // 1 hour
const MAX_IDLE_DAYS = 1.0;
// 5 minutes — replay window for idempotent retries. After this much time
// has passed since a soft-delete, the tombstone is physically removed.
const IDEMPOTENCY_TTL_DAYS = 5 / 1440;
const ATTEMPT_ID_RE = /^[0-9a-f]{32}$/;

export class VgiStorageDO extends DurableObject<Env> {
  private sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    // Run init once per DO lifetime under the input gate. Subsequent fetch()
    // calls skip the schema check entirely.
    ctx.blockConcurrencyWhile(async () => {
      this.ensureTables();
      if ((await ctx.storage.getAlarm()) === null) {
        this.scheduleCleanup();
      }
    });
  }

  private ensureTables(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS worker_state (
        execution_id BLOB NOT NULL,
        process_id INTEGER NOT NULL,
        state_data BLOB NOT NULL,
        last_attempt_id BLOB NOT NULL,
        collected_at REAL DEFAULT NULL,
        collected_by_attempt BLOB DEFAULT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (execution_id, process_id)
      );
      CREATE TABLE IF NOT EXISTS work_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id BLOB NOT NULL,
        work_item BLOB NOT NULL,
        attempt_id BLOB NOT NULL,
        popped_at REAL DEFAULT NULL,
        popped_by_attempt BLOB DEFAULT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_work_queue_live
        ON work_queue(execution_id, id) WHERE popped_at IS NULL;
      CREATE INDEX IF NOT EXISTS idx_work_queue_pop_replay
        ON work_queue(execution_id, popped_by_attempt)
        WHERE popped_by_attempt IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_work_queue_push_replay
        ON work_queue(execution_id, attempt_id);
      CREATE TABLE IF NOT EXISTS invocation_registry (
        execution_id BLOB PRIMARY KEY,
        last_push_attempt BLOB DEFAULT NULL,
        last_push_count INTEGER DEFAULT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now'))
      );
      CREATE TABLE IF NOT EXISTS transaction_state (
        transaction_id BLOB NOT NULL,
        key BLOB NOT NULL,
        value BLOB NOT NULL,
        last_attempt_id BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (transaction_id, key)
      );
      CREATE TABLE IF NOT EXISTS scan_worker_state (
        execution_id BLOB NOT NULL,
        stream_id BLOB NOT NULL,
        state_data BLOB NOT NULL,
        last_attempt_id BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (execution_id, stream_id)
      );
      CREATE TABLE IF NOT EXISTS aggregate_state (
        execution_id BLOB NOT NULL,
        group_id INTEGER NOT NULL,
        state_data BLOB NOT NULL,
        last_attempt_id BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (execution_id, group_id)
      );
      CREATE TABLE IF NOT EXISTS aggregate_window_partitions (
        execution_id BLOB NOT NULL,
        partition_id INTEGER NOT NULL,
        payload BLOB NOT NULL,
        last_attempt_id BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        last_activity_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (execution_id, partition_id)
      );
    `);
  }

  private scheduleCleanup(): void {
    this.ctx.storage.setAlarm(Date.now() + CLEANUP_INTERVAL_MS);
  }

  async alarm(): Promise<void> {
    const live = MAX_IDLE_DAYS;
    const tomb = IDEMPOTENCY_TTL_DAYS;

    this.sql.exec(
      `DELETE FROM worker_state
       WHERE (collected_at IS NOT NULL AND julianday('now') - collected_at > ?)
          OR (collected_at IS NULL AND julianday('now') - last_activity_at > ?)`,
      tomb,
      live,
    );
    this.sql.exec(
      `DELETE FROM work_queue
       WHERE (popped_at IS NOT NULL AND julianday('now') - popped_at > ?)
          OR (popped_at IS NULL AND julianday('now') - last_activity_at > ?)`,
      tomb,
      live,
    );
    this.sql.exec(
      `DELETE FROM invocation_registry WHERE julianday('now') - last_activity_at > ?`,
      live,
    );
    this.sql.exec(
      `DELETE FROM transaction_state WHERE julianday('now') - last_activity_at > ?`,
      live,
    );
    this.sql.exec(
      `DELETE FROM scan_worker_state WHERE julianday('now') - last_activity_at > ?`,
      live,
    );
    this.sql.exec(
      `DELETE FROM aggregate_state WHERE julianday('now') - last_activity_at > ?`,
      live,
    );
    this.sql.exec(
      `DELETE FROM aggregate_window_partitions WHERE julianday('now') - last_activity_at > ?`,
      live,
    );
    this.scheduleCleanup();
  }

  async fetch(request: Request): Promise<Response> {
    try {
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
        case "scan_worker_put":
          return this.scanWorkerPut(body);
        case "scan_worker_scan":
          return this.scanWorkerScan(body);
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
        case "aggregate_state_get":
          return this.aggregateStateGet(body);
        case "aggregate_state_put":
          return this.aggregateStatePut(body);
        case "aggregate_state_clear":
          return this.aggregateStateClear(body);
        case "aggregate_window_partition_put":
          return this.aggregateWindowPartitionPut(body);
        case "aggregate_window_partition_get":
          return this.aggregateWindowPartitionGet(body);
        case "aggregate_window_partition_delete":
          return this.aggregateWindowPartitionDelete(body);
        case "aggregate_window_partition_clear":
          return this.aggregateWindowPartitionClear(body);
        default:
          return Response.json({ error: "not_found" }, { status: 404 });
      }
    } catch (e: unknown) {
      return internalError(this.env, e, "do_internal");
    }
  }

  // --- helpers ---

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

  /**
   * Extract a required ``attempt_id`` from the request body. Returns
   * ``{ ok: true, attemptId }`` on success or ``{ ok: false, response }``
   * with a 400 to return to the caller.
   */
  private requireAttemptId(
    body: Record<string, unknown>,
  ): { ok: true; attemptId: string } | { ok: false; response: Response } {
    const raw = body.attempt_id;
    if (typeof raw !== "string" || !ATTEMPT_ID_RE.test(raw)) {
      return {
        ok: false,
        response: Response.json(
          {
            error: "bad_request",
            message: "attempt_id is required (32-char lowercase hex)",
          },
          { status: 400 },
        ),
      };
    }
    return { ok: true, attemptId: raw };
  }

  // --- Worker State ---

  private workerPut(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const workerId = body.worker_id as number;
    const state = this.b64ToBytes(body.state as string);

    // Replay-check: only the row's *most recent* writer is remembered. This
    // covers the common retry case (the Python client's `_post` retry loop
    // holds the caller, so all retries of an attempt arrive before any other
    // attempt) but does NOT protect against a stale retry that lands after a
    // different attempt has already written. INSERT OR REPLACE on the latter
    // path would clobber the newer value. In practice the client never
    // produces that sequence; callers issuing concurrent writes from outside
    // the lockstep client must serialise themselves.
    const replay = this.sql
      .exec(
        `SELECT 1 FROM worker_state
         WHERE execution_id = ? AND process_id = ? AND last_attempt_id = ?`,
        eid,
        workerId,
        a.attemptId,
      )
      .toArray();
    if (replay.length) return Response.json({});

    // INSERT OR REPLACE naturally re-livens a tombstoned row (collected_at/by
    // reset to NULL), which is the correct behaviour: a put after a collect
    // logically supersedes the collect.
    this.sql.exec(
      `INSERT OR REPLACE INTO worker_state
         (execution_id, process_id, state_data, last_attempt_id,
          collected_at, collected_by_attempt,
          created_at, last_activity_at)
       VALUES (?, ?, ?, ?, NULL, NULL, julianday('now'), julianday('now'))`,
      eid,
      workerId,
      state,
      a.attemptId,
    );
    return Response.json({});
  }

  private workerCollect(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);

    // Replay: the same attempt already collected → return those exact rows.
    // Order by process_id so the response is byte-identical across attempts;
    // UPDATE..RETURNING below also sorts the same way.
    const replay = this.sql
      .exec(
        `SELECT state_data FROM worker_state
         WHERE execution_id = ? AND collected_by_attempt = ?
         ORDER BY process_id`,
        eid,
        a.attemptId,
      )
      .toArray();
    if (replay.length) {
      return Response.json({
        states: replay.map((r) => this.bytesToB64(r.state_data as ArrayBuffer)),
      });
    }

    const rows = this.sql
      .exec(
        `UPDATE worker_state
         SET collected_at = julianday('now'),
             collected_by_attempt = ?
         WHERE execution_id = ? AND collected_at IS NULL
         RETURNING process_id, state_data`,
        a.attemptId,
        eid,
      )
      .toArray();
    // process_id is os.getpid() on the worker side — bounded well below
    // Number.MAX_SAFE_INTEGER on every supported platform, so the
    // Number() coercion is loss-free. Sort stability matters because
    // SQLite UPDATE..RETURNING does not guarantee row order, and the
    // replay-check SELECT above uses ORDER BY process_id — both paths
    // must produce byte-identical responses for the same attempt_id.
    rows.sort((x, y) => Number(x.process_id) - Number(y.process_id));
    return Response.json({
      states: rows.map((r) => this.bytesToB64(r.state_data as ArrayBuffer)),
    });
  }

  private workerScan(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);
    const rows = this.sql
      .exec(
        `SELECT process_id, state_data
         FROM worker_state
         WHERE execution_id = ? AND collected_at IS NULL
         ORDER BY process_id`,
        eid,
      )
      .toArray();
    const out = rows.map((row) => ({
      worker_id: row.process_id as number,
      state: this.bytesToB64(row.state_data as ArrayBuffer),
    }));
    return Response.json({ rows: out });
  }

  // --- Scan Worker State ---

  private scanWorkerPut(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const streamId = this.b64ToBytes(body.stream_id as string);
    const state = this.b64ToBytes(body.state as string);

    const replay = this.sql
      .exec(
        `SELECT 1 FROM scan_worker_state
         WHERE execution_id = ? AND stream_id = ? AND last_attempt_id = ?`,
        eid,
        streamId,
        a.attemptId,
      )
      .toArray();
    if (replay.length) return Response.json({});

    this.sql.exec(
      `INSERT OR REPLACE INTO scan_worker_state
         (execution_id, stream_id, state_data, last_attempt_id,
          created_at, last_activity_at)
       VALUES (?, ?, ?, ?, julianday('now'), julianday('now'))`,
      eid,
      streamId,
      state,
      a.attemptId,
    );
    return Response.json({});
  }

  private scanWorkerScan(body: Record<string, unknown>): Response {
    const eid = this.b64ToBytes(body.execution_id as string);
    const rows = this.sql
      .exec(
        `SELECT stream_id, state_data
         FROM scan_worker_state
         WHERE execution_id = ?`,
        eid,
      )
      .toArray();
    return Response.json({
      rows: rows.map((r) => ({
        stream_id: this.bytesToB64(r.stream_id as ArrayBuffer),
        state: this.bytesToB64(r.state_data as ArrayBuffer),
      })),
    });
  }

  // --- Work Queue ---

  private queuePush(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const items = body.items as string[];

    // Replay path for non-empty pushes: any row tagged with this attempt_id
    // means the previous attempt succeeded. Return the same count.
    if (items.length > 0) {
      const prior = this.sql
        .exec(
          `SELECT COUNT(*) AS n FROM work_queue
           WHERE execution_id = ? AND attempt_id = ?`,
          eid,
          a.attemptId,
        )
        .toArray();
      const n = Number(prior[0]?.n ?? 0);
      if (n > 0) return Response.json({ count: n });
    } else {
      // Empty push: no rows to scan; check the registry's last_push_attempt.
      const prior = this.sql
        .exec(
          `SELECT last_push_count FROM invocation_registry
           WHERE execution_id = ? AND last_push_attempt = ?`,
          eid,
          a.attemptId,
        )
        .toArray();
      if (prior.length) {
        return Response.json({ count: Number(prior[0].last_push_count) });
      }
    }

    // DO runtime requires storage.transactionSync() rather than raw BEGIN/COMMIT.
    // If the callback throws, the implicit write-coalescing transaction rolls
    // back automatically, so we get atomicity without manual ROLLBACK plumbing.
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `INSERT INTO invocation_registry
           (execution_id, last_push_attempt, last_push_count,
            created_at, last_activity_at)
         VALUES (?, ?, ?, julianday('now'), julianday('now'))
         ON CONFLICT(execution_id) DO UPDATE
           SET last_push_attempt = excluded.last_push_attempt,
               last_push_count   = excluded.last_push_count,
               last_activity_at  = julianday('now')`,
        eid,
        a.attemptId,
        items.length,
      );
      for (const item of items) {
        this.sql.exec(
          `INSERT INTO work_queue
             (execution_id, work_item, attempt_id,
              created_at, last_activity_at)
           VALUES (?, ?, ?, julianday('now'), julianday('now'))`,
          eid,
          this.b64ToBytes(item),
          a.attemptId,
        );
      }
    });
    return Response.json({ count: items.length });
  }

  private queuePop(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);

    const reg = this.sql
      .exec(`SELECT 1 FROM invocation_registry WHERE execution_id = ?`, eid)
      .toArray();
    if (reg.length === 0) {
      return Response.json({ error: "unknown_invocation" }, { status: 404 });
    }

    // Replay: same attempt already popped → return that exact item.
    const replay = this.sql
      .exec(
        `SELECT work_item FROM work_queue
         WHERE execution_id = ? AND popped_by_attempt = ?`,
        eid,
        a.attemptId,
      )
      .toArray();
    if (replay.length) {
      return Response.json({
        item: this.bytesToB64(replay[0].work_item as ArrayBuffer),
      });
    }

    const rows = this.sql
      .exec(
        `UPDATE work_queue
         SET popped_at = julianday('now'),
             popped_by_attempt = ?
         WHERE id = (
           SELECT id FROM work_queue
           WHERE execution_id = ? AND popped_at IS NULL
           ORDER BY id ASC LIMIT 1
         )
         RETURNING work_item`,
        a.attemptId,
        eid,
      )
      .toArray();

    // Bump registry activity even when the queue is empty — a worker
    // poll-cycling on an empty queue is still active use.
    this.sql.exec(
      `UPDATE invocation_registry
       SET last_activity_at = julianday('now')
       WHERE execution_id = ?`,
      eid,
    );

    if (rows.length === 0) return Response.json({ item: null });
    return Response.json({
      item: this.bytesToB64(rows[0].work_item as ArrayBuffer),
    });
  }

  private queueClear(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);

    const cleared = this.ctx.storage.transactionSync(() => {
      const deleted = this.sql
        .exec(`DELETE FROM work_queue WHERE execution_id = ? RETURNING id`, eid)
        .toArray();
      this.sql.exec(
        `DELETE FROM invocation_registry WHERE execution_id = ?`,
        eid,
      );
      return deleted.length;
    });
    return Response.json({ cleared });
  }

  // --- Transaction State ---

  private transactionStateGet(body: Record<string, unknown>): Response {
    const txnId = this.b64ToBytes(body.transaction_id as string);
    const keysB64 = body.keys as string[];
    // Per-key lookup so the response stays parallel to ``keys`` even with
    // misses (null entries). Matches FunctionStorageSqlite's contract.
    const values: (string | null)[] = [];
    for (const keyB64 of keysB64) {
      const key = this.b64ToBytes(keyB64);
      const rows = this.sql
        .exec(
          `SELECT value FROM transaction_state
           WHERE transaction_id = ? AND key = ?`,
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
    // Bump activity for every read row so that long-running transactions
    // that only read aren't GC'd out from under their owner. Note: this
    // touches every row in the transaction, not just the queried keys —
    // a large transaction with a small read still incurs O(transaction)
    // writes. Acceptable for our access pattern (transactions are
    // bounded and short-lived); revisit if a profile shows it.
    this.sql.exec(
      `UPDATE transaction_state
       SET last_activity_at = julianday('now')
       WHERE transaction_id = ?`,
      txnId,
    );
    return Response.json({ values });
  }

  private transactionStatePut(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const txnId = this.b64ToBytes(body.transaction_id as string);
    const items = body.items as Array<{ key: string; value: string }>;
    if (items.length === 0) return Response.json({});

    // Replay short-circuit: if the first item's row already has this
    // attempt_id, the prior write committed. Same "latest-attempt-only"
    // limitation as workerPut — see comment there.
    const firstKey = this.b64ToBytes(items[0].key);
    const prior = this.sql
      .exec(
        `SELECT 1 FROM transaction_state
         WHERE transaction_id = ? AND key = ? AND last_attempt_id = ?`,
        txnId,
        firstKey,
        a.attemptId,
      )
      .toArray();
    if (prior.length) return Response.json({});

    this.ctx.storage.transactionSync(() => {
      for (const item of items) {
        this.sql.exec(
          `INSERT OR REPLACE INTO transaction_state
             (transaction_id, key, value, last_attempt_id,
              created_at, last_activity_at)
           VALUES (?, ?, ?, ?, julianday('now'), julianday('now'))`,
          txnId,
          this.b64ToBytes(item.key),
          this.b64ToBytes(item.value),
          a.attemptId,
        );
      }
    });
    return Response.json({});
  }

  private transactionStateClear(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const txnId = this.b64ToBytes(body.transaction_id as string);
    // Single statement is atomic on its own; wrapped in transactionSync
    // for consistency with the other clear paths so future additions
    // (audit logging, soft-delete columns) inherit rollback for free.
    const cleared = this.ctx.storage.transactionSync(() => {
      const deleted = this.sql
        .exec(
          `DELETE FROM transaction_state
           WHERE transaction_id = ? RETURNING key`,
          txnId,
        )
        .toArray();
      return deleted.length;
    });
    return Response.json({ cleared });
  }

  // --- Aggregate State ---
  //
  // Per-(execution_id, group_id) state for aggregate functions. Aggregate
  // execution is heavily concurrent: multiple worker threads issue puts
  // for distinct group_ids during the UPDATE phase, then the coordinator
  // reads them back during FINALIZE. DuckDB's thread-local hash tables
  // guarantee each group_id is only written by one thread per RPC call,
  // so the only concurrency we have to worry about here is between
  // distinct RPC calls — which the DO's input gate already serializes.

  private aggregateStateGet(body: Record<string, unknown>): Response {
    // Read-only; no attempt_id required.
    const eid = this.b64ToBytes(body.execution_id as string);
    const groupIds = body.group_ids as number[];
    // Per-group lookup so the response is parallel to the input list with
    // ``null`` for misses. Matches FunctionStorageSqlite's contract
    // (function_storage.py:883).
    const out: ({ group_id: number; state: string } | null)[] = [];
    for (const gid of groupIds) {
      const rows = this.sql
        .exec(
          `SELECT state_data FROM aggregate_state
           WHERE execution_id = ? AND group_id = ?`,
          eid,
          gid,
        )
        .toArray();
      out.push(
        rows.length === 0
          ? null
          : { group_id: gid, state: this.bytesToB64(rows[0].state_data as ArrayBuffer) },
      );
    }
    return Response.json({ rows: out });
  }

  private aggregateStatePut(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const items = body.items as Array<{ group_id: number; state: string }>;
    if (items.length === 0) return Response.json({});

    // Replay short-circuit: if the first item's row already has this
    // attempt_id, the prior write committed. Same "latest-attempt-only"
    // limitation as workerPut.
    const firstGid = items[0].group_id;
    const prior = this.sql
      .exec(
        `SELECT 1 FROM aggregate_state
         WHERE execution_id = ? AND group_id = ? AND last_attempt_id = ?`,
        eid,
        firstGid,
        a.attemptId,
      )
      .toArray();
    if (prior.length) return Response.json({});

    this.ctx.storage.transactionSync(() => {
      for (const item of items) {
        this.sql.exec(
          `INSERT OR REPLACE INTO aggregate_state
             (execution_id, group_id, state_data, last_attempt_id,
              created_at, last_activity_at)
           VALUES (?, ?, ?, ?, julianday('now'), julianday('now'))`,
          eid,
          item.group_id,
          this.b64ToBytes(item.state),
          a.attemptId,
        );
      }
    });
    return Response.json({});
  }

  private aggregateStateClear(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const cleared = this.ctx.storage.transactionSync(() => {
      const deleted = this.sql
        .exec(
          `DELETE FROM aggregate_state
           WHERE execution_id = ? RETURNING group_id`,
          eid,
        )
        .toArray();
      return deleted.length;
    });
    return Response.json({ cleared });
  }

  // --- Aggregate Window Partition ---
  //
  // Per-(execution_id, partition_id) blob caching of the full partition
  // input for windowed aggregates. ``put`` and ``get`` are 1:1 per call;
  // ``delete`` removes one entry; ``clear`` is the terminal sweep called
  // by aggregate_destructor.

  private aggregateWindowPartitionPut(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const partitionId = body.partition_id as number;
    const data = this.b64ToBytes(body.data as string);

    const prior = this.sql
      .exec(
        `SELECT 1 FROM aggregate_window_partitions
         WHERE execution_id = ? AND partition_id = ? AND last_attempt_id = ?`,
        eid,
        partitionId,
        a.attemptId,
      )
      .toArray();
    if (prior.length) return Response.json({});

    this.sql.exec(
      `INSERT OR REPLACE INTO aggregate_window_partitions
         (execution_id, partition_id, payload, last_attempt_id,
          created_at, last_activity_at)
       VALUES (?, ?, ?, ?, julianday('now'), julianday('now'))`,
      eid,
      partitionId,
      data,
      a.attemptId,
    );
    return Response.json({});
  }

  private aggregateWindowPartitionGet(body: Record<string, unknown>): Response {
    // Read-only; no attempt_id required.
    const eid = this.b64ToBytes(body.execution_id as string);
    const partitionId = body.partition_id as number;
    const rows = this.sql
      .exec(
        `SELECT payload FROM aggregate_window_partitions
         WHERE execution_id = ? AND partition_id = ?`,
        eid,
        partitionId,
      )
      .toArray();
    if (rows.length === 0) return Response.json({ data: null });
    return Response.json({
      data: this.bytesToB64(rows[0].payload as ArrayBuffer),
    });
  }

  private aggregateWindowPartitionDelete(body: Record<string, unknown>): Response {
    // DELETE-by-PK is naturally idempotent (second delete is a no-op)
    // but we still require attempt_id for contract consistency.
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const partitionId = body.partition_id as number;
    this.sql.exec(
      `DELETE FROM aggregate_window_partitions
       WHERE execution_id = ? AND partition_id = ?`,
      eid,
      partitionId,
    );
    return Response.json({});
  }

  private aggregateWindowPartitionClear(body: Record<string, unknown>): Response {
    const a = this.requireAttemptId(body);
    if (!a.ok) return a.response;
    const eid = this.b64ToBytes(body.execution_id as string);
    const cleared = this.ctx.storage.transactionSync(() => {
      const deleted = this.sql
        .exec(
          `DELETE FROM aggregate_window_partitions
           WHERE execution_id = ? RETURNING partition_id`,
          eid,
        )
        .toArray();
      return deleted.length;
    });
    return Response.json({ cleared });
  }
}
