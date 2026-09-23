import { DurableObject } from "cloudflare:workers";

import type { Env } from "../env";
import type {
  FileSelector,
  FinalizeMessage,
  JobRecord,
  JobSpec,
  JobState,
  ScanMessage,
  StoredSelector,
  TransferMessage,
} from "../model";
import { sha256Hex } from "../hash";

interface JobRow {
  job_id: string;
  state: JobState;
  spec_json: string;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
  scheduled_at: string | null;
  error: string | null;
  pages_scanned: number;
  objects_discovered: number;
  listing_complete: number;
  selector_count: number;
}

interface ScanRow {
  status: "running" | "complete";
  attempt: number;
}

interface SelectorRow {
  id: number;
  selector_type: "key" | "prefix";
  selector_value: string;
}

interface OutboxRow {
  id: string;
  body: string;
}

export class JobCoordinator extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS job (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        job_id TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL,
        spec_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        started_at TEXT,
        ended_at TEXT,
        scheduled_at TEXT,
        error TEXT,
        pages_scanned INTEGER NOT NULL DEFAULT 0,
        objects_discovered INTEGER NOT NULL DEFAULT 0,
        listing_complete INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS selectors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        selector_type TEXT NOT NULL,
        selector_value TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        UNIQUE(selector_type, selector_value)
      );
      CREATE INDEX IF NOT EXISTS selectors_status_idx ON selectors(status, id);
      CREATE TABLE IF NOT EXISTS scan_pages (
        task_id TEXT PRIMARY KEY,
        selector_id INTEGER NOT NULL,
        page INTEGER NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        object_count INTEGER,
        cursor TEXT,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS outbox (
        id TEXT PRIMARY KEY,
        body TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
    `);
  }

  create(jobId: string, spec: JobSpec): JobRecord {
    const now = new Date().toISOString();
    this.sql.exec(
      `INSERT OR IGNORE INTO job
       (singleton, job_id, state, spec_json, created_at)
       VALUES (1, ?, 'pending', ?, ?)`,
      jobId,
      JSON.stringify(spec),
      now,
    );
    const job = this.get();
    if (job.id !== jobId) throw new Error("job coordinator already belongs to another job");
    return job;
  }

  get(): JobRecord {
    const row = this.sql
      .exec(
        `SELECT job.*, (SELECT COUNT(*) FROM selectors) AS selector_count
         FROM job WHERE singleton = 1`,
      )
      .toArray()[0] as unknown as JobRow | undefined;
    if (!row) throw new Error("job not found");
    return rowToJob(row);
  }

  addSelectors(selectors: FileSelector[]): JobRecord {
    const job = this.get();
    if (job.state !== "pending") throw new Error("selectors can only be changed while a job is pending");
    if (job.scheduledAt) throw new Error("selectors cannot be changed after a job is scheduled");
    this.ctx.storage.transactionSync(() => {
      for (const selector of selectors) {
        this.sql.exec(
          `INSERT OR IGNORE INTO selectors
           (selector_type, selector_value, created_at) VALUES (?, ?, ?)`,
          selector.type,
          selector.value,
          new Date().toISOString(),
        );
      }
    });
    return this.get();
  }

  async start(): Promise<JobRecord> {
    const job = this.get();
    if (job.selectorCount === 0) throw new Error("add at least one key or prefix selector before starting");
    if (job.state !== "pending" && job.state !== "running") {
      throw new Error(`cannot start job in ${job.state} state`);
    }
    this.activate();
    await this.ctx.storage.setAlarm(Date.now());
    this.ctx.waitUntil(this.runAlarmWork());
    return this.get();
  }

  async schedule(at: string): Promise<JobRecord> {
    const job = this.get();
    if (job.selectorCount === 0) throw new Error("add at least one key or prefix selector before scheduling");
    if (job.state !== "pending") throw new Error(`cannot schedule job in ${job.state} state`);
    const timestamp = new Date(at).getTime();
    if (!Number.isFinite(timestamp) || timestamp <= Date.now()) throw new Error("schedule must be in the future");
    this.sql.exec("UPDATE job SET scheduled_at = ? WHERE singleton = 1", new Date(timestamp).toISOString());
    await this.ctx.storage.setAlarm(timestamp);
    return this.get();
  }

  beginScan(taskId: string, selectorId: number, page: number, attempt: number): boolean {
    return this.ctx.storage.transactionSync(() => {
      const current = this.sql
        .exec("SELECT status, attempt FROM scan_pages WHERE task_id = ?", taskId)
        .toArray()[0] as unknown as ScanRow | undefined;
      if (current?.status === "complete") return false;
      if (current?.status === "running" && current.attempt >= attempt) return false;
      this.sql.exec(
        `INSERT INTO scan_pages (task_id, selector_id, page, status, attempt, updated_at)
         VALUES (?, ?, ?, 'running', ?, ?)
         ON CONFLICT(task_id) DO UPDATE SET
           status = 'running', attempt = excluded.attempt, updated_at = excluded.updated_at`,
        taskId,
        selectorId,
        page,
        attempt,
        new Date().toISOString(),
      );
      return true;
    });
  }

  async completeScan(
    taskId: string,
    selector: StoredSelector,
    objectCount: number,
    cursor: string | undefined,
    nextMessage?: ScanMessage,
  ): Promise<JobRecord> {
    this.ctx.storage.transactionSync(() => {
      const current = this.sql
        .exec("SELECT status, attempt FROM scan_pages WHERE task_id = ?", taskId)
        .toArray()[0] as unknown as ScanRow | undefined;
      if (!current) throw new Error(`scan task ${taskId} was not started`);
      if (current.status === "complete") return;
      this.sql.exec(
        `UPDATE scan_pages
         SET status = 'complete', object_count = ?, cursor = ?, updated_at = ?
         WHERE task_id = ?`,
        objectCount,
        cursor ?? null,
        new Date().toISOString(),
        taskId,
      );
      this.sql.exec(
        `UPDATE job
         SET pages_scanned = pages_scanned + 1,
             objects_discovered = objects_discovered + ?
         WHERE singleton = 1`,
        objectCount,
      );
      if (nextMessage) {
        this.insertOutbox(nextMessage);
      } else {
        this.sql.exec("UPDATE selectors SET status = 'complete' WHERE id = ?", selector.id);
        const incomplete = this.sql.exec("SELECT COUNT(*) AS count FROM selectors WHERE status <> 'complete'").toArray()[0] as
          | { count: number }
          | undefined;
        if ((incomplete?.count ?? 0) === 0) {
          const job = this.get();
          const finalizer: FinalizeMessage = {
            kind: "finalize",
            taskId: `finalize:${job.id}`,
            jobId: job.id,
          };
          this.sql.exec("UPDATE job SET listing_complete = 1 WHERE singleton = 1");
          this.insertOutbox(finalizer);
        }
      }
    });
    await this.ctx.storage.setAlarm(Date.now());
    this.ctx.waitUntil(this.runAlarmWork());
    return this.get();
  }

  finish(state: Extract<JobState, "succeeded" | "failed" | "canceled">, error?: string): JobRecord {
    this.sql.exec(
      `UPDATE job
       SET state = ?, error = ?, ended_at = COALESCE(ended_at, ?)
       WHERE singleton = 1 AND state = 'running'`,
      state,
      error ?? null,
      new Date().toISOString(),
    );
    return this.get();
  }

  override async alarm(): Promise<void> {
    await this.runAlarmWork();
  }

  private activate(): void {
    this.sql.exec(
      `UPDATE job
       SET state = 'running', started_at = COALESCE(started_at, ?), scheduled_at = NULL
       WHERE singleton = 1 AND state = 'pending'`,
      new Date().toISOString(),
    );
  }

  private async runAlarmWork(): Promise<void> {
    let job = this.get();
    if (job.state === "pending" && job.scheduledAt && new Date(job.scheduledAt).getTime() <= Date.now()) {
      this.activate();
      job = this.get();
    }
    if (job.state === "running") await this.stagePendingSelectors(job.id);
    await this.flushOutbox();
    await this.scheduleNextAlarm();
  }

  private async stagePendingSelectors(jobId: string): Promise<void> {
    const selectors = this.sql
      .exec(
        `SELECT id, selector_type, selector_value
         FROM selectors WHERE status = 'pending' ORDER BY id LIMIT 100`,
      )
      .toArray() as unknown as SelectorRow[];
    if (selectors.length === 0) return;
    const messages = await Promise.all(
      selectors.map(async (row): Promise<ScanMessage> => {
        const selector: StoredSelector = {
          id: row.id,
          type: row.selector_type,
          value: row.selector_value,
        };
        return {
          kind: "scan",
          taskId: await sha256Hex("scan", jobId, row.id.toString(), "1"),
          jobId,
          page: 1,
          selector,
        };
      }),
    );
    this.ctx.storage.transactionSync(() => {
      for (const message of messages) {
        this.insertOutbox(message);
        this.sql.exec("UPDATE selectors SET status = 'dispatched' WHERE id = ?", message.selector.id);
      }
    });
  }

  private insertOutbox(message: TransferMessage): void {
    this.sql.exec(
      "INSERT OR IGNORE INTO outbox (id, body, created_at) VALUES (?, ?, ?)",
      message.taskId,
      JSON.stringify(message),
      new Date().toISOString(),
    );
  }

  private async flushOutbox(): Promise<void> {
    const rows = this.sql
      .exec("SELECT id, body FROM outbox ORDER BY created_at LIMIT 100")
      .toArray() as unknown as OutboxRow[];
    if (rows.length === 0) return;
    await this.env.TRANSFER_QUEUE.sendBatch(
      rows.map((row) => ({ body: JSON.parse(row.body) as TransferMessage })),
    );
    this.ctx.storage.transactionSync(() => {
      for (const row of rows) this.sql.exec("DELETE FROM outbox WHERE id = ?", row.id);
    });
  }

  private async scheduleNextAlarm(): Promise<void> {
    const job = this.get();
    const outbox = this.sql.exec("SELECT COUNT(*) AS count FROM outbox").toArray()[0] as
      | { count: number }
      | undefined;
    const pendingSelectors = this.sql
      .exec("SELECT COUNT(*) AS count FROM selectors WHERE status = 'pending'")
      .toArray()[0] as { count: number } | undefined;
    if ((outbox?.count ?? 0) > 0 || (job.state === "running" && (pendingSelectors?.count ?? 0) > 0)) {
      await this.ctx.storage.setAlarm(Date.now() + 100);
    } else if (job.state === "pending" && job.scheduledAt) {
      await this.ctx.storage.setAlarm(new Date(job.scheduledAt).getTime());
    }
  }
}

function rowToJob(row: JobRow): JobRecord {
  return {
    id: row.job_id,
    state: row.state,
    spec: JSON.parse(row.spec_json) as JobSpec,
    createdAt: row.created_at,
    startedAt: row.started_at ?? undefined,
    endedAt: row.ended_at ?? undefined,
    scheduledAt: row.scheduled_at ?? undefined,
    error: row.error ?? undefined,
    pagesScanned: row.pages_scanned,
    objectsDiscovered: row.objects_discovered,
    listingComplete: row.listing_complete === 1,
    selectorCount: row.selector_count,
  };
}
