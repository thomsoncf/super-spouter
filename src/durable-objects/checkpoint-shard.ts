import { DurableObject } from "cloudflare:workers";

import type { Env } from "../env";
import type { CheckpointRegistration, CheckpointSummary, TransferStatus } from "../model";

interface TransferRow {
  status: TransferStatus;
  attempt: number;
}
type SummaryColumn = Exclude<keyof CheckpointSummary, "bytesTransferred">;

const statusColumn: Record<TransferStatus, SummaryColumn> = {
  queued: "queued",
  running: "running",
  retrying: "retrying",
  succeeded: "succeeded",
  failed: "failed",
  skipped: "skipped",
};

export class CheckpointShard extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS transfers (
        task_id TEXT PRIMARY KEY,
        object_key TEXT NOT NULL,
        size INTEGER NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        bytes_transferred INTEGER NOT NULL DEFAULT 0,
        etag TEXT,
        error TEXT,
        updated_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS transfers_status_idx ON transfers(status);
      CREATE TABLE IF NOT EXISTS summary (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        queued INTEGER NOT NULL DEFAULT 0,
        running INTEGER NOT NULL DEFAULT 0,
        retrying INTEGER NOT NULL DEFAULT 0,
        succeeded INTEGER NOT NULL DEFAULT 0,
        failed INTEGER NOT NULL DEFAULT 0,
        skipped INTEGER NOT NULL DEFAULT 0,
        bytes_transferred INTEGER NOT NULL DEFAULT 0
      );
      INSERT OR IGNORE INTO summary (singleton) VALUES (1);
    `);
  }

  registerBatch(entries: CheckpointRegistration[]): number {
    return this.ctx.storage.transactionSync(() => {
      let inserted = 0;
      for (const entry of entries) {
        const existing = this.sql
          .exec("SELECT status, attempt FROM transfers WHERE task_id = ?", entry.taskId)
          .toArray()[0];
        if (existing) continue;
        this.sql.exec(
          `INSERT INTO transfers
           (task_id, object_key, size, status, updated_at)
           VALUES (?, ?, ?, 'queued', ?)`,
          entry.taskId,
          entry.objectKey,
          entry.size,
          new Date().toISOString(),
        );
        this.sql.exec("UPDATE summary SET queued = queued + 1 WHERE singleton = 1");
        inserted++;
      }
      return inserted;
    });
  }

  begin(taskId: string, attempt: number): boolean {
    return this.ctx.storage.transactionSync(() => {
      const row = this.getTransfer(taskId);
      if (!row) throw new Error(`checkpoint ${taskId} is not registered`);
      if (isTerminal(row.status)) return false;
      if (row.status === "running" && row.attempt >= attempt) return false;
      this.move(row.status, "running");
      this.sql.exec(
        "UPDATE transfers SET status = 'running', attempt = ?, error = NULL, updated_at = ? WHERE task_id = ?",
        attempt,
        new Date().toISOString(),
        taskId,
      );
      return true;
    });
  }

  retry(taskId: string, error: string): void {
    this.ctx.storage.transactionSync(() => {
      const row = this.getTransfer(taskId);
      if (!row || isTerminal(row.status) || row.status === "retrying") return;
      this.move(row.status, "retrying");
      this.sql.exec(
        "UPDATE transfers SET status = 'retrying', error = ?, updated_at = ? WHERE task_id = ?",
        error.slice(0, 4_096),
        new Date().toISOString(),
        taskId,
      );
    });
  }

  complete(
    taskId: string,
    status: Extract<TransferStatus, "succeeded" | "failed" | "skipped">,
    bytesTransferred: number,
    etag?: string,
    error?: string,
  ): void {
    this.ctx.storage.transactionSync(() => {
      const row = this.getTransfer(taskId);
      if (!row) throw new Error(`checkpoint ${taskId} is not registered`);
      if (isTerminal(row.status)) return;
      this.move(row.status, status, bytesTransferred);
      this.sql.exec(
        `UPDATE transfers
         SET status = ?, bytes_transferred = ?, etag = ?, error = ?, updated_at = ?
         WHERE task_id = ?`,
        status,
        bytesTransferred,
        etag ?? null,
        error?.slice(0, 4_096) ?? null,
        new Date().toISOString(),
        taskId,
      );
    });
  }

  summary(): CheckpointSummary {
    const row = this.sql.exec("SELECT * FROM summary WHERE singleton = 1").toArray()[0] as unknown as {
      queued: number;
      running: number;
      retrying: number;
      succeeded: number;
      failed: number;
      skipped: number;
      bytes_transferred: number;
    };
    return {
      queued: row.queued,
      running: row.running,
      retrying: row.retrying,
      succeeded: row.succeeded,
      failed: row.failed,
      skipped: row.skipped,
      bytesTransferred: row.bytes_transferred,
    };
  }

  private getTransfer(taskId: string): TransferRow | undefined {
    return this.sql
      .exec("SELECT status, attempt FROM transfers WHERE task_id = ?", taskId)
      .toArray()[0] as unknown as TransferRow | undefined;
  }

  private move(from: TransferStatus, to: TransferStatus, bytesTransferred = 0): void {
    if (from === to) return;
    const fromColumn = statusColumn[from];
    const toColumn = statusColumn[to];
    this.sql.exec(
      `UPDATE summary
       SET ${fromColumn} = MAX(0, ${fromColumn} - 1),
           ${toColumn} = ${toColumn} + 1,
           bytes_transferred = bytes_transferred + ?
       WHERE singleton = 1`,
      bytesTransferred,
    );
  }
}

function isTerminal(status: TransferStatus): boolean {
  return status === "succeeded" || status === "failed" || status === "skipped";
}
