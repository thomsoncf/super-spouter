import { checkpoint } from "./durable";
import type { Env } from "./env";
import { PermanentTransferError } from "./errors";
import { processFinalize } from "./finalizer";
import type { TransferMessage } from "./model";
import { processScan } from "./scanner";
import { processCopy } from "./transfer";

export async function consume(batch: MessageBatch<TransferMessage>, env: Env): Promise<void> {
  // Wrangler config uses max_batch_size=1. Keep this sequential if operators
  // increase the batch size so long streams do not exhaust six open connections.
  for (const message of batch.messages) {
    const body = message.body;
    try {
      if (body.kind === "scan") {
        await processScan(body, message.attempts, env);
        message.ack();
        continue;
      }
      if (body.kind === "finalize") {
        if (await processFinalize(body, env)) message.ack();
        else message.retry({ delaySeconds: 10 });
        continue;
      }

      await processCopy(body, message.attempts, env);
      message.ack();
    } catch (error) {
      if (body.kind === "copy") {
        const checkpoints = checkpoint(env, body.jobId, body.shard);
        const text = errorMessage(error);
        if (error instanceof PermanentTransferError || message.attempts >= maxAttempts(env)) {
          await checkpoints.complete(body.taskId, "failed", 0, undefined, text);
          message.ack();
        } else {
          await checkpoints.retry(body.taskId, text);
          message.retry({ delaySeconds: retryDelay(message.attempts) });
        }
      } else {
        message.retry({ delaySeconds: retryDelay(message.attempts) });
      }
    }
  }
}

function maxAttempts(env: Env): number {
  const parsed = Number.parseInt(env.MAX_ATTEMPTS, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 8;
}

function retryDelay(attempt: number): number {
  return Math.min(900, 2 ** Math.min(attempt, 9));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
