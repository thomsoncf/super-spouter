import type { TransferMessage } from "./model";

const encoder = new TextEncoder();

// Queues accepts at most 100 messages or 256 KB per sendBatch call. Leave room
// for envelope overhead that is not represented in JSON.stringify().
export function makeQueueBatches(
  messages: TransferMessage[],
  maxMessages = 100,
  maxBytes = 240_000,
): TransferMessage[][] {
  const batches: TransferMessage[][] = [];
  let batch: TransferMessage[] = [];
  let batchBytes = 0;
  for (const message of messages) {
    const bytes = encoder.encode(JSON.stringify(message)).byteLength + 100;
    if (bytes > 127_000) throw new Error(`queue message ${message.taskId} is too large`);
    if (batch.length > 0 && (batch.length >= maxMessages || batchBytes + bytes > maxBytes)) {
      batches.push(batch);
      batch = [];
      batchBytes = 0;
    }
    batch.push(message);
    batchBytes += bytes;
  }
  if (batch.length > 0) batches.push(batch);
  return batches;
}
