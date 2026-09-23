import { checkpoint } from "./durable";
import type { Env } from "./env";
import type { CheckpointSummary } from "./model";

export async function aggregateCheckpoints(env: Env, jobId: string, shardCount: number): Promise<CheckpointSummary> {
  const total = emptySummary();
  // Workers permit six simultaneous outgoing connections. Keep explicit chunks
  // so status reads do not create a large burst of pending DO requests.
  for (let offset = 0; offset < shardCount; offset += 6) {
    const summaries = await Promise.all(
      Array.from({ length: Math.min(6, shardCount - offset) }, (_, index) =>
        checkpoint(env, jobId, offset + index).summary(),
      ),
    );
    for (const summary of summaries) addSummary(total, summary);
  }
  return total;
}
function emptySummary(): CheckpointSummary {
  return {
    queued: 0,
    running: 0,
    retrying: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    bytesTransferred: 0,
  };
}

function addSummary(target: CheckpointSummary, value: CheckpointSummary): void {
  target.queued += value.queued;
  target.running += value.running;
  target.retrying += value.retrying;
  target.succeeded += value.succeeded;
  target.failed += value.failed;
  target.skipped += value.skipped;
  target.bytesTransferred += value.bytesTransferred;
}
