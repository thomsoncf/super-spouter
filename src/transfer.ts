import { destinationExists, putDestination } from "./destinations";
import { checkpoint, jobCoordinator } from "./durable";
import type { Env } from "./env";
import { PermanentTransferError } from "./errors";
import type { CopyMessage } from "./model";

export async function processCopy(message: CopyMessage, attempt: number, env: Env): Promise<void> {
  const coordinator = jobCoordinator(env, message.jobId);
  const job = await coordinator.get();
  if (job.state !== "running") return;

  const checkpoints = checkpoint(env, message.jobId, message.shard);
  if (!(await checkpoints.begin(message.taskId, attempt))) return;

  const maxSinglePut = parsePositiveInt(env.MAX_SINGLE_PUT_BYTES, 512 * 1024 * 1024);
  if (message.object.size > maxSinglePut) {
    throw new PermanentTransferError(
      `object is ${message.object.size} bytes; multipart/resumable transfers are required above ${maxSinglePut}`,
    );
  }

  if (job.spec.conflictPolicy !== "overwrite") {
    const exists = await destinationExists(job.spec.destination, message.object.key, env);
    if (exists && job.spec.conflictPolicy === "skip") {
      await checkpoints.complete(message.taskId, "skipped", 0);
      return;
    }
    if (exists) throw new PermanentTransferError(`destination object already exists: ${message.object.key}`);
  }

  const source = await env.SOURCE_BUCKET.get(message.object.key, {
    onlyIf: { etagMatches: message.object.etag },
  });
  if (!source) throw new PermanentTransferError(`source object disappeared: ${message.object.key}`);
  if (!("body" in source)) {
    throw new PermanentTransferError(`source object changed after it was listed: ${message.object.key}`);
  }

  const result = await putDestination(job.spec.destination, message.object, source.body, env);
  if (job.spec.verification === "size" && result.size !== message.object.size) {
    throw new Error(
      `size verification failed for ${message.object.key}: expected ${message.object.size}, got ${result.size}`,
    );
  }
  await checkpoints.complete(message.taskId, "succeeded", message.object.size, result.etag);
}

function parsePositiveInt(value: string, fallback: number): number {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
