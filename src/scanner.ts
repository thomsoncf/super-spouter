import { checkpoint, jobCoordinator } from "./durable";
import type { Env } from "./env";
import { checkpointShard, sha256Hex } from "./hash";
import type {
  CheckpointRegistration,
  CopyMessage,
  ObjectDescriptor,
  ScanMessage,
} from "./model";
import { makeQueueBatches } from "./queue-batches";

export async function processScan(message: ScanMessage, attempt: number, env: Env): Promise<void> {
  const coordinator = jobCoordinator(env, message.jobId);
  const job = await coordinator.get();
  if (job.state !== "running") return;
  if (!(await coordinator.beginScan(message.taskId, message.selector.id, message.page, attempt))) return;

  const listing = await listSelection(message, env);
  const copyMessages = await Promise.all(
    listing.objects.map(async (object): Promise<CopyMessage> => {
      const shard = await checkpointShard(job.id, object.key, job.spec.shardCount);
      const descriptor = toDescriptor(object);
      return {
        kind: "copy",
        taskId: await sha256Hex(
          "copy",
          job.id,
          descriptor.key,
          descriptor.version,
          job.spec.destination.provider,
          job.spec.destination.bucket,
        ),
        jobId: job.id,
        shard,
        object: descriptor,
      };
    }),
  );

  await registerCheckpoints(copyMessages, job.id, env);
  for (const batch of makeQueueBatches(copyMessages)) {
    await env.TRANSFER_QUEUE.sendBatch(batch.map((body) => ({ body })));
  }

  let nextMessage: ScanMessage | undefined;
  if (listing.truncated) {
    if (!listing.cursor) throw new Error("R2 returned a truncated listing without a cursor");
    nextMessage = {
      kind: "scan",
      taskId: await sha256Hex("scan", job.id, message.selector.id.toString(), listing.cursor),
      jobId: job.id,
      page: message.page + 1,
      selector: message.selector,
      cursor: listing.cursor,
    };
  }
  await coordinator.completeScan(
    message.taskId,
    message.selector,
    copyMessages.length,
    listing.cursor,
    nextMessage,
  );
}

async function listSelection(
  message: ScanMessage,
  env: Env,
): Promise<{ objects: R2Object[]; truncated: boolean; cursor?: string }> {
  if (message.selector.type === "key") {
    const object = await env.SOURCE_BUCKET.head(message.selector.value);
    return { objects: object ? [object] : [], truncated: false };
  }
  return env.SOURCE_BUCKET.list({
    prefix: message.selector.value,
    cursor: message.cursor,
    limit: parseBoundedInt(env.LIST_PAGE_SIZE, 1, 1_000, 1_000),
    include: ["httpMetadata", "customMetadata"],
  });
}

async function registerCheckpoints(messages: CopyMessage[], jobId: string, env: Env): Promise<void> {
  const byShard = new Map<number, CheckpointRegistration[]>();
  for (const message of messages) {
    const entries = byShard.get(message.shard) ?? [];
    entries.push({ taskId: message.taskId, objectKey: message.object.key, size: message.object.size });
    byShard.set(message.shard, entries);
  }
  const shards = [...byShard.entries()];
  // Keep fan-out within the per-invocation six-connection limit. Although the
  // runtime queues a seventh connection, an explicit bound avoids a large
  // pending RPC set and keeps scanner memory predictable.
  for (let offset = 0; offset < shards.length; offset += 6) {
    await Promise.all(
      shards
        .slice(offset, offset + 6)
        .map(([shard, entries]) => checkpoint(env, jobId, shard).registerBatch(entries)),
    );
  }
}

function toDescriptor(object: R2Object): ObjectDescriptor {
  return {
    key: object.key,
    version: object.version,
    size: object.size,
    etag: object.etag,
    uploaded: object.uploaded.toISOString(),
    httpMetadata: object.httpMetadata,
    customMetadata: object.customMetadata,
  };
}

function parseBoundedInt(value: string, min: number, max: number, fallback: number): number {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= min && parsed <= max ? parsed : fallback;
}
