import { jobCoordinator } from "./durable";
import type { Env } from "./env";
import type { FinalizeMessage } from "./model";
import { aggregateCheckpoints } from "./status";

export async function processFinalize(message: FinalizeMessage, env: Env): Promise<boolean> {
  const coordinator = jobCoordinator(env, message.jobId);
  const job = await coordinator.get();
  if (job.state !== "running") return true;
  if (!job.listingComplete) return false;

  const summary = await aggregateCheckpoints(env, job.id, job.spec.shardCount);
  if (summary.queued + summary.running + summary.retrying > 0) return false;
  if (summary.failed > 0) {
    await coordinator.finish("failed", `${summary.failed} transfer(s) failed`);
  } else {
    await coordinator.finish("succeeded");
  }
  return true;
}
