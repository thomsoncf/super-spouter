import type { Env } from "./env";
import type { CheckpointShard } from "./durable-objects/checkpoint-shard";
import type { JobCoordinator } from "./durable-objects/job-coordinator";
import { checkpointName } from "./hash";

export function jobCoordinator(env: Env, jobId: string): DurableObjectStub<JobCoordinator> {
  return env.JOBS.get(env.JOBS.idFromName(jobId));
}
export function checkpoint(env: Env, jobId: string, shard: number): DurableObjectStub<CheckpointShard> {
  const name = checkpointName(jobId, shard);
  return env.CHECKPOINTS.get(env.CHECKPOINTS.idFromName(name));
}
