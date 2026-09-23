import type { CheckpointShard } from "./durable-objects/checkpoint-shard";
import type { JobCoordinator } from "./durable-objects/job-coordinator";
import type { TransferMessage } from "./model";

export interface Env {
  SOURCE_BUCKET: R2Bucket;
  TRANSFER_QUEUE: Queue<TransferMessage>;
  JOBS: DurableObjectNamespace<JobCoordinator>;
  CHECKPOINTS: DurableObjectNamespace<CheckpointShard>;

  CHECKPOINT_SHARDS: string;
  LIST_PAGE_SIZE: string;
  MAX_ATTEMPTS: string;
  MAX_SINGLE_PUT_BYTES: string;

  S3_ACCESS_KEY_ID?: string;
  S3_SECRET_ACCESS_KEY?: string;
  GCS_CLIENT_EMAIL?: string;
  GCS_PRIVATE_KEY?: string;
  API_TOKEN?: string;
}
