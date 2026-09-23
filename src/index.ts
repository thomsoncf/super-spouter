import { handleAPI } from "./api";
import { consume } from "./consumer";
import { CheckpointShard } from "./durable-objects/checkpoint-shard";
import { JobCoordinator } from "./durable-objects/job-coordinator";
import type { Env } from "./env";
import type { TransferMessage } from "./model";

export { CheckpointShard, JobCoordinator };

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return handleAPI(request, env);
  },

  queue(batch: MessageBatch<TransferMessage>, env: Env): Promise<void> {
    return consume(batch, env);
  },
} satisfies ExportedHandler<Env, TransferMessage>;
