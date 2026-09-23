import { jobCoordinator } from "./durable";
import type { Env } from "./env";
import { aggregateCheckpoints } from "./status";
import { parseCreateJob, parseSchedule, parseSelectors } from "./validation";

export async function handleAPI(request: Request, env: Env): Promise<Response> {
  if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
  const url = new URL(request.url);
  if (request.method === "GET" && url.pathname === "/healthz") {
    return json({ status: "ok" });
  }
  if (request.method === "POST" && url.pathname === "/v1/jobs") {
    try {
      const spec = parseCreateJob(await request.json(), parseShardCount(env.CHECKPOINT_SHARDS));
      const jobId = crypto.randomUUID();
      const job = await jobCoordinator(env, jobId).create(jobId, spec);
      return json(job, 201);
    } catch (error) {
      return json({ error: errorMessage(error) }, 400);
    }
  }

  const match = /^\/v1\/jobs\/([0-9a-f-]+)(?:\/(start|schedule|selectors))?$/.exec(url.pathname);
  if (!match) return json({ error: "not found" }, 404);
  const jobId = match[1];
  if (!jobId) return json({ error: "invalid job id" }, 400);
  const coordinator = jobCoordinator(env, jobId);
  try {
    if (request.method === "POST" && match[2] === "start") {
      return json(await coordinator.start(), 202);
    }
    if (request.method === "POST" && match[2] === "schedule") {
      const at = parseSchedule(await request.json());
      return json(await coordinator.schedule(at), 202);
    }
    if (request.method === "POST" && match[2] === "selectors") {
      const selectors = parseSelectors(await request.json());
      return json(await coordinator.addSelectors(selectors), 200);
    }
    if (request.method === "GET" && !match[2]) {
      const job = await coordinator.get();
      const transfers = await aggregateCheckpoints(env, job.id, job.spec.shardCount);
      return json({ ...job, transfers });
    }
    return json({ error: "method not allowed" }, 405);
  } catch (error) {
    const message = errorMessage(error);
    return json({ error: message }, message.includes("not found") ? 404 : 409);
  }
}

function authorized(request: Request, env: Env): boolean {
  if (!env.API_TOKEN) return true;
  return request.headers.get("authorization") === `Bearer ${env.API_TOKEN}`;
}

function parseShardCount(value: string): number {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : 64;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function json(body: unknown, status = 200): Response {
  return Response.json(body, { status, headers: { "cache-control": "no-store" } });
}
