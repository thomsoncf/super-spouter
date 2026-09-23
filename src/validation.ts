import type { ConflictPolicy, Destination, FileSelector, JobSpec, VerificationMode } from "./model";

export interface CreateJobRequest {
  destination: Destination;
  conflictPolicy?: ConflictPolicy;
  verification?: VerificationMode;
  shardCount?: number;
}

const allowedShardCounts = new Set([16, 32, 64, 128, 256, 512, 1024]);

export function parseCreateJob(value: unknown, defaultShardCount: number): JobSpec {
  if (!isRecord(value) || !isRecord(value.destination)) {
    throw new Error("destination is required");
  }
  const destination = parseDestination(value.destination);
  const conflictPolicy = value.conflictPolicy ?? "skip";
  if (conflictPolicy !== "skip" && conflictPolicy !== "overwrite") {
    throw new Error("conflictPolicy must be skip or overwrite");
  }
  const verification = value.verification ?? "size";
  if (verification !== "none" && verification !== "size") {
    throw new Error("verification must be none or size");
  }
  const shardCount = value.shardCount ?? defaultShardCount;
  if (typeof shardCount !== "number" || !allowedShardCounts.has(shardCount)) {
    throw new Error("shardCount must be a power-of-two value from 16 through 1024");
  }
  return { destination, conflictPolicy, verification, shardCount };
}

export function parseSelectors(value: unknown): FileSelector[] {
  if (!isRecord(value) || !Array.isArray(value.selectors)) {
    throw new Error("selectors must be an array");
  }
  if (value.selectors.length < 1 || value.selectors.length > 1_000) {
    throw new Error("each request must contain between 1 and 1000 selectors");
  }
  return value.selectors.map((entry, index) => {
    if (!isRecord(entry) || (entry.type !== "key" && entry.type !== "prefix")) {
      throw new Error(`selectors[${index}].type must be key or prefix`);
    }
    if (typeof entry.value !== "string") {
      throw new Error(`selectors[${index}].value must be a string`);
    }
    if (entry.type === "key" && entry.value.length === 0) {
      throw new Error(`selectors[${index}].value cannot be empty for a key`);
    }
    if (new TextEncoder().encode(entry.value).byteLength > 1_024) {
      throw new Error(`selectors[${index}].value exceeds the R2 key limit of 1024 bytes`);
    }
    return { type: entry.type, value: entry.value };
  });
}

export function parseSchedule(value: unknown): string {
  if (!isRecord(value) || typeof value.at !== "string") throw new Error("at must be an ISO-8601 timestamp");
  const timestamp = new Date(value.at);
  if (!Number.isFinite(timestamp.getTime())) throw new Error("at must be an ISO-8601 timestamp");
  if (timestamp.getTime() <= Date.now()) throw new Error("at must be in the future");
  return timestamp.toISOString();
}

function parseDestination(value: Record<string, unknown>): Destination {
  const provider = requiredString(value.provider, "destination.provider");
  const bucket = requiredString(value.bucket, "destination.bucket");
  if (provider === "s3") {
    return {
      provider,
      bucket,
      region: requiredString(value.region, "destination.region"),
      endpoint: optionalString(value.endpoint, "destination.endpoint"),
    };
  }
  if (provider === "gcs") {
    return { provider, bucket };
  }
  throw new Error("destination.provider must be s3 or gcs");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(value: unknown, field: string): string {
  const parsed = optionalString(value, field);
  if (parsed === undefined || parsed.length === 0) throw new Error(`${field} is required`);
  return parsed;
}

function optionalString(value: unknown, field: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new Error(`${field} must be a string`);
  return value.trim();
}
