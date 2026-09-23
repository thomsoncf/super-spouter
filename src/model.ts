export type Destination =
  | {
      provider: "s3";
      bucket: string;
      region: string;
      endpoint?: string;
    }
  | {
      provider: "gcs";
      bucket: string;
    };

export type ConflictPolicy = "skip" | "overwrite";
export type VerificationMode = "none" | "size";
export type JobState = "pending" | "running" | "succeeded" | "failed" | "canceled";

export interface JobSpec {
  destination: Destination;
  conflictPolicy: ConflictPolicy;
  verification: VerificationMode;
  shardCount: number;
}

export interface JobRecord {
  id: string;
  state: JobState;
  spec: JobSpec;
  createdAt: string;
  startedAt?: string;
  endedAt?: string;
  error?: string;
  scheduledAt?: string;
  pagesScanned: number;
  objectsDiscovered: number;
  listingComplete: boolean;
  selectorCount: number;
}

export type SelectorType = "key" | "prefix";

export interface FileSelector {
  type: SelectorType;
  value: string;
}

export interface StoredSelector extends FileSelector {
  id: number;
}

export interface ObjectDescriptor {
  key: string;
  version: string;
  size: number;
  etag: string;
  uploaded: string;
  httpMetadata?: R2HTTPMetadata;
  customMetadata?: Record<string, string>;
}

export interface ScanMessage {
  kind: "scan";
  taskId: string;
  jobId: string;
  page: number;
  selector: StoredSelector;
  cursor?: string;
}

export interface CopyMessage {
  kind: "copy";
  taskId: string;
  jobId: string;
  shard: number;
  object: ObjectDescriptor;
}

export interface FinalizeMessage {
  kind: "finalize";
  taskId: string;
  jobId: string;
}

export type TransferMessage = ScanMessage | CopyMessage | FinalizeMessage;

export type TransferStatus =
  | "queued"
  | "running"
  | "retrying"
  | "succeeded"
  | "failed"
  | "skipped";

export interface CheckpointRegistration {
  taskId: string;
  objectKey: string;
  size: number;
}

export interface CheckpointSummary {
  queued: number;
  running: number;
  retrying: number;
  succeeded: number;
  failed: number;
  skipped: number;
  bytesTransferred: number;
}

export interface JobStatus extends JobRecord {
  transfers: CheckpointSummary;
}
