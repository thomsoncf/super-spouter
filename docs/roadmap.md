# Roadmap

## Large objects

- Add S3 create/upload-part/complete/abort operations.
- Persist the upload ID and per-part ETags in the object's checkpoint shard.
- Add GCS resumable-session URL and confirmed byte offset to the checkpoint.
- Split long transfers into Queue messages bounded below the 15-minute consumer
  duration.

## Scheduling and selection

- Add job cancellation and selector deletion while pending.
- Add recurring schedules that create immutable run records.
- Add optional destination-key rewriting without weakening deduplication.
- Add manifest upload from R2 for billions of selectors.

## Operations and security

- Partition tenants or regions across multiple queues when needed.
- Add per-destination rate-limit Durable Objects.
- Export metrics with Workers Analytics Engine or Logpush.
- Add retention alarms to delete old checkpoint shards.
- Replace deployment-wide destination credentials with scoped, short-lived
  federation where supported.
- Add KV only as a disposable job-discovery index if list/search becomes useful.
