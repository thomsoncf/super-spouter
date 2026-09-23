# HTTP API

The deployed Worker is the complete customer-facing API. There is no dependency
on the Cloudflare dashboard or control plane beyond normal deployment and
resource provisioning.

Set `API_TOKEN` with `wrangler secret put`. When configured, every request must
include:

```http
Authorization: Bearer <API_TOKEN>
```

All responses use JSON. Job IDs are UUIDs returned by the create operation.

## Select files

Selection is a separate, repeatable API operation so a migration can contain an
effectively unlimited number of exact keys and prefixes without exceeding
Workers request-body or Queue message limits.

Each selector upload accepts 1–1,000 entries. Call it as many times as needed
while the job is pending. Selectors are stored as rows in the job's
SQLite-backed Durable Object and duplicates are ignored.

```http
POST /v1/jobs/{jobId}/selectors
Content-Type: application/json
```

```json
{
  "selectors": [
    { "type": "key", "value": "invoices/2026/0001.pdf" },
    { "type": "key", "value": "invoices/2026/0002.pdf" },
    { "type": "prefix", "value": "exports/customer-42/" },
    { "type": "prefix", "value": "media/originals/" }
  ]
}
```

Selector behavior:

- `key` selects exactly one R2 object. Missing keys produce no copy task.
- `prefix` paginates every key beginning with the supplied string.
- An empty prefix selects the entire bound R2 bucket.
- Selectors may overlap. A deterministic task ID derived from job, key, object
  version, and destination deduplicates the resulting copy.
- An object changed after listing is rejected rather than copying a different
  version silently.
- R2 keys and prefixes are limited to 1,024 UTF-8 bytes by the platform.
- Selectors become immutable when the job starts or is scheduled to start.

“Unlimited” means there is no application-level total selector count. Uploads
are chunked across requests and stored as individual SQLite rows. The physical
ceiling remains the Durable Object's 10 GB storage limit; extremely large jobs
can be split into multiple jobs before reaching it.

## Create a job

```http
POST /v1/jobs
Content-Type: application/json
```

S3 destination:

```json
{
  "destination": {
    "provider": "s3",
    "bucket": "archive-bucket",
    "region": "us-west-2"
  },
  "conflictPolicy": "skip",
  "verification": "size",
  "shardCount": 64
}
```

GCS destination:

```json
{
  "destination": {
    "provider": "gcs",
    "bucket": "archive-bucket"
  },
  "conflictPolicy": "overwrite",
  "verification": "size",
  "shardCount": 128
}
```

Fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `destination.provider` | yes | `s3` or `gcs` |
| `destination.bucket` | yes | Destination bucket name |
| `destination.region` | S3 only | AWS signing region |
| `destination.endpoint` | no | Path-style S3-compatible endpoint override |
| `conflictPolicy` | no | `skip` (default) or `overwrite` |
| `verification` | no | `size` (default) or `none` |
| `shardCount` | no | 16, 32, 64, 128, 256, 512, or 1,024; default 64 |

`skip` checks the destination before reading R2 and records the object as
skipped when it exists. `overwrite` uploads unconditionally using the same key.

The response is a pending job. Raw destination credentials are never accepted
by this endpoint; they come from secrets in the customer's Worker deployment.

## Start immediately

At least one selector must exist.

```http
POST /v1/jobs/{jobId}/start
```

The job coordinator transitions to `running`, then uses a durable SQLite outbox
to publish selector scans to the Queue.

## Schedule a start

Schedules are one-time ISO-8601 timestamps and are implemented with the job's
Durable Object alarm. At least one selector must exist.

```http
POST /v1/jobs/{jobId}/schedule
Content-Type: application/json
```

```json
{
  "at": "2026-10-01T02:00:00Z"
}
```

Submitting a new future timestamp while the job is still pending replaces the
previous schedule. Dynamic recurring cron rules are intentionally not part of
v1; callers can create and schedule one job per run, or a future version can
have an alarm create recurring run records.

## Read status

```http
GET /v1/jobs/{jobId}
```

Example:

```json
{
  "id": "f1394d14-fc44-47ec-899c-690d984a9db3",
  "state": "running",
  "selectorCount": 24000,
  "pagesScanned": 18,
  "objectsDiscovered": 17642,
  "listingComplete": false,
  "transfers": {
    "queued": 1050,
    "running": 122,
    "retrying": 4,
    "succeeded": 16012,
    "failed": 1,
    "skipped": 453,
    "bytesTransferred": 9182736455
  }
}
```

The API reads all checkpoint-shard summaries in groups of six to respect the
Worker connection limit. This is strongly consistent but costs one Durable
Object request per shard, so status polling should normally be measured in
seconds rather than milliseconds.

## Health

```http
GET /healthz
```
