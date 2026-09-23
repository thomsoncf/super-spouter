# Super Spouter

Super Spouter is a self-hosted Cloudflare Worker that copies selected objects
from the deployer's R2 bucket to Amazon S3 or Google Cloud Storage. It does not
integrate with the Cloudflare dashboard or control plane.

The customer deploys every component in their own Cloudflare account:

- a Worker exposing the migration API and consuming transfer work;
- a Cloudflare Queue plus dead-letter queue;
- SQLite-backed Durable Objects for jobs and sharded checkpoints;
- an R2 bucket binding for the source;
- Worker secrets for API authentication and destination credentials.

Workers KV is intentionally excluded from the correctness path. See
[the architecture decision](docs/architecture.md) for the limits and scaling
analysis.

The GitLab investigation and diagram of the original inward migration system
are captured in [the Super Slurper reference architecture](docs/super-slurper-reference.md).

## Current scope

Implemented:

- exact-key and prefix selectors, uploaded in repeatable batches;
- overlapping-selector deduplication using deterministic task IDs;
- `skip` and `overwrite` conflict policies;
- immediate and one-time scheduled starts using Durable Object alarms;
- paginated R2 listings, Queue retries, durable checkpoints, and finalization;
- streaming S3 and GCS uploads without buffering object bodies;
- size verification and aggregated job status.

Objects above `MAX_SINGLE_PUT_BYTES` currently fail explicitly. S3 multipart
and GCS resumable sessions are the next milestone; the default single-upload
guard is 512 MiB.

## Deploy

```bash
npm install
npx wrangler queues create super-spouter-transfers
npx wrangler queues create super-spouter-transfers-dlq
```

Replace `replace-with-source-bucket` in `wrangler.jsonc`, then set an API token
and the credentials for the destinations the deployment will use:

```bash
npx wrangler secret put API_TOKEN
npx wrangler secret put S3_ACCESS_KEY_ID
npx wrangler secret put S3_SECRET_ACCESS_KEY
npx wrangler secret put GCS_CLIENT_EMAIL
npx wrangler secret put GCS_PRIVATE_KEY
```

```bash
npm run check
npm run deploy
```

The first deployment creates the SQLite-backed Durable Object classes through
the `v1` migration in `wrangler.jsonc`.

## Use

The migration flow is deliberately explicit:

1. `POST /v1/jobs` creates a pending job.
2. Repeatedly call `POST /v1/jobs/{id}/selectors` with exact keys and/or
   prefixes. The aggregate selector count is not limited to one request.
3. Call `POST /v1/jobs/{id}/start`, or schedule it with
   `POST /v1/jobs/{id}/schedule`.
4. Read `GET /v1/jobs/{id}` for strongly consistent aggregate progress.

See [API.md](docs/API.md) for complete request schemas, examples, selection
semantics, limits, and status fields.

## Development

```bash
npm install
npm run check
npm run dev
```

## License

MIT.
