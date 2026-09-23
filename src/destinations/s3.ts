import { AwsClient } from "aws4fetch";

import type { Env } from "../env";
import { PermanentTransferError } from "../errors";
import type { Destination, ObjectDescriptor } from "../model";

type S3Destination = Extract<Destination, { provider: "s3" }>;

export interface PutResult {
  size: number;
  etag?: string;
}

export async function s3Exists(destination: S3Destination, key: string, env: Env): Promise<boolean> {
  const client = s3Client(destination, env);
  const request = await client.sign(s3ObjectURL(destination, key), { method: "HEAD" });
  const response = await fetch(request);
  if (response.status === 404) {
    await response.body?.cancel();
    return false;
  }
  if (!response.ok) throw new Error(`S3 HeadObject failed with ${response.status}: ${await response.text()}`);
  await response.body?.cancel();
  return true;
}

export async function putS3(
  destination: S3Destination,
  object: ObjectDescriptor,
  body: ReadableStream,
  env: Env,
): Promise<PutResult> {
  const client = s3Client(destination, env);
  const headers = new Headers({
    "content-length": object.size.toString(),
    "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
  });
  applyHTTPMetadata(headers, object.httpMetadata);
  for (const [name, value] of Object.entries(object.customMetadata ?? {})) {
    headers.set(`x-amz-meta-${name}`, value);
  }
  const request = await client.sign(s3ObjectURL(destination, object.key), {
    method: "PUT",
    headers,
    body,
  });
  const response = await fetch(request);
  if (!response.ok) {
    const message = `S3 PutObject failed with ${response.status}: ${await response.text()}`;
    if (response.status >= 400 && response.status < 500 && response.status !== 408 && response.status !== 429) {
      throw new PermanentTransferError(message);
    }
    throw new Error(message);
  }

  const headRequest = await client.sign(s3ObjectURL(destination, object.key), { method: "HEAD" });
  const head = await fetch(headRequest);
  if (!head.ok) throw new Error(`S3 verification HeadObject failed with ${head.status}`);
  return {
    size: Number.parseInt(head.headers.get("content-length") ?? "-1", 10),
    etag: head.headers.get("etag") ?? undefined,
  };
}

function s3Client(destination: S3Destination, env: Env): AwsClient {
  if (!env.S3_ACCESS_KEY_ID || !env.S3_SECRET_ACCESS_KEY) {
    throw new PermanentTransferError("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY secrets are required");
  }
  return new AwsClient({
    accessKeyId: env.S3_ACCESS_KEY_ID,
    secretAccessKey: env.S3_SECRET_ACCESS_KEY,
    service: "s3",
    region: destination.region,
  });
}

function s3ObjectURL(destination: S3Destination, key: string): string {
  const encodedKey = key.split("/").map(encodeURIComponent).join("/");
  if (destination.endpoint) {
    return `${destination.endpoint.replace(/\/$/, "")}/${encodeURIComponent(destination.bucket)}/${encodedKey}`;
  }
  return `https://${destination.bucket}.s3.${destination.region}.amazonaws.com/${encodedKey}`;
}

function applyHTTPMetadata(headers: Headers, metadata: R2HTTPMetadata | undefined): void {
  if (!metadata) return;
  if (metadata.contentType) headers.set("content-type", metadata.contentType);
  if (metadata.contentLanguage) headers.set("content-language", metadata.contentLanguage);
  if (metadata.contentDisposition) headers.set("content-disposition", metadata.contentDisposition);
  if (metadata.contentEncoding) headers.set("content-encoding", metadata.contentEncoding);
  if (metadata.cacheControl) headers.set("cache-control", metadata.cacheControl);
  if (metadata.cacheExpiry) headers.set("expires", metadata.cacheExpiry.toUTCString());
}
