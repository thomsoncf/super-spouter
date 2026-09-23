import type { Env } from "../env";
import { PermanentTransferError } from "../errors";
import type { Destination, ObjectDescriptor } from "../model";
import type { PutResult } from "./s3";

type GCSDestination = Extract<Destination, { provider: "gcs" }>;

let cachedToken: { value: string; expiresAt: number; identity: string } | undefined;

export async function gcsExists(destination: GCSDestination, key: string, env: Env): Promise<boolean> {
  const token = await gcsToken(env);
  const response = await fetch(gcsMetadataURL(destination.bucket, key), {
    headers: { authorization: `Bearer ${token}` },
  });
  if (response.status === 404) {
    await response.body?.cancel();
    return false;
  }
  if (!response.ok) throw new Error(`GCS object lookup failed with ${response.status}: ${await response.text()}`);
  await response.body?.cancel();
  return true;
}

export async function putGCS(
  destination: GCSDestination,
  object: ObjectDescriptor,
  body: ReadableStream,
  env: Env,
): Promise<PutResult> {
  const token = await gcsToken(env);
  const url = new URL(`https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(destination.bucket)}/o`);
  url.searchParams.set("uploadType", "media");
  url.searchParams.set("name", object.key);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-length": object.size.toString(),
      "content-type": object.httpMetadata?.contentType ?? "application/octet-stream",
    },
    body,
  });
  if (!response.ok) {
    const message = `GCS upload failed with ${response.status}: ${await response.text()}`;
    if (response.status >= 400 && response.status < 500 && response.status !== 408 && response.status !== 429) {
      throw new PermanentTransferError(message);
    }
    throw new Error(message);
  }
  let metadata = (await response.json()) as { size?: string; etag?: string };

  if (Object.keys(object.customMetadata ?? {}).length > 0) {
    const patch = await fetch(gcsMetadataURL(destination.bucket, object.key), {
      method: "PATCH",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ metadata: object.customMetadata }),
    });
    if (!patch.ok) throw new Error(`GCS metadata update failed with ${patch.status}: ${await patch.text()}`);
    metadata = (await patch.json()) as { size?: string; etag?: string };
  }
  return { size: Number.parseInt(metadata.size ?? "-1", 10), etag: metadata.etag };
}

function gcsMetadataURL(bucket: string, key: string): string {
  return `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(key)}`;
}

async function gcsToken(env: Env): Promise<string> {
  if (!env.GCS_CLIENT_EMAIL || !env.GCS_PRIVATE_KEY) {
    throw new PermanentTransferError("GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY secrets are required");
  }
  if (
    cachedToken &&
    cachedToken.identity === env.GCS_CLIENT_EMAIL &&
    cachedToken.expiresAt > Date.now() + 60_000
  ) {
    return cachedToken.value;
  }

  const now = Math.floor(Date.now() / 1_000);
  const header = base64URL(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const payload = base64URL(
    JSON.stringify({
      iss: env.GCS_CLIENT_EMAIL,
      scope: "https://www.googleapis.com/auth/devstorage.read_write",
      aud: "https://oauth2.googleapis.com/token",
      iat: now,
      exp: now + 3_600,
    }),
  );
  const unsigned = `${header}.${payload}`;
  const privateKey = await crypto.subtle.importKey(
    "pkcs8",
    pemToArrayBuffer(env.GCS_PRIVATE_KEY.replaceAll("\\n", "\n")),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    privateKey,
    new TextEncoder().encode(unsigned),
  );
  const assertion = `${unsigned}.${base64URL(signature)}`;
  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }),
  });
  if (!response.ok) throw new PermanentTransferError(`GCS token exchange failed with ${response.status}`);
  const token = (await response.json()) as { access_token?: string; expires_in?: number };
  if (!token.access_token) throw new PermanentTransferError("GCS token response did not contain access_token");
  cachedToken = {
    value: token.access_token,
    expiresAt: Date.now() + (token.expires_in ?? 3_600) * 1_000,
    identity: env.GCS_CLIENT_EMAIL,
  };
  return cachedToken.value;
}

function pemToArrayBuffer(pem: string): ArrayBuffer {
  const base64 = pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\s/g, "");
  const bytes = Uint8Array.from(atob(base64), (char) => char.charCodeAt(0));
  return bytes.buffer;
}

function base64URL(value: string | ArrayBuffer): string {
  const bytes =
    typeof value === "string" ? new TextEncoder().encode(value) : new Uint8Array(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}
