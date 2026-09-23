import type { Env } from "../env";
import type { Destination, ObjectDescriptor } from "../model";
import { gcsExists, putGCS } from "./gcs";
import { putS3, s3Exists, type PutResult } from "./s3";

export function destinationExists(destination: Destination, key: string, env: Env): Promise<boolean> {
  return destination.provider === "s3"
    ? s3Exists(destination, key, env)
    : gcsExists(destination, key, env);
}
export function putDestination(
  destination: Destination,
  object: ObjectDescriptor,
  body: ReadableStream,
  env: Env,
): Promise<PutResult> {
  return destination.provider === "s3"
    ? putS3(destination, object, body, env)
    : putGCS(destination, object, body, env);
}
