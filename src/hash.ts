const encoder = new TextEncoder();

export async function sha256Hex(...parts: string[]): Promise<string> {
  const input = encoder.encode(parts.join("\u0000"));
  const digest = await crypto.subtle.digest("SHA-256", input);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
export async function checkpointShard(jobId: string, objectKey: string, shardCount: number): Promise<number> {
  const input = encoder.encode(`${jobId}\u0000${objectKey}`);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", input));
  const value = new DataView(digest.buffer).getUint32(0, false);
  return value % shardCount;
}

export function checkpointName(jobId: string, shard: number): string {
  return `${jobId}:${shard.toString(16).padStart(4, "0")}`;
}
