import { describe, expect, it } from "vitest";

import { checkpointName, checkpointShard, sha256Hex } from "../src/hash";

describe("checkpoint hashing", () => {
  it("is deterministic and remains inside the configured shard range", async () => {
    const first = await checkpointShard("job-1", "media/photo.jpg", 64);
    const second = await checkpointShard("job-1", "media/photo.jpg", 64);
    expect(second).toBe(first);
    expect(first).toBeGreaterThanOrEqual(0);
    expect(first).toBeLessThan(64);
  });

  it("uses unambiguous separators when deriving task IDs", async () => {
    expect(await sha256Hex("ab", "c")).not.toBe(await sha256Hex("a", "bc"));
    expect(await sha256Hex("job", "key")).toHaveLength(64);
  });

  it("produces stable Durable Object names", () => {
    expect(checkpointName("job-1", 15)).toBe("job-1:000f");
  });
});
