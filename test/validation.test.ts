import { describe, expect, it, vi } from "vitest";

import { parseCreateJob, parseSchedule, parseSelectors } from "../src/validation";

describe("parseCreateJob", () => {
  it("applies safe defaults for an S3 job", () => {
    expect(
      parseCreateJob(
        { destination: { provider: "s3", bucket: "archive", region: "us-west-2" } },
        64,
      ),
    ).toEqual({
      destination: { provider: "s3", bucket: "archive", region: "us-west-2", endpoint: undefined },
      conflictPolicy: "skip",
      verification: "size",
      shardCount: 64,
    });
  });

  it("rejects unsupported conflict policies and shard counts", () => {
    expect(() =>
      parseCreateJob(
        {
          destination: { provider: "gcs", bucket: "archive" },
          conflictPolicy: "fail",
        },
        64,
      ),
    ).toThrow("conflictPolicy must be skip or overwrite");
    expect(() =>
      parseCreateJob(
        { destination: { provider: "gcs", bucket: "archive" }, shardCount: 63 },
        64,
      ),
    ).toThrow("shardCount must be a power-of-two value");
  });
});

describe("parseSelectors", () => {
  it("accepts exact keys, prefixes, and an empty whole-bucket prefix", () => {
    expect(
      parseSelectors({
        selectors: [
          { type: "key", value: "one.txt" },
          { type: "prefix", value: "invoices/" },
          { type: "prefix", value: "" },
        ],
      }),
    ).toEqual([
      { type: "key", value: "one.txt" },
      { type: "prefix", value: "invoices/" },
      { type: "prefix", value: "" },
    ]);
  });

  it("bounds each upload while allowing repeatable uploads", () => {
    const selectors = Array.from({ length: 1_001 }, (_, index) => ({
      type: "key",
      value: `${index}.txt`,
    }));
    expect(() => parseSelectors({ selectors })).toThrow("between 1 and 1000 selectors");
  });

  it("enforces the R2 UTF-8 key-size limit", () => {
    expect(() =>
      parseSelectors({ selectors: [{ type: "key", value: "é".repeat(513) }] }),
    ).toThrow("exceeds the R2 key limit");
  });
});

describe("parseSchedule", () => {
  it("normalizes future ISO-8601 timestamps", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-17T00:00:00Z"));
    expect(parseSchedule({ at: "2026-10-01T02:00:00Z" })).toBe("2026-10-01T02:00:00.000Z");
    expect(() => parseSchedule({ at: "2026-09-16T23:59:59Z" })).toThrow("at must be in the future");
    vi.useRealTimers();
  });
});
