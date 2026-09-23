import { describe, expect, it } from "vitest";

import type { FinalizeMessage } from "../src/model";
import { makeQueueBatches } from "../src/queue-batches";

function message(index: number, padding = ""): FinalizeMessage {
  return {
    kind: "finalize",
    taskId: `finalize:${index}:${padding}`,
    jobId: "job-1",
  };
}

describe("makeQueueBatches", () => {
  it("respects the message-count limit", () => {
    const batches = makeQueueBatches(Array.from({ length: 205 }, (_, index) => message(index)));
    expect(batches.map((batch) => batch.length)).toEqual([100, 100, 5]);
  });

  it("respects the encoded byte budget", () => {
    const batches = makeQueueBatches([message(1, "x".repeat(400)), message(2, "x".repeat(400))], 100, 700);
    expect(batches).toHaveLength(2);
  });

  it("rejects an oversized queue message", () => {
    expect(() => makeQueueBatches([message(1, "x".repeat(127_000))])).toThrow("is too large");
  });
});
