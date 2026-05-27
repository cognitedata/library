import { describe, expect, it } from "vitest";
import {
  mergeRecordStreamTriggerIntoStart,
  readRecordStreamTriggerFromStart,
} from "./recordStreamTriggerConfigModel";

describe("recordStreamTriggerConfigModel", () => {
  it("round-trips typed fields into trigger_rule", () => {
    const merged = mergeRecordStreamTriggerIntoStart(
      { trigger_type: "recordStream" },
      {
        streamExternalId: "stream-a",
        batchSize: 200,
        batchTimeout: 120,
        filter: { and: [] },
        sources: [{ space: "sp", externalId: "view" }],
      }
    );
    const fields = readRecordStreamTriggerFromStart(merged);
    expect(fields.streamExternalId).toBe("stream-a");
    expect(fields.batchSize).toBe(200);
    expect(fields.batchTimeout).toBe(120);
    const rule = merged.trigger_rule as Record<string, unknown>;
    expect(rule.streamExternalId).toBe("stream-a");
    expect(rule.batchSize).toBe(200);
  });
});
