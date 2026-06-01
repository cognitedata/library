import { describe, expect, it } from "vitest";
import {
  mergeDataModelingTriggerIntoStart,
  readDataModelingTriggerFromStart,
} from "./dataModelingTriggerConfigModel";

describe("dataModelingTriggerConfigModel", () => {
  it("round-trips typed fields into trigger_rule", () => {
    const merged = mergeDataModelingTriggerIntoStart(
      { trigger_type: "dataModeling" },
      {
        batchSize: 200,
        batchTimeout: 120,
        dataModelingQueryText: JSON.stringify({ with: {}, select: {} }),
      }
    );
    const fields = readDataModelingTriggerFromStart(merged);
    expect(fields.batchSize).toBe(200);
    expect(fields.batchTimeout).toBe(120);
    expect(fields.dataModelingQueryText).toContain("\"with\"");
    const rule = merged.trigger_rule as Record<string, unknown>;
    expect(rule.triggerType).toBe("dataModeling");
    expect(rule.batchSize).toBe(200);
    expect(rule.dataModelingQuery).toBeDefined();
  });
});
