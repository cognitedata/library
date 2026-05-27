import { describe, expect, it } from "vitest";
import {
  readReadMode,
  readStreamExternalId,
  recordsQuerySummary,
  validateRecordsQueryConfig,
} from "./recordsQueryConfigModel";

describe("recordsQueryConfigModel", () => {
  it("validates stream required", () => {
    const v = validateRecordsQueryConfig({});
    expect(v.ok).toBe(false);
    expect(v.issues).toContain("transform.query.recordsErrorStreamRequired");
  });

  it("reads sync mode and builds summary", () => {
    const cfg = {
      stream_external_id: "my-stream",
      read_mode: "filter",
      sources: [{ space: "s", externalId: "c" }],
    };
    expect(readStreamExternalId(cfg)).toBe("my-stream");
    expect(readReadMode(cfg)).toBe("filter");
    expect(recordsQuerySummary(cfg)).toContain("my-stream");
    expect(recordsQuerySummary(cfg)).toContain("1 source");
  });
});
