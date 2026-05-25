import { describe, expect, it } from "vitest";
import {
  buildIndexSummary,
  indexKindPairCount,
  indexKindsStructuredEditable,
  indexKindsToConfig,
  metadataIndexKeyPreset,
  parseIndexKindsJson,
  rowsFromIndexKinds,
} from "./buildIndexNodeConfigModel";

describe("buildIndexNodeConfigModel", () => {
  it("parses metadata indexKey preset", () => {
    const rows = rowsFromIndexKinds({ metadata: ["indexKey"] });
    expect(rows).toEqual([{ kind: "metadata", properties: ["indexKey"] }]);
    expect(indexKindsToConfig(rows)).toEqual({ metadata: ["indexKey"] });
    expect(buildIndexSummary({ index_kinds: { metadata: ["indexKey"] } })).toBe("metadata:indexKey");
    expect(indexKindPairCount({ index_kinds: { metadata: ["indexKey"] } })).toBe(1);
  });

  it("accepts structured editable shapes only", () => {
    expect(indexKindsStructuredEditable({ metadata: ["indexKey"] })).toBe(true);
    expect(indexKindsStructuredEditable([])).toBe(false);
    expect(indexKindsStructuredEditable({ metadata: "indexKey" })).toBe(false);
  });

  it("parses JSON text", () => {
    expect(parseIndexKindsJson('{"metadata":["indexKey"]}')).toEqual([metadataIndexKeyPreset()]);
    expect(parseIndexKindsJson("{")).toBe(null);
  });
});
