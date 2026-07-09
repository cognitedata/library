import { describe, expect, it } from "vitest";
import { DISCOVERY_MODULES } from "../modules/discoveryModules";
import { INDEX_ROOT } from "../utils/treeNodeIds";

describe("discovery shell modules", () => {
  it("registers modules in connection-root order", () => {
    const ids = DISCOVERY_MODULES.map((m) => m.treeRootId);
    expect(ids).toEqual([
      "data",
      "fusion",
      "gov",
      "extract",
      "transform",
      INDEX_ROOT,
      "monitor",
      "settings",
    ]);
  });

  it("places indexing after transform", () => {
    const transformIdx = DISCOVERY_MODULES.findIndex((m) => m.id === "transform");
    const indexIdx = DISCOVERY_MODULES.findIndex((m) => m.id === "inverted_index");
    expect(transformIdx).toBeGreaterThanOrEqual(0);
    expect(indexIdx).toBeGreaterThan(transformIdx);
  });
});
