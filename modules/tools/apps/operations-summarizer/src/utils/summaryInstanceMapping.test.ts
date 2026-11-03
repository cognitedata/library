import type { NodeDefinition } from "@cognite/sdk";
import { describe, expect, it } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import {
  dedupeSummaryInstancesByNodeId,
  encodeTestBaselineSelectionKey,
  firstSummaryInstanceOrNull,
  mapNodesToSummaryInstances,
  nodeToSummaryInstance,
  resolveSummaryFromTestSelection,
  summaryInstanceNodeKey,
} from "./summaryInstanceMapping";

function makeNode(overrides: Partial<NodeDefinition> = {}): NodeDefinition {
  return {
    instanceType: "node",
    externalId: "n1",
    space: "s1",
    createdTime: 1000,
    version: 1,
    ...overrides,
  } as NodeDefinition;
}

describe("summaryInstanceMapping", () => {
  it("nodeToSummaryInstance maps fields", () => {
    const n = makeNode({
      externalId: "x",
      space: "sp",
      createdTime: 42,
      // @ts-expect-error Narrow fixture; DMS view property map is wider than literal inference
      properties: { a: { b: 1 } },
    });
    expect(nodeToSummaryInstance(n)).toEqual({
      externalId: "x",
      space: "sp",
      createdTime: 42,
      properties: { a: { b: 1 } },
    });
  });

  it("mapNodesToSummaryInstances filters non-nodes and maps", () => {
    const items = [
      makeNode({ externalId: "a" }),
      { instanceType: "edge", externalId: "e" },
      null,
      makeNode({ externalId: "b" }),
    ];
    const out = mapNodesToSummaryInstances(items);
    expect(out.map((x) => x.externalId)).toEqual(["a", "b"]);
  });

  it("mapNodesToSummaryInstances handles nullish input", () => {
    expect(mapNodesToSummaryInstances(undefined)).toEqual([]);
    expect(mapNodesToSummaryInstances(null)).toEqual([]);
  });

  it("firstSummaryInstanceOrNull returns first node or null", () => {
    expect(firstSummaryInstanceOrNull([])).toBeNull();
    expect(
      firstSummaryInstanceOrNull([{ foo: 1 }, makeNode({ externalId: "z" })])
        ?.externalId,
    ).toBe("z");
  });

  it("summaryInstanceNodeKey combines space and externalId", () => {
    const s: SummaryInstance = {
      space: "sp",
      externalId: "e1",
      createdTime: 1,
      properties: {},
    };
    expect(summaryInstanceNodeKey(s)).toBe("sp\0e1");
  });

  it("dedupeSummaryInstancesByNodeId keeps first occurrence", () => {
    const a: SummaryInstance = {
      space: "s",
      externalId: "dup",
      createdTime: 1,
      properties: {},
    };
    const b: SummaryInstance = { ...a, createdTime: 2 };
    expect(dedupeSummaryInstancesByNodeId([a, b])).toEqual([a]);
  });

  it("encodeTestBaselineSelectionKey round-trips via resolveSummaryFromTestSelection", () => {
    const s: SummaryInstance = {
      space: "sp_x",
      externalId: "ext_y",
      createdTime: 1,
      properties: {},
    };
    const key = encodeTestBaselineSelectionKey(s);
    expect(resolveSummaryFromTestSelection([s], key)).toBe(s);
  });

  it("resolveSummaryFromTestSelection falls back to externalId-only key", () => {
    const s: SummaryInstance = {
      space: "sp",
      externalId: "legacy",
      createdTime: 1,
      properties: {},
    };
    expect(resolveSummaryFromTestSelection([s], "legacy")).toBe(s);
  });
});
