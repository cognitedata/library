import { describe, expect, it } from "vitest";
import type { TransformCanvasNode } from "../types/transformCanvas";
import {
  filterTransformCanvasNodesBySearch,
  formatTransformCanvasNodeLabelWithId,
  transformCanvasNodeMatchesSearch,
} from "./transformCanvasFlowSearch";

function node(id: string, kind: TransformCanvasNode["kind"], data: TransformCanvasNode["data"]): TransformCanvasNode {
  return { id, kind, position: { x: 0, y: 0 }, data };
}

describe("transformCanvasFlowSearch", () => {
  it("matches label, kind, and config fields", () => {
    const n = node("q1", "query_view", {
      label: "Assets",
      config: { view_external_id: "CogniteAsset", view_space: "sp_dm" },
    });
    expect(transformCanvasNodeMatchesSearch(n, "asset")).toBe(true);
    expect(transformCanvasNodeMatchesSearch(n, "view query")).toBe(true);
    expect(transformCanvasNodeMatchesSearch(n, "CogniteAsset")).toBe(true);
    expect(transformCanvasNodeMatchesSearch(n, "merge")).toBe(false);
  });

  it("formatTransformCanvasNodeLabelWithId includes id in brackets when label differs", () => {
    const n = node("q1", "query_view", { label: "CogniteAsset (v1)" });
    expect(formatTransformCanvasNodeLabelWithId(n)).toBe("CogniteAsset (v1) (q1)");
    expect(formatTransformCanvasNodeLabelWithId(node("only_id", "end", {}))).toBe("only_id");
  });

  it("filterTransformCanvasNodesBySearch returns matches only", () => {
    const nodes = [
      node("a", "transform", { label: "Alpha" }),
      node("b", "merge", { label: "Beta merge" }),
    ];
    expect(filterTransformCanvasNodesBySearch(nodes, "merge")).toHaveLength(1);
    expect(filterTransformCanvasNodesBySearch(nodes, "merge")[0]?.id).toBe("b");
  });
});
