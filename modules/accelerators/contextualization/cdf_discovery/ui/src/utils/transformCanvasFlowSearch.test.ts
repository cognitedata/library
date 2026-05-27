import { describe, expect, it } from "vitest";
import type { MessageKey } from "../i18n/types";
import type { TransformCanvasNode } from "../types/transformCanvas";
import {
  filterTransformCanvasNodesBySearch,
  formatTransformCanvasNodeLabelWithId,
  transformCanvasNodeMatchesSearch,
} from "./transformCanvasFlowSearch";

function node(id: string, kind: TransformCanvasNode["kind"], data: TransformCanvasNode["data"]): TransformCanvasNode {
  return { id, kind, position: { x: 0, y: 0 }, data };
}

const t = (key: MessageKey) => {
  const labels: Partial<Record<MessageKey, string>> = {
    "transform.palette.query_view": "View query",
    "transform.palette.merge": "Merge",
    "transform.canvas.defaultEndLabel": "End",
  };
  return labels[key] ?? key;
};

describe("transformCanvasFlowSearch", () => {
  it("matches label, kind, and config fields", () => {
    const n = node("q1", "query_view", {
      label: "Assets",
      config: { view_external_id: "CogniteAsset", view_space: "sp_dm" },
    });
    expect(transformCanvasNodeMatchesSearch(n, "asset", t)).toBe(true);
    expect(transformCanvasNodeMatchesSearch(n, "view query", t)).toBe(true);
    expect(transformCanvasNodeMatchesSearch(n, "CogniteAsset", t)).toBe(true);
    expect(transformCanvasNodeMatchesSearch(n, "merge", t)).toBe(false);
  });

  it("formatTransformCanvasNodeLabelWithId includes id in brackets when label differs", () => {
    const n = node("q1", "query_view", { label: "CogniteAsset (v1)" });
    expect(formatTransformCanvasNodeLabelWithId(n, t)).toBe("CogniteAsset (v1) (q1)");
    expect(formatTransformCanvasNodeLabelWithId(node("only_id", "end", {}), t)).toBe("End (only_id)");
  });

  it("filterTransformCanvasNodesBySearch returns matches only", () => {
    const nodes = [
      node("a", "transform", { label: "Alpha" }),
      node("b", "merge", { label: "Beta merge" }),
    ];
    expect(filterTransformCanvasNodesBySearch(nodes, "merge", t)).toHaveLength(1);
    expect(filterTransformCanvasNodesBySearch(nodes, "merge", t)[0]?.id).toBe("b");
  });
});
