import { describe, expect, it } from "vitest";
import type { WorkflowCanvasNode } from "../types/workflowCanvas";
import {
  canvasNodeDisplayLabel,
  canvasNodeKindLabel,
  canvasNodeMatchesSearch,
  filterCanvasNodesBySearch,
} from "./workflowCanvasFlowSearch";

function node(partial: Partial<WorkflowCanvasNode> & Pick<WorkflowCanvasNode, "id" | "kind">): WorkflowCanvasNode {
  return {
    position: { x: 0, y: 0 },
    data: {},
    ...partial,
  };
}

describe("workflowCanvasFlowSearch", () => {
  it("matches label, kind, and config description", () => {
    const n = node({
      id: "vq_asset",
      kind: "query_view",
      data: {
        label: "Asset Query",
        config: { view_external_id: "CogniteAsset", description: "discovery list" },
      },
    });
    expect(canvasNodeKindLabel(n)).toBe("View query");
    expect(canvasNodeDisplayLabel(n)).toBe("Asset Query");
    expect(canvasNodeMatchesSearch(n, "transform")).toBe(false);
    expect(canvasNodeMatchesSearch(n, "view query")).toBe(true);
    expect(canvasNodeMatchesSearch(n, "CogniteAsset")).toBe(true);
  });

  it("filterCanvasNodesBySearch returns matches only", () => {
    const nodes = [
      node({ id: "a", kind: "transform", data: { label: "Tag Based" } }),
      node({ id: "b", kind: "query_view", data: { label: "Asset Query" } }),
    ];
    expect(filterCanvasNodesBySearch(nodes, "asset")).toHaveLength(1);
    expect(filterCanvasNodesBySearch(nodes, "asset")[0]?.id).toBe("b");
  });
});
