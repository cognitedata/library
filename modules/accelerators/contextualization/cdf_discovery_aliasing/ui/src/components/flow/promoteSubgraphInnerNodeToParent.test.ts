import type { Edge, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";
import { subgraphHasLiftableInnerContent } from "./liftSubgraphInnerToParent";
import { canPromoteInnerSubtreeToOwningGraph, promoteSubgraphInnerSubtreeToParentWorkflow } from "./promoteSubgraphInnerNodeToParent";
import { canvasToFlowNodes } from "./flowDocumentBridge";
import { SUBFLOW_PORT_HANDLE_IN_PREFIX, type WorkflowCanvasNodeData } from "../../types/workflowCanvas";

describe("promoteSubgraphInnerSubtreeToParentWorkflow", () => {
  it("moves one inner node to the parent graph and shrinks inner_canvas", () => {
    const s: Node = {
      id: "s",
      type: "keaSourceView",
      position: { x: 0, y: 0 },
      data: { label: "S", ref: { view_external_id: "v1" } },
      selected: false,
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 100, y: 200 },
      data: { label: "A" },
      selected: true,
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      position: { x: 350, y: 250 },
      data: { label: "B" },
      selected: true,
    };
    const c: Node = {
      id: "c",
      type: "keaExtraction",
      position: { x: 520, y: 200 },
      data: { label: "C" },
      selected: false,
    };
    const nodes: Node[] = [s, a, b, c];
    const edges: Edge[] = [
      { id: "e_sa", source: "s", target: "a" },
      { id: "e_ab", source: "a", target: "b" },
      { id: "e_bc", source: "b", target: "c" },
    ];

    const collapsed = collapseSelectionToSubgraph(nodes, edges, [a, b], "lr");
    expect(collapsed).not.toBeNull();
    const sg = collapsed!.nodes.find((n) => n.type === "keaSubgraph")!;
    expect(subgraphHasLiftableInnerContent(collapsed!.nodes, sg.id)).toBe(true);

    const innerDoc = (sg.data as WorkflowCanvasNodeData).inner_canvas!;
    const innerRf = canvasToFlowNodes(innerDoc.nodes);
    const data = sg.data as WorkflowCanvasNodeData;
    const hubIn = String(data.subflow_hub_input_id ?? "").trim();
    const hubOut = String(data.subflow_hub_output_id ?? "").trim();
    expect(hubIn.length).toBeGreaterThan(0);
    expect(hubOut.length).toBeGreaterThan(0);
    expect(canPromoteInnerSubtreeToOwningGraph(innerRf, hubIn, hubOut, "a")).toBe(true);

    const promoted = promoteSubgraphInnerSubtreeToParentWorkflow(collapsed!.nodes, collapsed!.edges, sg.id, "a", "lr");
    expect(promoted).not.toBeNull();

    const ids = new Set(promoted!.nodes.map((n) => n.id));
    expect(ids.has("s")).toBe(true);
    expect(ids.has("a")).toBe(true);
    expect(ids.has("b")).toBe(false);
    expect(ids.has("c")).toBe(true);
    expect(ids.has(sg.id)).toBe(true);

    const sg2 = promoted!.nodes.find((n) => n.id === sg.id)!;
    const inner2 = (sg2.data as WorkflowCanvasNodeData).inner_canvas!;
    const innerIds = new Set(inner2.nodes.map((n) => n.id));
    expect(innerIds.has("a")).toBe(false);
    expect(innerIds.has("b")).toBe(true);
    expect(innerIds.has(hubIn)).toBe(true);
    expect(innerIds.has(hubOut)).toBe(true);

    const hasOuterPromotedToFrame = promoted!.edges.some(
      (e) => e.source === "a" && e.target === sg.id && String(e.targetHandle ?? "").startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX)
    );
    expect(hasOuterPromotedToFrame).toBe(true);
    const innerEdges2 = inner2.edges ?? [];
    const hasHubInToB = innerEdges2.some((e) => e.source === hubIn && e.target === "b");
    expect(hasHubInToB).toBe(true);
  });
});
