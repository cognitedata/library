import type { Edge, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";
import { liftSubgraphInnerToParentWorkflow, subgraphHasLiftableInnerContent } from "./liftSubgraphInnerToParent";

describe("liftSubgraphInnerToParentWorkflow", () => {
  it("inverts collapse: restores outer nodes, inner edges, and crossing edges", () => {
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

    const lifted = liftSubgraphInnerToParentWorkflow(collapsed!.nodes, collapsed!.edges, sg.id, "lr");
    expect(lifted).not.toBeNull();

    const ids = new Set(lifted!.nodes.map((n) => n.id));
    expect(ids.has("s")).toBe(true);
    expect(ids.has("a")).toBe(true);
    expect(ids.has("b")).toBe(true);
    expect(ids.has("c")).toBe(true);
    expect(ids.has(sg.id)).toBe(false);

    const has = (src: string, tgt: string) =>
      lifted!.edges.some((e) => e.source === src && e.target === tgt);
    expect(has("s", "a")).toBe(true);
    expect(has("a", "b")).toBe(true);
    expect(has("b", "c")).toBe(true);

    const na = lifted!.nodes.find((n) => n.id === "a")!;
    const nb = lifted!.nodes.find((n) => n.id === "b")!;
    expect(na.position.x).toBeCloseTo(100, 0);
    expect(na.position.y).toBeCloseTo(200, 0);
    expect(nb.position.x).toBeCloseTo(350, 0);
    expect(nb.position.y).toBeCloseTo(250, 0);
  });
});
