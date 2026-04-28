import type { Edge, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";
import {
  convertSubflowToSubgraph,
  convertSubgraphToSubflow,
  subflowCanConvertToSubgraph,
} from "./subflowSubgraphConvert";
import { wrapSelectionInNewSubflow } from "./wrapSelectionInSubflow";

describe("subflowSubgraphConvert", () => {
  it("convertSubflowToSubgraph removes frame and yields one keaSubgraph with inner members", () => {
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 40, y: 60 },
      data: { label: "A" },
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      position: { x: 200, y: 80 },
      data: { label: "B" },
    };
    const wrapped = wrapSelectionInNewSubflow([a, b], [a, b]);
    expect(wrapped).not.toBeNull();
    const sf = wrapped!.find((n) => n.type === "keaSubflow");
    expect(sf).toBeDefined();
    expect(subflowCanConvertToSubgraph(wrapped!, sf!.id)).toBe(true);

    const edges: Edge[] = [{ id: "e_ab", source: "a", target: "b" }];
    const res = convertSubflowToSubgraph(wrapped!, edges, sf!.id, "lr");
    expect(res).not.toBeNull();
    expect(res!.nodes.some((n) => n.type === "keaSubflow")).toBe(false);
    const sg = res!.nodes.find((n) => n.type === "keaSubgraph");
    expect(sg).toBeDefined();
    expect(res!.nodes.some((n) => n.id === "a")).toBe(false);
    expect(res!.nodes.some((n) => n.id === "b")).toBe(false);
    const inner = (sg!.data as { inner_canvas?: { nodes?: { id: string }[] } }).inner_canvas;
    expect(inner?.nodes?.some((n) => n.id === "a")).toBe(true);
    expect(inner?.nodes?.some((n) => n.id === "b")).toBe(true);
  });

  it("convertSubgraphToSubflow yields keaSubflow with former inner members", () => {
    const s: Node = {
      id: "s",
      type: "keaSourceView",
      position: { x: 0, y: 0 },
      data: { label: "S", ref: { view_external_id: "v1" } },
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
    const nodes: Node[] = [s, a, b];
    const edges: Edge[] = [
      { id: "e_sa", source: "s", target: "a" },
      { id: "e_ab", source: "a", target: "b" },
    ];
    const collapsed = collapseSelectionToSubgraph(nodes, edges, [a, b], "lr");
    expect(collapsed).not.toBeNull();
    const sg = collapsed!.nodes.find((n) => n.type === "keaSubgraph")!;
    const res = convertSubgraphToSubflow(collapsed!.nodes, collapsed!.edges, sg.id, "lr");
    expect(res).not.toBeNull();
    expect(res!.nodes.some((n) => n.type === "keaSubgraph")).toBe(false);
    const sf = res!.nodes.find((n) => n.type === "keaSubflow");
    expect(sf).toBeDefined();
    const childA = res!.nodes.find((n) => n.id === "a");
    expect(childA?.parentId).toBe(sf!.id);
    const childB = res!.nodes.find((n) => n.id === "b");
    expect(childB?.parentId).toBe(sf!.id);
  });
});
