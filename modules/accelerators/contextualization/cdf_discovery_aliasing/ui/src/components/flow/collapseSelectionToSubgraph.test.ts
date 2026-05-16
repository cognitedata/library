import type { Edge, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { subflowSourceHandleForPort, subflowTargetHandleForPort } from "../../types/workflowCanvas";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";

describe("collapseSelectionToSubgraph", () => {
  it("returns null when no groupable selected nodes", () => {
    const n: Node = {
      id: "start",
      type: "keaStart",
      position: { x: 0, y: 0 },
      data: {},
      selected: true,
    };
    const out = collapseSelectionToSubgraph([n], [], [n], "lr");
    expect(out).toBeNull();
  });

  it("replaces selection with one keaSubgraph, preserves inner edges, rewires crossing edges to in/out", () => {
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

    const res = collapseSelectionToSubgraph(nodes, edges, [a, b], "lr");
    expect(res).not.toBeNull();

    const sg = res!.nodes.find((n) => n.type === "keaSubgraph");
    expect(sg).toBeDefined();
    expect(res!.nodes.find((n) => n.id === "a")).toBeUndefined();
    expect(res!.nodes.find((n) => n.id === "b")).toBeUndefined();
    expect(res!.nodes.find((n) => n.id === "s")).toBeDefined();
    expect(res!.nodes.find((n) => n.id === "c")).toBeDefined();

    const wf = sg!.data as WorkflowCanvasNodeData;
    const inner = wf.inner_canvas;
    const hubIn = String(wf.subflow_hub_input_id ?? "").trim();
    const hubOut = String(wf.subflow_hub_output_id ?? "").trim();
    expect(hubIn.length).toBeGreaterThan(0);
    expect(hubOut.length).toBeGreaterThan(0);
    expect(inner?.nodes?.length).toBe(4);
    expect(inner!.nodes!.some((n) => n.kind === "subflow_graph_in" && n.id === hubIn)).toBe(true);
    expect(inner!.nodes!.some((n) => n.kind === "subflow_graph_out" && n.id === hubOut)).toBe(true);

    expect(inner?.edges?.length).toBe(3);
    expect(inner!.edges!.some((e) => e.source === "a" && e.target === "b")).toBe(true);
    expect(inner!.edges!.some((e) => e.source === hubIn && e.target === "a")).toBe(true);
    expect(inner!.edges!.some((e) => e.source === "b" && e.target === hubOut)).toBe(true);

    const toSg = res!.edges.find((e) => e.target === sg!.id);
    expect(toSg?.source).toBe("s");
    expect(toSg?.targetHandle).toBe(subflowTargetHandleForPort("in"));

    const fromSg = res!.edges.find((e) => e.source === sg!.id);
    expect(fromSg?.target).toBe("c");
    expect(fromSg?.sourceHandle).toBe(subflowSourceHandleForPort("out"));

    const inPorts = wf.subflow_ports?.inputs ?? [];
    expect(inPorts[0]?.inner_target_rf_type).toBe("keaExtraction");
    const outPorts = wf.subflow_ports?.outputs ?? [];
    expect(outPorts[0]?.inner_source_rf_type).toBe("keaAliasing");
  });

  it("assigns one subgraph output port per distinct member outbound socket (source + handle)", () => {
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
      position: { x: 100, y: 40 },
      data: { label: "A" },
      selected: true,
    };
    const b: Node = {
      id: "b",
      type: "keaExtraction",
      position: { x: 100, y: 140 },
      data: { label: "B" },
      selected: true,
    };
    const c1: Node = {
      id: "c1",
      type: "keaAliasing",
      position: { x: 400, y: 20 },
      data: { label: "C1" },
      selected: false,
    };
    const c2: Node = {
      id: "c2",
      type: "keaAliasing",
      position: { x: 400, y: 160 },
      data: { label: "C2" },
      selected: false,
    };
    const nodes: Node[] = [s, a, b, c1, c2];
    const edges: Edge[] = [
      { id: "e_sa", source: "s", target: "a" },
      { id: "e_sb", source: "s", target: "b" },
      { id: "e_ac1", source: "a", target: "c1" },
      { id: "e_bc2", source: "b", target: "c2" },
    ];

    const res = collapseSelectionToSubgraph(nodes, edges, [a, b], "lr");
    expect(res).not.toBeNull();
    const sg = res!.nodes.find((n) => n.type === "keaSubgraph")!;
    const wf = sg.data as WorkflowCanvasNodeData;
    expect((wf.subflow_ports?.outputs ?? []).length).toBe(2);

    const fromSg = res!.edges.filter((e) => e.source === sg.id);
    expect(fromSg.length).toBe(2);
    expect(new Set(fromSg.map((e) => e.sourceHandle)).size).toBe(2);

    const hubOut = String(wf.subflow_hub_output_id ?? "").trim();
    const innerEdges = wf.inner_canvas?.edges ?? [];
    const toHub = innerEdges.filter((e) => e.target === hubOut);
    expect(toHub.length).toBe(2);
    expect(new Set(toHub.map((e) => e.target_handle ?? "")).size).toBe(2);
  });
});
