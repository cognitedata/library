import type { Edge, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import {
  emptyWorkflowCanvasDocument,
  subflowSourceHandleForPort,
  subflowTargetHandleForPort,
  type WorkflowCanvasEdge,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { adoptNodesIntoSubgraph } from "./adoptNodesIntoSubgraph";

describe("adoptNodesIntoSubgraph", () => {
  it("moves members into inner_canvas and rewires crossing edges through subgraph ports and hubs", () => {
    const inner = emptyWorkflowCanvasDocument();
    const g: Node = {
      id: "g",
      type: "keaSubgraph",
      position: { x: 0, y: 0 },
      data: {
        label: "G",
        subflow_ports: {
          inputs: [{ id: "in", label: "in" }],
          outputs: [{ id: "out", label: "out" }],
        },
        inner_canvas: inner,
      },
    };
    const s: Node = {
      id: "s",
      type: "keaSourceView",
      position: { x: -300, y: 40 },
      data: { label: "S", ref: { view_external_id: "v1" } },
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 40, y: 30 },
      data: { label: "A" },
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      position: { x: 80, y: 50 },
      data: { label: "B" },
    };
    const c: Node = {
      id: "c",
      type: "keaExtraction",
      position: { x: 300, y: 40 },
      data: { label: "C" },
    };
    const nodes: Node[] = [s, g, a, b, c];
    const edges: Edge[] = [
      { id: "e_sa", source: "s", target: "a" },
      { id: "e_ab", source: "a", target: "b" },
      { id: "e_bc", source: "b", target: "c" },
    ];

    const res = adoptNodesIntoSubgraph(nodes, edges, new Set(["a", "b"]), "g", "lr");
    expect(res).not.toBeNull();

    expect(res!.nodes.some((n) => n.id === "a")).toBe(false);
    expect(res!.nodes.some((n) => n.id === "b")).toBe(false);
    expect(res!.nodes.find((n) => n.id === "s")).toBeDefined();
    expect(res!.nodes.find((n) => n.id === "c")).toBeDefined();
    const sg = res!.nodes.find((n) => n.id === "g");
    expect(sg).toBeDefined();

    const toG = res!.edges.find((e) => e.target === "g");
    expect(toG?.source).toBe("s");
    expect(toG?.targetHandle).toBe(subflowTargetHandleForPort("in"));

    const fromG = res!.edges.find((e) => e.source === "g");
    expect(fromG?.target).toBe("c");
    expect(fromG?.sourceHandle).toBe(subflowSourceHandleForPort("out"));

    const wf = sg!.data as WorkflowCanvasNodeData;
    const ic = wf.inner_canvas;
    expect(ic?.nodes?.some((n) => n.id === "a")).toBe(true);
    expect(ic?.nodes?.some((n) => n.id === "b")).toBe(true);
    const hubIn = String((sg!.data as { subflow_hub_input_id?: string }).subflow_hub_input_id ?? "").trim();
    const hubOut = String((sg!.data as { subflow_hub_output_id?: string }).subflow_hub_output_id ?? "").trim();
    expect(hubIn.length).toBeGreaterThan(0);
    expect(hubOut.length).toBeGreaterThan(0);
    const innerEdges = (ic?.edges ?? []) as WorkflowCanvasEdge[];
    expect(innerEdges.some((e) => e.source === "a" && e.target === "b")).toBe(true);
    expect(innerEdges.some((e) => e.source === hubIn && e.target === "a")).toBe(true);
    expect(innerEdges.some((e) => e.source === "b" && e.target === hubOut)).toBe(true);
  });

  it("assigns one subgraph input port per distinct member inbound socket; labels follow inner nodes", () => {
    const inner = emptyWorkflowCanvasDocument();
    const g: Node = {
      id: "g",
      type: "keaSubgraph",
      position: { x: 0, y: 0 },
      data: {
        label: "G",
        subflow_ports: {
          inputs: [{ id: "in", label: "in" }],
          outputs: [{ id: "out", label: "out" }],
        },
        inner_canvas: inner,
      },
    };
    const s1: Node = {
      id: "s1",
      type: "keaSourceView",
      position: { x: -400, y: 0 },
      data: { label: "S1", ref: { view_external_id: "v1" } },
    };
    const s2: Node = {
      id: "s2",
      type: "keaSourceView",
      position: { x: -400, y: 120 },
      data: { label: "S2", ref: { view_external_id: "v2" } },
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 40, y: 20 },
      data: { label: "A" },
    };
    const b: Node = {
      id: "b",
      type: "keaExtraction",
      position: { x: 40, y: 100 },
      data: { label: "B" },
    };
    const nodes: Node[] = [s1, s2, g, a, b];
    const edges: Edge[] = [
      { id: "e_s1a", source: "s1", target: "a" },
      { id: "e_s2b", source: "s2", target: "b" },
    ];

    const res = adoptNodesIntoSubgraph(nodes, edges, new Set(["a", "b"]), "g", "lr");
    expect(res).not.toBeNull();
    const sg = res!.nodes.find((n) => n.id === "g")!;
    const wf = sg.data as WorkflowCanvasNodeData;
    const inputs = wf.subflow_ports?.inputs ?? [];
    expect(inputs.length).toBe(2);
    const inIds = new Set(inputs.map((p) => p.id));
    expect(inIds.has("in")).toBe(true);
    expect(new Set(inputs.map((p) => p.label))).toEqual(new Set(["A", "B"]));

    const toG = res!.edges.filter((e) => e.target === "g");
    expect(toG.length).toBe(2);
    const handles = new Set(toG.map((e) => e.targetHandle));
    expect(handles.size).toBe(2);

    const hubIn = String(wf.subflow_hub_input_id ?? "").trim();
    const innerEdges = (wf.inner_canvas?.edges ?? []) as WorkflowCanvasEdge[];
    const fromHub = innerEdges.filter((e) => e.source === hubIn);
    expect(fromHub.length).toBe(2);
    const sh = new Set(fromHub.map((e) => e.source_handle ?? ""));
    expect(sh.size).toBe(2);
  });

  it("assigns one subgraph output port per distinct member outbound socket; labels follow inner nodes", () => {
    const inner = emptyWorkflowCanvasDocument();
    const g: Node = {
      id: "g",
      type: "keaSubgraph",
      position: { x: 0, y: 0 },
      data: {
        label: "G",
        subflow_ports: {
          inputs: [{ id: "in", label: "in" }],
          outputs: [{ id: "out", label: "out" }],
        },
        inner_canvas: inner,
      },
    };
    const s: Node = {
      id: "s",
      type: "keaSourceView",
      position: { x: -300, y: 40 },
      data: { label: "S", ref: { view_external_id: "v1" } },
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 40, y: 30 },
      data: { label: "A" },
    };
    const b: Node = {
      id: "b",
      type: "keaExtraction",
      position: { x: 40, y: 100 },
      data: { label: "B" },
    };
    const c1: Node = {
      id: "c1",
      type: "keaAliasing",
      position: { x: 300, y: 20 },
      data: { label: "C1" },
    };
    const c2: Node = {
      id: "c2",
      type: "keaAliasing",
      position: { x: 300, y: 120 },
      data: { label: "C2" },
    };
    const nodes: Node[] = [s, g, a, b, c1, c2];
    const edges: Edge[] = [
      { id: "e_sa", source: "s", target: "a" },
      { id: "e_sb", source: "s", target: "b" },
      { id: "e_ac1", source: "a", target: "c1" },
      { id: "e_bc2", source: "b", target: "c2" },
    ];

    const res = adoptNodesIntoSubgraph(nodes, edges, new Set(["a", "b"]), "g", "lr");
    expect(res).not.toBeNull();
    const sg = res!.nodes.find((n) => n.id === "g")!;
    const wf = sg.data as WorkflowCanvasNodeData;
    const outputs = wf.subflow_ports?.outputs ?? [];
    expect(outputs.length).toBe(2);
    const outIds = new Set(outputs.map((p) => p.id));
    expect(outIds.has("out")).toBe(true);
    expect(new Set(outputs.map((p) => p.label))).toEqual(new Set(["A", "B"]));

    const fromG = res!.edges.filter((e) => e.source === "g");
    expect(fromG.length).toBe(2);
    const srcHandles = new Set(fromG.map((e) => e.sourceHandle));
    expect(srcHandles.size).toBe(2);

    const hubOut = String(wf.subflow_hub_output_id ?? "").trim();
    const innerEdges = (wf.inner_canvas?.edges ?? []) as WorkflowCanvasEdge[];
    const toHub = innerEdges.filter((e) => e.target === hubOut);
    expect(toHub.length).toBe(2);
    const th = new Set(toHub.map((e) => e.target_handle ?? ""));
    expect(th.size).toBe(2);
  });
});
