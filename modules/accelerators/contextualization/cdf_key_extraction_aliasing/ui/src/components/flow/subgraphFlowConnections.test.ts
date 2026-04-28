import type { Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { subflowTargetHandleForPort, subflowSourceHandleForPort } from "../../types/workflowCanvas";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { isValidKeaFlowConnection } from "./subgraphFlowConnections";

function node(id: string, type: string, data?: Partial<WorkflowCanvasNodeData>): Node {
  return {
    id,
    type,
    position: { x: 0, y: 0 },
    data: (data ?? {}) as Record<string, unknown>,
  };
}

describe("isValidKeaFlowConnection subgraph ports with inner peer types", () => {
  it("allows outer sourceView into subgraph in when port targets inner keaExtraction", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "keaExtraction" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const sv = node("sv", "keaSourceView", { label: "S" });
    const nodes = new Map([["sg", sg], ["sv", sv]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "sv",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("rejects outer end into subgraph in when port only accepts inner keaExtraction", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "keaExtraction" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const end = node("end", "keaEnd", { label: "E" });
    const nodes = new Map([["sg", sg], ["end", end]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "end",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(false);
  });

  it("allows subgraph out to end when port originates from inner keaExtraction", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "in" }],
        outputs: [{ id: "out", label: "B", inner_source_rf_type: "keaExtraction" }],
      },
    });
    const end = node("end", "keaEnd", {});
    const nodes = new Map<string, Node>([
      ["sg", sg],
      ["end", end],
    ]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "sg",
      target: "end",
      sourceHandle: subflowSourceHandleForPort("out"),
      targetHandle: "in",
    });
    expect(ok).toBe(true);
  });

  it("rejects subgraph out to sourceView when port originates from inner keaExtraction", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "in" }],
        outputs: [{ id: "out", label: "B", inner_source_rf_type: "keaExtraction" }],
      },
    });
    const sv = node("sv", "keaSourceView", {});
    const nodes = new Map<string, Node>([
      ["sg", sg],
      ["sv", sv],
    ]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "sg",
      target: "sv",
      sourceHandle: subflowSourceHandleForPort("out"),
      targetHandle: "in",
    });
    expect(ok).toBe(false);
  });

  it("rejects extraction out → match validation rule; allows extraction validation handle → rule", () => {
    const ext = node("ext", "keaExtraction", { label: "E" });
    const rule = node("rule", "keaMatchValidationRuleExtraction", { label: "R" });
    const nodes = new Map<string, Node>([
      ["ext", ext],
      ["rule", rule],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "ext",
        target: "rule",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "ext",
        target: "rule",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(true);
  });

  it("rejects aliasing out → match validation rule; allows validation handle → rule", () => {
    const al = node("al", "keaAliasing", { label: "A" });
    const rule = node("rule", "keaMatchValidationRuleAliasing", { label: "R" });
    const nodes = new Map<string, Node>([
      ["al", al],
      ["rule", rule],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "al",
        target: "rule",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "al",
        target: "rule",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(true);
  });

  it("still allows extraction out → aliasing", () => {
    const ext = node("ext", "keaExtraction", {});
    const al = node("al", "keaAliasing", {});
    const nodes = new Map<string, Node>([
      ["ext", ext],
      ["al", al],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "ext",
        target: "al",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(true);
  });

  it("allows source view out → match validation rule (source_view context)", () => {
    const sv = node("sv", "keaSourceView", {});
    const vr = node("vr", "keaMatchValidationRuleSourceView", {});
    const nodes = new Map<string, Node>([
      ["sv", sv],
      ["vr", vr],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "sv",
        target: "vr",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(true);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "sv",
        target: "vr",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(false);
  });
});
