import type { Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { subflowTargetHandleForPort, subflowSourceHandleForPort } from "../../types/workflowCanvas";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { isValidDiscoveryFlowConnection } from "./subgraphFlowConnections";
import { discoveryValidationRuleLayoutRfTypes } from "./flowConstants";

function node(id: string, type: string, data?: Partial<WorkflowCanvasNodeData>): Node {
  return {
    id,
    type,
    position: { x: 0, y: 0 },
    data: (data ?? {}) as Record<string, unknown>,
  };
}

describe("isValidDiscoveryFlowConnection subgraph ports with inner peer types", () => {
  it("allows outer view query into subgraph in when port targets inner discoveryTransform", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "discoveryTransform" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const vq = node("vq", "discoveryViewQuery", { label: "Q" });
    const nodes = new Map([["sg", sg], ["vq", vq]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "vq",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("rejects outer sourceView into subgraph in when port targets inner discoveryViewQuery", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "discoveryViewQuery" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const sv = node("sv", "discoverySourceView", {});
    const nodes = new Map([["sv", sv], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "sv",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(false);
  });

  it("allows outer start into subgraph in when port targets inner discoveryViewQuery", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "discoveryViewQuery" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const st = node("st", "discoveryStart", {});
    const nodes = new Map([["st", st], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "st",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("allows outer viewQuery into subgraph in when port targets inner discoveryJoin", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "L", inner_target_rf_type: "discoveryJoin" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const vq = node("vq", "discoveryViewQuery", {});
    const nodes = new Map([["vq", vq], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "vq",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("allows outer transform into subgraph in when port targets inner discoveryJoin", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "R", inner_target_rf_type: "discoveryJoin" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const tf = node("tf", "discoveryTransform", {});
    const nodes = new Map([["tf", tf], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "tf",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("rejects outer end into subgraph in when port only accepts inner discoveryTransform", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "discoveryTransform" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const end = node("end", "discoveryEnd", { label: "E" });
    const nodes = new Map([["sg", sg], ["end", end]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "end",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(false);
  });

  it("allows subgraph out to end when port originates from inner discoveryTransform", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "in" }],
        outputs: [{ id: "out", label: "B", inner_source_rf_type: "discoveryTransform" }],
      },
    });
    const end = node("end", "discoveryEnd", {});
    const nodes = new Map<string, Node>([
      ["sg", sg],
      ["end", end],
    ]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "sg",
      target: "end",
      sourceHandle: subflowSourceHandleForPort("out"),
      targetHandle: "in",
    });
    expect(ok).toBe(true);
  });

  it("allows inner discoveryJoin into subgraph graph-out when port declares inner_source discoveryJoin", () => {
    const go = node("go", "discoverySubflowGraphOut", {
      subflow_ports: {
        inputs: [],
        outputs: [{ id: "jout", label: "joined", inner_source_rf_type: "discoveryJoin" }],
      },
    });
    const jn = node("jn", "discoveryJoin", {});
    const nodes = new Map<string, Node>([
      ["jn", jn],
      ["go", go],
    ]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "jn",
      target: "go",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("jout"),
    });
    expect(ok).toBe(true);
  });

  it("rejects subgraph out to sourceView when port originates from inner discoveryTransform", () => {
    const sg = node("sg", "discoverySubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "in" }],
        outputs: [{ id: "out", label: "B", inner_source_rf_type: "discoveryTransform" }],
      },
    });
    const sv = node("sv", "discoverySourceView", {});
    const nodes = new Map<string, Node>([
      ["sg", sg],
      ["sv", sv],
    ]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidDiscoveryFlowConnection(getNode, {
      source: "sg",
      target: "sv",
      sourceHandle: subflowSourceHandleForPort("out"),
      targetHandle: "in",
    });
    expect(ok).toBe(false);
  });

  it("rejects transform out → match validation rule; allows transform validation handle → rule", () => {
    const ext = node("ext", "discoveryTransform", { label: "E" });
    const rule = node("rule", "discoveryMatchValidationRuleExtraction", { label: "R" });
    const nodes = new Map<string, Node>([
      ["ext", ext],
      ["rule", rule],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "ext",
        target: "rule",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "ext",
        target: "rule",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(true);
  });

  it("rejects transform out → aliasing match validation rule; allows validation handle → rule", () => {
    const al = node("al", "discoveryTransform", { label: "A" });
    const rule = node("rule", "discoveryMatchValidationRuleAliasing", { label: "R" });
    const nodes = new Map<string, Node>([
      ["al", al],
      ["rule", rule],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "al",
        target: "rule",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "al",
        target: "rule",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(true);
  });

  it("rejects discovery validate validation handle → match rule; allows out → extraction-context rule", () => {
    const va = node("va", "discoveryValidate", { label: "V" });
    const ruleEx = node("rule_ex", "discoveryMatchValidationRuleExtraction", { label: "R" });
    const ruleAl = node("rule_al", "discoveryMatchValidationRuleAliasing", { label: "R2" });
    const nodes = new Map<string, Node>([
      ["va", va],
      ["rule_ex", ruleEx],
      ["rule_al", ruleAl],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "va",
        target: "rule_ex",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(false);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "va",
        target: "rule_ex",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(true);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "va",
        target: "rule_al",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
  });

  it("rejects discovery validate out → transform", () => {
    const va = node("va", "discoveryValidate", {});
    const tf = node("tf", "discoveryTransform", {});
    const nodes = new Map<string, Node>([
      ["va", va],
      ["tf", tf],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "va",
        target: "tf",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
  });

  it("allows persistence nodes out → discoveryEnd only (rejects query, transform, validate, join, save, inverted index)", () => {
    const save = node("sv", "discoveryViewSave", {});
    const rawSave = node("sr", "discoveryRawSave", {});
    const classicSave = node("sc", "discoveryClassicSave", {});
    const ap = node("ap", "discoveryAliasPersistence", {});
    const end = node("end", "discoveryEnd", {});
    const vq = node("vq", "discoveryViewQuery", {});
    const tf = node("tf", "discoveryTransform", {});
    const va = node("va", "discoveryValidate", {});
    const jn = node("jn", "discoveryJoin", {});
    const save2 = node("s2", "discoveryRawSave", {});
    const inv = node("inv", "discoveryInvertedIndex", {});
    const nodes = new Map<string, Node>([
      ["sv", save],
      ["sr", rawSave],
      ["sc", classicSave],
      ["ap", ap],
      ["end", end],
      ["vq", vq],
      ["tf", tf],
      ["va", va],
      ["jn", jn],
      ["s2", save2],
      ["inv", inv],
    ]);
    const getNode = (id: string) => nodes.get(id);
    for (const src of ["sv", "sr", "sc", "ap", "inv"] as const) {
      for (const tgt of ["sv", "vq", "tf", "va", "jn", "s2", "inv", "ap"] as const) {
        expect(
          isValidDiscoveryFlowConnection(getNode, {
            source: src,
            target: tgt,
            sourceHandle: "out",
            targetHandle: "in",
          })
        ).toBe(false);
      }
      expect(
        isValidDiscoveryFlowConnection(getNode, {
          source: src,
          target: "end",
          sourceHandle: "out",
          targetHandle: "in",
        })
      ).toBe(true);
    }
  });

  it("still allows extraction out → aliasing", () => {
    const ext = node("ext", "discoveryTransform", {});
    const al = node("al", "discoveryTransform", {});
    const nodes = new Map<string, Node>([
      ["ext", ext],
      ["al", al],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "ext",
        target: "al",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(true);
  });

  it("canvas compile mode: start may wire only to query discovery nodes", () => {
    const start = node("st", "discoveryStart", {});
    const vq = node("vq", "discoveryViewQuery", {});
    const sv = node("sv", "discoverySourceView", {});
    const nodes = new Map<string, Node>([
      ["st", start],
      ["vq", vq],
      ["sv", sv],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, { source: "st", target: "vq", sourceHandle: "out", targetHandle: "in" }, discoveryValidationRuleLayoutRfTypes, "canvas")
    ).toBe(true);
    expect(
      isValidDiscoveryFlowConnection(getNode, { source: "st", target: "sv", sourceHandle: "out", targetHandle: "in" }, discoveryValidationRuleLayoutRfTypes, "canvas")
    ).toBe(false);
  });

  it("rejects source view, transform, or query as upstream of a discovery query (only start allowed)", () => {
    const sv = node("sv", "discoverySourceView", {});
    const tf = node("tf", "discoveryTransform", {});
    const qPrev = node("qp", "discoveryViewQuery", {});
    const st = node("st", "discoveryStart", {});
    const vq = node("vq", "discoveryViewQuery", {});
    const rq = node("rq", "discoveryRawQuery", {});
    const cq = node("cq", "discoveryClassicQuery", {});
    const nodes = new Map<string, Node>([
      ["sv", sv],
      ["tf", tf],
      ["qp", qPrev],
      ["st", st],
      ["vq", vq],
      ["rq", rq],
      ["cq", cq],
    ]);
    const getNode = (id: string) => nodes.get(id);
    for (const q of ["vq", "rq", "cq"] as const) {
      expect(
        isValidDiscoveryFlowConnection(getNode, { source: "sv", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(false);
      expect(
        isValidDiscoveryFlowConnection(getNode, { source: "tf", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(false);
      expect(
        isValidDiscoveryFlowConnection(getNode, { source: "qp", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(false);
      expect(
        isValidDiscoveryFlowConnection(getNode, { source: "st", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(true);
    }
  });

  it("allows source view out → match validation rule (source_view context)", () => {
    const sv = node("sv", "discoverySourceView", {});
    const vr = node("vr", "discoveryMatchValidationRuleSourceView", {});
    const nodes = new Map<string, Node>([
      ["sv", sv],
      ["vr", vr],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "sv",
        target: "vr",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(true);
    expect(
      isValidDiscoveryFlowConnection(getNode, {
        source: "sv",
        target: "vr",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(false);
  });
});
