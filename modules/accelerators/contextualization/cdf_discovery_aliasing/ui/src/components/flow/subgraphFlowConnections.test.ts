import type { Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { subflowTargetHandleForPort, subflowSourceHandleForPort } from "../../types/workflowCanvas";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { isValidKeaFlowConnection } from "./subgraphFlowConnections";
import { keaValidationRuleLayoutRfTypes } from "./flowConstants";

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

  it("rejects outer sourceView into subgraph in when port targets inner keaViewQuery", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "keaViewQuery" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const sv = node("sv", "keaSourceView", {});
    const nodes = new Map([["sv", sv], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "sv",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(false);
  });

  it("allows outer start into subgraph in when port targets inner keaViewQuery", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "A", inner_target_rf_type: "keaViewQuery" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const st = node("st", "keaStart", {});
    const nodes = new Map([["st", st], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "st",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("allows outer viewQuery into subgraph in when port targets inner keaJoin", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "L", inner_target_rf_type: "keaJoin" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const vq = node("vq", "keaViewQuery", {});
    const nodes = new Map([["vq", vq], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "vq",
      target: "sg",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("in"),
    });
    expect(ok).toBe(true);
  });

  it("allows outer transform into subgraph in when port targets inner keaJoin", () => {
    const sg = node("sg", "keaSubgraph", {
      subflow_ports: {
        inputs: [{ id: "in", label: "R", inner_target_rf_type: "keaJoin" }],
        outputs: [{ id: "out", label: "out" }],
      },
    });
    const tf = node("tf", "keaTransform", {});
    const nodes = new Map([["tf", tf], ["sg", sg]]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "tf",
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

  it("allows inner keaJoin into subgraph graph-out when port declares inner_source keaJoin", () => {
    const go = node("go", "keaSubflowGraphOut", {
      subflow_ports: {
        inputs: [],
        outputs: [{ id: "jout", label: "joined", inner_source_rf_type: "keaJoin" }],
      },
    });
    const jn = node("jn", "keaJoin", {});
    const nodes = new Map<string, Node>([
      ["jn", jn],
      ["go", go],
    ]);
    const getNode = (id: string) => nodes.get(id);
    const ok = isValidKeaFlowConnection(getNode, {
      source: "jn",
      target: "go",
      sourceHandle: "out",
      targetHandle: subflowTargetHandleForPort("jout"),
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

  it("rejects discovery validate validation handle → match rule; allows out → extraction-context rule", () => {
    const va = node("va", "keaDiscoveryValidate", { label: "V" });
    const ruleEx = node("rule_ex", "keaMatchValidationRuleExtraction", { label: "R" });
    const ruleAl = node("rule_al", "keaMatchValidationRuleAliasing", { label: "R2" });
    const nodes = new Map<string, Node>([
      ["va", va],
      ["rule_ex", ruleEx],
      ["rule_al", ruleAl],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "va",
        target: "rule_ex",
        sourceHandle: "validation",
        targetHandle: "in",
      })
    ).toBe(false);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "va",
        target: "rule_ex",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(true);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "va",
        target: "rule_al",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
  });

  it("rejects discovery validate out → transform", () => {
    const va = node("va", "keaDiscoveryValidate", {});
    const tf = node("tf", "keaTransform", {});
    const nodes = new Map<string, Node>([
      ["va", va],
      ["tf", tf],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, {
        source: "va",
        target: "tf",
        sourceHandle: "out",
        targetHandle: "in",
      })
    ).toBe(false);
  });

  it("allows persistence nodes out → keaEnd only (rejects query, transform, validate, join, save, inverted index)", () => {
    const save = node("sv", "keaViewSave", {});
    const rawSave = node("sr", "keaRawSave", {});
    const classicSave = node("sc", "keaClassicSave", {});
    const ap = node("ap", "keaAliasPersistence", {});
    const end = node("end", "keaEnd", {});
    const vq = node("vq", "keaViewQuery", {});
    const tf = node("tf", "keaTransform", {});
    const va = node("va", "keaDiscoveryValidate", {});
    const jn = node("jn", "keaJoin", {});
    const save2 = node("s2", "keaRawSave", {});
    const inv = node("inv", "keaInvertedIndex", {});
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
          isValidKeaFlowConnection(getNode, {
            source: src,
            target: tgt,
            sourceHandle: "out",
            targetHandle: "in",
          })
        ).toBe(false);
      }
      expect(
        isValidKeaFlowConnection(getNode, {
          source: src,
          target: "end",
          sourceHandle: "out",
          targetHandle: "in",
        })
      ).toBe(true);
    }
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

  it("canvas compile mode: start may wire only to query discovery nodes", () => {
    const start = node("st", "keaStart", {});
    const vq = node("vq", "keaViewQuery", {});
    const sv = node("sv", "keaSourceView", {});
    const nodes = new Map<string, Node>([
      ["st", start],
      ["vq", vq],
      ["sv", sv],
    ]);
    const getNode = (id: string) => nodes.get(id);
    expect(
      isValidKeaFlowConnection(getNode, { source: "st", target: "vq", sourceHandle: "out", targetHandle: "in" }, keaValidationRuleLayoutRfTypes, "canvas")
    ).toBe(true);
    expect(
      isValidKeaFlowConnection(getNode, { source: "st", target: "sv", sourceHandle: "out", targetHandle: "in" }, keaValidationRuleLayoutRfTypes, "canvas")
    ).toBe(false);
  });

  it("rejects source view, transform, or query as upstream of a discovery query (only start allowed)", () => {
    const sv = node("sv", "keaSourceView", {});
    const tf = node("tf", "keaTransform", {});
    const qPrev = node("qp", "keaViewQuery", {});
    const st = node("st", "keaStart", {});
    const vq = node("vq", "keaViewQuery", {});
    const rq = node("rq", "keaRawQuery", {});
    const cq = node("cq", "keaClassicQuery", {});
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
        isValidKeaFlowConnection(getNode, { source: "sv", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(false);
      expect(
        isValidKeaFlowConnection(getNode, { source: "tf", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(false);
      expect(
        isValidKeaFlowConnection(getNode, { source: "qp", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(false);
      expect(
        isValidKeaFlowConnection(getNode, { source: "st", target: q, sourceHandle: "out", targetHandle: "in" })
      ).toBe(true);
    }
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
