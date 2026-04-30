import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import { layoutFlowNodes } from "./autoLayoutFlow";

const scopeStripFirst: Record<string, unknown> = {
  key_extraction: {
    config: {
      data: {
        extraction_rules: [
          {
            name: "ext_one",
            enabled: true,
            handler: "regex_handler",
            priority: 10,
            aliasing_pipeline: ["Strip Delimiter", "rule_a", "rule_b"],
          },
        ],
      },
    },
  },
  aliasing_rule_definitions: {
    "Strip Delimiter": {
      name: "Strip Delimiter",
      handler: "regex_substitution",
      enabled: true,
      priority: 5,
      config: { patterns: [] },
    },
  },
  aliasing: {
    config: {
      data: {
        aliasing_rules: [
          { name: "rule_a", enabled: true, handler: "character_substitution", priority: 10, config: {} },
          { name: "rule_b", enabled: true, handler: "character_substitution", priority: 20, config: {} },
        ],
      },
    },
  },
};

describe("layoutFlowNodes / subflow interior", () => {
  it("lays out children inside a subflow and resizes the frame", () => {
    const sf: Node = {
      id: "sf",
      type: "keaSubflow",
      position: { x: 0, y: 0 },
      data: { label: "Group" },
      style: { width: 400, height: 300 },
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      parentId: "sf",
      position: { x: 5, y: 5 },
      data: { label: "A", handler_id: "h1" },
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      parentId: "sf",
      position: { x: 5, y: 5 },
      data: { label: "B", handler_id: "h2" },
    };
    const edges: Edge[] = [
      { id: "e1", source: "a", target: "b", data: { kind: "data" } },
    ];
    const out = layoutFlowNodes([sf, a, b], edges, "lr");
    const sfn = out.find((n) => n.id === "sf")!;
    const w = (sfn.style as { width?: number }).width;
    const h = (sfn.style as { height?: number }).height;
    expect(typeof w).toBe("number");
    expect(typeof h).toBe("number");
    expect(w!).toBeGreaterThanOrEqual(200);
    expect(h!).toBeGreaterThanOrEqual(140);
    const na = out.find((n) => n.id === "a")!;
    const nb = out.find((n) => n.id === "b")!;
    expect(na.parentId).toBe("sf");
    expect(nb.parentId).toBe("sf");
    expect(na.position.x).toBeLessThan(nb.position.x);
  });

  it("orders same-layer keaAliasing by pipeline_rank, not node id", () => {
    const st: Node = { id: "st", type: "keaStart", position: { x: 0, y: 0 }, data: {} };
    const ext: Node = { id: "ext", type: "keaExtraction", position: { x: 0, y: 0 }, data: { label: "e" } };
    const alC: Node = {
      id: "al_cogniteasset_explicit_aliases_from_raw",
      type: "keaAliasing",
      position: { x: 0, y: 0 },
      data: { label: "c", pipeline_rank: 1 },
    };
    const alS: Node = {
      id: "al_strip_delimiter",
      type: "keaAliasing",
      position: { x: 0, y: 0 },
      data: { label: "s", pipeline_rank: 0 },
    };
    const alL: Node = {
      id: "al_leading_zero_normalization",
      type: "keaAliasing",
      position: { x: 0, y: 0 },
      data: { label: "l", pipeline_rank: 2 },
    };
    const en: Node = { id: "en", type: "keaEnd", position: { x: 0, y: 0 }, data: {} };
    const edges: Edge[] = [
      { id: "e0", source: "st", target: "ext", data: { kind: "data" } },
      { id: "e1", source: "ext", target: "al_cogniteasset_explicit_aliases_from_raw", data: { kind: "data" } },
      { id: "e2", source: "ext", target: "al_strip_delimiter", data: { kind: "data" } },
      { id: "e3", source: "ext", target: "al_leading_zero_normalization", data: { kind: "data" } },
      { id: "e4", source: "al_cogniteasset_explicit_aliases_from_raw", target: "en", data: { kind: "data" } },
      { id: "e5", source: "al_strip_delimiter", target: "en", data: { kind: "data" } },
      { id: "e6", source: "al_leading_zero_normalization", target: "en", data: { kind: "data" } },
    ];
    const out = layoutFlowNodes([st, ext, alC, alS, alL, en], edges, "lr");
    const pS = out.find((n) => n.id === "al_strip_delimiter")!.position.y;
    const pC = out.find((n) => n.id === "al_cogniteasset_explicit_aliases_from_raw")!.position.y;
    const pL = out.find((n) => n.id === "al_leading_zero_normalization")!.position.y;
    expect(pS).toBeLessThan(pC);
    expect(pC).toBeLessThan(pL);
  });

  it("infers same-layer keaAliasing order from workflow scope when pipeline_rank is absent", () => {
    const st: Node = { id: "st", type: "keaStart", position: { x: 0, y: 0 }, data: {} };
    const ext: Node = { id: "ext", type: "keaExtraction", position: { x: 0, y: 0 }, data: { label: "e" } };
    const alC: Node = {
      id: "al_cogniteasset_explicit_aliases_from_raw",
      type: "keaAliasing",
      position: { x: 0, y: 0 },
      data: { label: "c", ref: { aliasing_rule_name: "rule_a" } },
    };
    const alS: Node = {
      id: "al_strip_delimiter",
      type: "keaAliasing",
      position: { x: 0, y: 0 },
      data: { label: "s", ref: { aliasing_rule_name: "Strip Delimiter" } },
    };
    const alL: Node = {
      id: "al_leading_zero_normalization",
      type: "keaAliasing",
      position: { x: 0, y: 0 },
      data: { label: "l", ref: { aliasing_rule_name: "rule_b" } },
    };
    const en: Node = { id: "en", type: "keaEnd", position: { x: 0, y: 0 }, data: {} };
    const edges: Edge[] = [
      { id: "e0", source: "st", target: "ext", data: { kind: "data" } },
      { id: "e1", source: "ext", target: "al_cogniteasset_explicit_aliases_from_raw", data: { kind: "data" } },
      { id: "e2", source: "ext", target: "al_strip_delimiter", data: { kind: "data" } },
      { id: "e3", source: "ext", target: "al_leading_zero_normalization", data: { kind: "data" } },
      { id: "e4", source: "al_cogniteasset_explicit_aliases_from_raw", target: "en", data: { kind: "data" } },
      { id: "e5", source: "al_strip_delimiter", target: "en", data: { kind: "data" } },
      { id: "e6", source: "al_leading_zero_normalization", target: "en", data: { kind: "data" } },
    ];
    const out = layoutFlowNodes([st, ext, alC, alS, alL, en], edges, "lr", scopeStripFirst);
    const pS = out.find((n) => n.id === "al_strip_delimiter")!.position.y;
    const pC = out.find((n) => n.id === "al_cogniteasset_explicit_aliases_from_raw")!.position.y;
    const pL = out.find((n) => n.id === "al_leading_zero_normalization")!.position.y;
    expect(pS).toBeLessThan(pC);
    expect(pC).toBeLessThan(pL);
  });
});
