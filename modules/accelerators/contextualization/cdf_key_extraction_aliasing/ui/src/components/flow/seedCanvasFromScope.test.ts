import { describe, expect, it } from "vitest";
import { seedCanvasFromScope } from "./seedCanvasFromScope";

describe("seedCanvasFromScope — structural layout from rules (not YAML validation / pipeline)", () => {
  it("chains aliasing nodes by sorted aliasing_rules order and links extraction to the first aliasing node", () => {
    const scope: Record<string, unknown> = {
      associations: [
        {
          kind: "source_view_to_extraction",
          source_view_index: 0,
          extraction_rule_name: "ext_one",
        },
      ],
      source_views: [{ view_external_id: "v1", entity_type: "asset" }],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                scope_filters: { entity_type: ["asset"] },
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              {
                name: "alpha_alias",
                enabled: true,
                handler: "regex_substitution",
                priority: 10,
                config: { patterns: [] },
              },
              {
                name: "beta_alias",
                enabled: true,
                handler: "regex_substitution",
                priority: 20,
                config: { patterns: [] },
              },
            ],
          },
        },
      },
    };

    const doc = seedCanvasFromScope(scope);
    const extOne = doc.nodes.find((n) => n.kind === "extraction");
    const alAlpha = doc.nodes.find(
      (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name === "alpha_alias"
    );
    const alBeta = doc.nodes.find(
      (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name === "beta_alias"
    );
    expect(extOne).toBeDefined();
    expect(alAlpha).toBeDefined();
    expect(alBeta).toBeDefined();
    expect(
      doc.edges.some((e) => e.kind === "data" && e.source === extOne!.id && e.target === alAlpha!.id)
    ).toBe(true);
    const seq = doc.edges.filter((e) => e.kind === "sequence" && e.source === alAlpha!.id && e.target === alBeta!.id);
    expect(seq.length).toBeGreaterThanOrEqual(1);
  });

  it("orders three aliasing rules by priority then name (sequence edges)", () => {
    const scope: Record<string, unknown> = {
      associations: [
        {
          kind: "source_view_to_extraction",
          source_view_index: 0,
          extraction_rule_name: "ext_one",
        },
      ],
      source_views: [{ view_external_id: "v1", entity_type: "asset" }],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                scope_filters: { entity_type: ["asset"] },
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              {
                name: "step_one",
                enabled: true,
                handler: "regex_substitution",
                priority: 10,
                config: { patterns: [] },
              },
              {
                name: "step_two",
                enabled: true,
                handler: "regex_substitution",
                priority: 20,
                config: { patterns: [] },
              },
              {
                name: "step_three",
                enabled: true,
                handler: "regex_substitution",
                priority: 30,
                config: { patterns: [] },
              },
            ],
          },
        },
      },
    };

    const doc = seedCanvasFromScope(scope);
    const idFor = (name: string) =>
      doc.nodes.find((n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === name)?.id;
    const a = idFor("step_one");
    const b = idFor("step_two");
    const c = idFor("step_three");
    expect(a && b && c).toBeTruthy();
    expect(doc.edges.some((e) => e.kind === "sequence" && e.source === a && e.target === b)).toBe(true);
    expect(doc.edges.some((e) => e.kind === "sequence" && e.source === b && e.target === c)).toBe(true);
  });

  it("sequences two aliasing rules when extraction has no aliasing_pipeline in YAML", () => {
    const scope: Record<string, unknown> = {
      associations: [
        {
          kind: "source_view_to_extraction",
          source_view_index: 0,
          extraction_rule_name: "ext_one",
        },
      ],
      source_views: [{ view_external_id: "v1", entity_type: "asset" }],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                scope_filters: { entity_type: ["asset"] },
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              {
                name: "first_al",
                enabled: true,
                handler: "regex_substitution",
                priority: 10,
                config: { patterns: [] },
              },
              {
                name: "second_al",
                enabled: true,
                handler: "regex_substitution",
                priority: 20,
                config: { patterns: [] },
              },
            ],
          },
        },
      },
    };

    const doc = seedCanvasFromScope(scope);
    const idFirst = doc.nodes.find(
      (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === "first_al"
    )?.id;
    const idSecond = doc.nodes.find(
      (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === "second_al"
    )?.id;
    expect(
      doc.edges.some((e) => e.kind === "sequence" && e.source === idFirst && e.target === idSecond)
    ).toBe(true);
  });

  it("wires source view → extraction only from top-level associations", () => {
    const scope: Record<string, unknown> = {
      associations: [
        {
          kind: "source_view_to_extraction",
          source_view_index: 1,
          extraction_rule_name: "only_rule",
        },
      ],
      source_views: [
        { view_external_id: "v0", entity_type: "asset" },
        { view_external_id: "v1", entity_type: "timeseries" },
      ],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "only_rule",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                scope_filters: { entity_type: ["timeseries"] },
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              {
                name: "al_one",
                enabled: true,
                handler: "regex_substitution",
                priority: 10,
                config: { patterns: [] },
              },
            ],
          },
        },
      },
    };

    const doc = seedCanvasFromScope(scope);
    const extId = doc.nodes.find((n) => n.kind === "extraction")?.id;
    expect(extId).toBeDefined();
    const sv0ToExt = doc.edges.some(
      (e) => e.kind === "data" && e.source === "sv_0" && e.target === extId
    );
    const sv1ToExt = doc.edges.some(
      (e) => e.kind === "data" && e.source === "sv_1" && e.target === extId
    );
    expect(sv0ToExt).toBe(false);
    expect(sv1ToExt).toBe(true);
  });
});
