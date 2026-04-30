import { describe, expect, it } from "vitest";
import { syncWorkflowScopeFromCanvas } from "./canvasScopeSync";
import { mergeScopeRootsForTriggerFlowSeed, seedCanvasFromScope } from "./seedCanvasFromScope";
import { validationRulesStepsToLinearNames } from "./seedScopeConfigHelpers";

describe("mergeScopeRootsForTriggerFlowSeed", () => {
  it("fills missing aliasing_rule_definitions from workflow scope fallback", () => {
    const triggerCfg: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: ["Strip Delimiter"],
              },
            ],
          },
        },
      },
      aliasing: { config: { data: { aliasing_rules: [] } } },
    };
    const scopeFallback: Record<string, unknown> = {
      aliasing_rule_definitions: {
        "Strip Delimiter": {
          name: "Strip Delimiter",
          handler: "regex_substitution",
          config: { patterns: [] },
        },
      },
    };
    const merged = mergeScopeRootsForTriggerFlowSeed(triggerCfg, scopeFallback);
    expect((merged.aliasing_rule_definitions as Record<string, unknown>)["Strip Delimiter"]).toBeDefined();
    const doc = seedCanvasFromScope(merged);
    expect(
      doc.nodes.some(
        (n) =>
          n.kind === "aliasing" &&
          (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name === "Strip Delimiter"
      )
    ).toBe(true);
  });

  it("keeps trigger definitions when already present", () => {
    const def = { x: { name: "x", handler: "regex_substitution", config: {} } };
    const merged = mergeScopeRootsForTriggerFlowSeed(
      { aliasing_rule_definitions: def },
      { aliasing_rule_definitions: { y: { name: "y", handler: "regex_substitution", config: {} } } }
    );
    expect(merged.aliasing_rule_definitions).toEqual(def);
  });
});

describe("seedCanvasFromScope — layout from rules, aliasing_pipeline, and validation", () => {
  it("connects extraction to the pipeline head and adds sequence along declared pipeline", () => {
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
                aliasing_pipeline: ["alpha_alias", "beta_alias"],
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

  it("adds Strip Delimiter from aliasing_rule_definitions when only extraction pipeline references it", () => {
    const scope: Record<string, unknown> = {
      associations: [],
      source_views: [],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: ["Strip Delimiter"],
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
          priority: 10,
          preserve_original: true,
          config: {
            patterns: [{ pattern: "[^\\p{L}\\p{N}]", replacement: "" }],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const ext = doc.nodes.find((n) => n.kind === "extraction");
    const alStrip = doc.nodes.find(
      (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name === "Strip Delimiter"
    );
    expect(ext).toBeDefined();
    expect(alStrip).toBeDefined();
    expect(alStrip?.data?.handler_id).toBe("regex_substitution");
    expect(doc.edges.some((e) => e.kind === "data" && e.source === ext?.id && e.target === alStrip?.id)).toBe(true);
  });

  it("places pipeline-first definition-only Strip Delimiter before pathway aliasing_rules rows", () => {
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
              {
                name: "rule_a",
                enabled: true,
                handler: "character_substitution",
                priority: 10,
                config: {},
              },
              {
                name: "rule_b",
                enabled: true,
                handler: "character_substitution",
                priority: 20,
                config: {},
              },
            ],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const alOrder = doc.nodes
      .filter((n) => n.kind === "aliasing")
      .map((n) => (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name);
    expect(alOrder).toEqual(["Strip Delimiter", "rule_a", "rule_b"]);
  });

  it("seeds definition-only Strip Delimiter when scope has no top-level aliasing block", () => {
    const scope: Record<string, unknown> = {
      associations: [],
      source_views: [],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: ["Strip Delimiter"],
              },
            ],
          },
        },
      },
      aliasing_rule_definitions: {
        "Strip Delimiter": {
          name: "Strip Delimiter",
          type: "regex_substitution",
          enabled: true,
          priority: 10,
          config: { patterns: [] },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const alStrip = doc.nodes.find(
      (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name === "Strip Delimiter"
    );
    expect(alStrip?.data?.handler_id).toBe("regex_substitution");
  });

  it("orders a three-step pipeline with sequence edges from aliasing_pipeline on the extraction rule", () => {
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
                aliasing_pipeline: ["step_one", "step_two", "step_three"],
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
      doc.nodes.find(
        (n) => n.kind === "aliasing" && (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === name
      )?.id;
    const a = idFor("step_one");
    const b = idFor("step_two");
    const c = idFor("step_three");
    expect(a && b && c).toBeTruthy();
    expect(doc.edges.some((e) => e.kind === "sequence" && e.source === a && e.target === b)).toBe(true);
    expect(doc.edges.some((e) => e.kind === "sequence" && e.source === b && e.target === c)).toBe(true);
  });

  it("with no aliasing_pipeline, wires head from extraction and backfills sequence along pathway order", () => {
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
      doc.edges.some((e) => e.kind === "data" && e.target === idFirst)
    ).toBe(true);
    expect(
      doc.edges.some((e) => e.kind === "sequence" && e.source === idFirst && e.target === idSecond)
    ).toBe(true);
    expect(doc.edges.some((e) => e.kind === "data" && e.target === "flow_end" && e.source === idSecond!)).toBe(
      true
    );
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

  it("seeds match_validation_extraction chain from per-rule validation_rules", () => {
    const scope: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "rule_a",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                validation: { validation_rules: ["m1", "m2"] },
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: { aliasing_rules: [] },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const ext = doc.nodes.find((n) => n.kind === "extraction");
    const m1 = doc.nodes.find(
      (n) => n.kind === "match_validation_extraction" && (n.data as { validation_rule_name?: string }).validation_rule_name === "m1"
    );
    const m2 = doc.nodes.find(
      (n) => n.kind === "match_validation_extraction" && (n.data as { validation_rule_name?: string }).validation_rule_name === "m2"
    );
    expect(ext).toBeDefined();
    expect(m1).toBeDefined();
    expect(m2).toBeDefined();
    expect(
      doc.edges.some((e) => e.kind === "data" && e.source === ext!.id && e.target === m1!.id)
    ).toBe(true);
    expect(
      doc.edges.some((e) => e.kind === "sequence" && e.source === m1!.id && e.target === m2!.id)
    ).toBe(true);
  });

  it("seeds match_validation_aliasing from a transform row validation", () => {
    const scope: Record<string, unknown> = {
      key_extraction: { config: { data: { extraction_rules: [] } } },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              {
                name: "al_t",
                enabled: true,
                handler: "regex_substitution",
                priority: 10,
                config: { patterns: [] },
                validation: { validation_rules: ["al_v1", "al_v2"] },
              },
            ],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const al = doc.nodes.find((n) => n.kind === "aliasing");
    const v1 = doc.nodes.find(
      (n) => n.kind === "match_validation_aliasing" && (n.data as { validation_rule_name?: string }).validation_rule_name === "al_v1"
    );
    expect(
      doc.edges.some((e) => e.kind === "data" && e.source === al?.id && e.target === v1?.id)
    ).toBe(true);
  });

  it("seeds global validation from key_extraction / aliasing config.data.validation", () => {
    const scope: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            validation: { validation_rules: ["ge1"] },
            extraction_rules: [
              {
                name: "e1",
                enabled: true,
                handler: "regex_handler",
                priority: 1,
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            validation: { validation_rules: ["ga1"] },
            aliasing_rules: [
              { name: "a1", enabled: true, handler: "regex_substitution", priority: 1, config: { patterns: [] } },
            ],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const ge1 = doc.nodes.find(
      (n) => n.kind === "match_validation_extraction" && (n.data as { ref?: { extraction_global_validation?: boolean } }).ref?.extraction_global_validation
    );
    const ga1 = doc.nodes.find(
      (n) => n.kind === "match_validation_aliasing" && (n.data as { ref?: { aliasing_global_validation?: boolean } }).ref?.aliasing_global_validation
    );
    const ext0 = doc.nodes.find((n) => n.kind === "extraction");
    const al0 = doc.nodes.find((n) => n.kind === "aliasing");
    expect(ge1).toBeDefined();
    expect(ga1).toBeDefined();
    expect(doc.edges.some((e) => e.source === ext0?.id && e.target === ge1?.id)).toBe(true);
    expect(doc.edges.some((e) => e.source === al0?.id && e.target === ga1?.id)).toBe(true);
  });

  it("round-trips linear extraction validation through sync", () => {
    const scope: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "rule_x",
                enabled: true,
                handler: "regex_handler",
                priority: 5,
                validation: { validation_rules: ["a", "b"] },
              },
            ],
          },
        },
      },
      aliasing: { config: { data: { aliasing_rules: [] } } },
    };
    const seeded = seedCanvasFromScope(scope);
    const merged = syncWorkflowScopeFromCanvas(seeded, JSON.parse(JSON.stringify(scope)) as Record<string, unknown>);
    const rules = (merged.key_extraction as { config: { data: { extraction_rules: { name: string; validation?: { validation_rules?: string[] } }[] } } })
      .config.data.extraction_rules;
    const v = rules.find((r) => r.name === "rule_x")?.validation?.validation_rules;
    // Sync may emit linear shorthand (e.g. object form) for two-step chains; names must still round-trip.
    expect(validationRulesStepsToLinearNames(v)).toEqual(["a", "b"]);
  });

  it("orders al canvas rows by pathway encounter order, not priority", () => {
    const scope: Record<string, unknown> = {
      associations: [],
      source_views: [],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_one",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: ["path_first"],
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            pathways: {
              steps: [
                {
                  mode: "sequential",
                  rules: [
                    {
                      name: "path_first",
                      enabled: true,
                      handler: "regex_substitution",
                      priority: 99,
                      config: { patterns: [] },
                    },
                    {
                      name: "path_second",
                      enabled: true,
                      handler: "regex_substitution",
                      priority: 1,
                      config: { patterns: [] },
                    },
                  ],
                },
              ],
            },
            aliasing_rules: [],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const alOrder = doc.nodes
      .filter((n) => n.kind === "aliasing")
      .map((n) => (n.data?.ref as { aliasing_rule_name?: string }).aliasing_rule_name);
    expect(alOrder).toEqual(["path_first", "path_second"]);
  });

  it("does not pathway-spine sequence across parallel pathway branches (lift parity)", () => {
    const scope: Record<string, unknown> = {
      associations: [],
      source_views: [],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_p",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: ["branch_a"],
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            pathways: {
              steps: [
                {
                  mode: "parallel",
                  branches: [
                    [{ name: "branch_a", enabled: true, handler: "regex_substitution", priority: 10, config: { patterns: [] } }],
                    [{ name: "branch_b", enabled: true, handler: "regex_substitution", priority: 20, config: { patterns: [] } }],
                  ],
                },
              ],
            },
            aliasing_rules: [],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const idA = doc.nodes.find((n) => (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === "branch_a")?.id;
    const idB = doc.nodes.find((n) => (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === "branch_b")?.id;
    expect(idA).toBeTruthy();
    expect(idB).toBeTruthy();
    const seqAToB = doc.edges.some((e) => e.kind === "sequence" && e.source === idA && e.target === idB);
    expect(seqAToB).toBe(false);
    const ext = doc.nodes.find((n) => n.kind === "extraction");
    expect(doc.edges.some((e) => e.kind === "data" && e.source === ext?.id && e.target === idA)).toBe(true);
  });

  it("wires concurrent aliasing_pipeline with two data edges from extraction", () => {
    const scope: Record<string, unknown> = {
      associations: [],
      source_views: [],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_c",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: [
                  {
                    hierarchy: {
                      mode: "concurrent",
                      children: ["branch_a", "branch_b"],
                    },
                  },
                ],
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              { name: "branch_a", enabled: true, handler: "regex_substitution", priority: 10, config: { patterns: [] } },
              { name: "branch_b", enabled: true, handler: "regex_substitution", priority: 20, config: { patterns: [] } },
            ],
          },
        },
      },
    };
    const doc = seedCanvasFromScope(scope);
    const ext = doc.nodes.find((n) => n.kind === "extraction");
    const idA = doc.nodes.find((n) => (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === "branch_a")?.id;
    const idB = doc.nodes.find((n) => (n.data?.ref as { aliasing_rule_name?: string })?.aliasing_rule_name === "branch_b")?.id;
    const dataFromExt = doc.edges.filter((e) => e.kind === "data" && e.source === ext?.id);
    expect(dataFromExt.some((e) => e.target === idA)).toBe(true);
    expect(dataFromExt.some((e) => e.target === idB)).toBe(true);
  });

  it("round-trips concurrent aliasing_pipeline through sync", () => {
    const scope: Record<string, unknown> = {
      associations: [],
      source_views: [],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "ext_rt",
                enabled: true,
                handler: "regex_handler",
                priority: 10,
                aliasing_pipeline: [
                  {
                    hierarchy: {
                      mode: "concurrent",
                      children: ["ba", "bb"],
                    },
                  },
                ],
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              { name: "ba", enabled: true, handler: "regex_substitution", priority: 10, config: { patterns: [] } },
              { name: "bb", enabled: true, handler: "regex_substitution", priority: 20, config: { patterns: [] } },
            ],
          },
        },
      },
    };
    const copy = JSON.parse(JSON.stringify(scope)) as Record<string, unknown>;
    const merged = syncWorkflowScopeFromCanvas(seedCanvasFromScope(copy), copy);
    const row = (
      merged.key_extraction as {
        config: { data: { extraction_rules: { name: string; aliasing_pipeline?: unknown[] }[] } };
      }
    ).config.data.extraction_rules.find((r) => r.name === "ext_rt");
    const pl = row?.aliasing_pipeline;
    expect(Array.isArray(pl)).toBe(true);
    expect(JSON.stringify(pl)).toMatch(/concurrent/i);
  });
});
