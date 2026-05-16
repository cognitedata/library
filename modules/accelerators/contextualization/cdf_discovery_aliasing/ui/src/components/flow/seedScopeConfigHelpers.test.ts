import { describe, expect, it } from "vitest";
import {
  buildExtractionMatchValidationSubgraph,
  collectAliasingRuleNamesReferencedByExtractionPipelines,
  collapseRepeatingValidationNameSequence,
  dedupeConsecutiveValidationStepNames,
  orderedAliasingRuleNamesForSeed,
  validationRulesLinearNamesForSeed,
  validationRulesStepsToLinearNames,
} from "./seedScopeConfigHelpers";

describe("collectAliasingRuleNamesReferencedByExtractionPipelines", () => {
  it("collects shorthand branch keys and tail rule names (Strip Delimiter style)", () => {
    const scope: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "asset_tag_candidate",
                aliasing_pipeline: [
                  {
                    hierarchy: {
                      mode: "concurrent",
                      children: [
                        { cogniteasset_explicit_aliases_from_raw: ["Strip Delimiter"] },
                        { leading_zero_normalization: ["Strip Delimiter"] },
                      ],
                    },
                  },
                ],
              },
            ],
          },
        },
      },
    };
    expect(collectAliasingRuleNamesReferencedByExtractionPipelines(scope)).toEqual([
      "cogniteasset_explicit_aliases_from_raw",
      "Strip Delimiter",
      "leading_zero_normalization",
    ]); // second branch’s ``Strip Delimiter`` string is deduped in encounter order
  });

  it("orderedAliasingRuleNamesForSeed matches pipeline-first definition-only merge", () => {
    const scope: Record<string, unknown> = {
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
    expect(orderedAliasingRuleNamesForSeed(scope)).toEqual(["Strip Delimiter", "rule_a", "rule_b"]);
  });
});

describe("validationRulesStepsToLinearNames", () => {
  it("collects rule ids from full rule-definition objects (name + config)", () => {
    const steps = [
      { name: "blacklist", priority: 10, match: { keywords: ["x"] } },
      { name: "isa_compliant", priority: 50, match: { expressions: [] } },
    ];
    expect(validationRulesStepsToLinearNames(steps)).toEqual(["blacklist", "isa_compliant"]);
  });

  it("mixes string steps and object steps", () => {
    expect(
      validationRulesStepsToLinearNames([
        "quick_string",
        { name: "full_rule", priority: 1, match: {} },
      ])
    ).toEqual(["quick_string", "full_rule"]);
  });

  it("does not collapse alternating names (used for aliasing pipeline shorthand)", () => {
    expect(validationRulesStepsToLinearNames(["a", "b", "a", "b"])).toEqual(["a", "b", "a", "b"]);
  });
});

describe("dedupeConsecutiveValidationStepNames", () => {
  it("merges repeated adjacent ids", () => {
    expect(dedupeConsecutiveValidationStepNames(["a", "a", "b", "b", "b"])).toEqual(["a", "b"]);
  });
});

describe("collapseRepeatingValidationNameSequence", () => {
  it("keeps one period when the list is k full repeats of the same prefix", () => {
    expect(
      collapseRepeatingValidationNameSequence([
        "blacklist",
        "whitelist",
        "not_isa_penalty",
        "blacklist",
        "whitelist",
        "not_isa_penalty",
        "blacklist",
        "whitelist",
        "not_isa_penalty",
        "blacklist",
        "whitelist",
        "not_isa_penalty",
      ])
    ).toEqual(["blacklist", "whitelist", "not_isa_penalty"]);
  });
});

describe("validationRulesStepsToLinearNames concurrent hierarchy", () => {
  it("collapses identical concurrent branches (no 4× linear concat)", () => {
    const chain = { blacklist: [{ whitelist: ["not_isa_penalty"] }] };
    const steps = [
      {
        hierarchy: {
          mode: "concurrent",
          children: [chain, chain, chain, chain],
        },
      },
    ];
    expect(validationRulesStepsToLinearNames(steps)).toEqual(["blacklist", "whitelist", "not_isa_penalty"]);
  });
});

describe("validationRulesLinearNamesForSeed", () => {
  it("collapses k× repeated validation chain from duplicated YAML array entries", () => {
    const chain = { blacklist: [{ whitelist: ["not_isa_penalty"] }] };
    const steps = [chain, chain, chain, chain];
    expect(validationRulesLinearNamesForSeed(steps)).toEqual(["blacklist", "whitelist", "not_isa_penalty"]);
  });
});

describe("buildExtractionMatchValidationSubgraph", () => {
  it("skips consecutive duplicate step names in the same chain", () => {
    const g = buildExtractionMatchValidationSubgraph(["x", "x", "y"], "er", false, {
      y: 0,
      xStart: 0,
      xStep: 100,
      idPrefix: "t",
    });
    expect(g.nodes).toHaveLength(2);
  });
});
