import { describe, expect, it } from "vitest";
import {
  mergePatternsIntoStepYaml,
  mergeScopeIntoStepYaml,
  mergeStepYamlIntoDefault,
  patternsFromStepYaml,
  scopeFromStepYaml,
  stepYamlFromDefault,
} from "./defaultConfigYaml";

const DEFAULT_SNIPPET = `
function_version: 1.0.0
workflow: create_asset_hierarchy_from_files
file_asset_source:
  create:
    parameters:
      debug: false
    data:
      hierarchy_levels: [site, plant]
      scope:
        - name: SITE_A
          locations:
            - name: SYS_1
              files: [F-001]
  extract:
    parameters:
      debug: false
    data:
      patterns:
        - category: equipment
          sample: [P-101]
`;

describe("defaultConfigYaml", () => {
  it("extracts scope (create) step slice", () => {
    const slice = stepYamlFromDefault(DEFAULT_SNIPPET, "scope");
    const h = scopeFromStepYaml(slice);
    expect(h.hierarchy_levels).toEqual(["site", "plant"]);
    expect(h.scope[0]?.name).toBe("SITE_A");
  });

  it("round-trips scope in step slice", () => {
    const slice = stepYamlFromDefault(DEFAULT_SNIPPET, "scope");
    const merged = mergeScopeIntoStepYaml(slice, {
      hierarchy_levels: ["facility", "system"],
      scope: [{ name: "FAC", files: ["X-1"] }],
    });
    const h2 = scopeFromStepYaml(merged);
    expect(h2.hierarchy_levels).toEqual(["facility", "system"]);
    expect(h2.scope[0]?.files).toEqual(["X-1"]);
    const full = mergeStepYamlIntoDefault(DEFAULT_SNIPPET, "scope", merged);
    const h3 = scopeFromStepYaml(stepYamlFromDefault(full, "scope"));
    expect(h3.scope[0]?.files).toEqual(["X-1"]);
  });

  it("round-trips patterns in step slice", () => {
    const slice = stepYamlFromDefault(DEFAULT_SNIPPET, "extract");
    const p = patternsFromStepYaml(slice);
    expect(p.patterns[0]?.category).toBe("equipment");
    const merged = mergePatternsIntoStepYaml(slice, {
      patterns: [{ category: "general", sample: ["V-00"] }],
    });
    const p2 = patternsFromStepYaml(merged);
    expect(p2.patterns[0]?.sample).toEqual(["V-00"]);
  });

});
