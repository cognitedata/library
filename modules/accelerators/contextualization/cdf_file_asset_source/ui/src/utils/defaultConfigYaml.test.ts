import { describe, expect, it } from "vitest";
import {
  mergePatternsIntoStepYaml,
  mergeScopeIntoDefault,
  mergeStepYamlIntoDefault,
  patternsFromStepYaml,
  scopeFromDefault,
  stepYamlFromDefault,
} from "./defaultConfigYaml";

const DEFAULT_SNIPPET = `
function_version: 1.0.0
workflow: create_asset_hierarchy_from_files
scope_hierarchy:
  type: hierarchy
  levels: [site, unit]
  locations:
    - id: SITE_A
      name: SITE_A
      locations:
        - id: SYS_1
          name: SYS_1
          files: [F-001]
          locations: []
file_asset_source:
  create:
    parameters:
      debug: false
    data:
      limit: -1
  extract:
    parameters:
      debug: false
    data:
      patterns:
        - category: equipment
          sample: [P-101]
`;

describe("defaultConfigYaml", () => {
  it("extracts scope from top-level scope_hierarchy", () => {
    const slice = stepYamlFromDefault(DEFAULT_SNIPPET, "scope");
    const h = scopeFromDefault(slice);
    expect(h.levels).toEqual(["site", "unit"]);
    expect(h.scope[0]?.id).toBe("SITE_A");
  });

  it("round-trips scope_hierarchy on full document", () => {
    const merged = mergeScopeIntoDefault(DEFAULT_SNIPPET, {
      levels: ["facility", "system"],
      scope: [{ id: "FAC", name: "FAC", files: ["X-1"], locations: [] }],
    });
    const h2 = scopeFromDefault(merged);
    expect(h2.levels).toEqual(["facility", "system"]);
    expect(h2.scope[0]?.files).toEqual(["X-1"]);
    const full = mergeStepYamlIntoDefault(DEFAULT_SNIPPET, "scope", merged);
    const h3 = scopeFromDefault(full);
    expect(h3.scope[0]?.files).toEqual(["X-1"]);
  });

  it("round-trips patterns in extract step slice", () => {
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
