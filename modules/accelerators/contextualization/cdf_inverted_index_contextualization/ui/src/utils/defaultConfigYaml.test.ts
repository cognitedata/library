import { describe, expect, it } from "vitest";
import {
  emptyAnnotationIndexConfig,
  emptyIndexFieldProperty,
  emptyScopeConfig,
  emptyScopePropertyOverride,
  type IndexFieldView,
} from "../types/invertedIndexConfig";
import {
  annotationFromDoc,
  docFromYaml,
  indexFieldsFromDoc,
  mergeAnnotationIntoDoc,
  mergeIndexFieldsIntoDoc,
  mergeScopeIntoDoc,
  sanitizeIndexFieldsForPersist,
  scopeFromDoc,
  yamlFromDoc,
} from "./defaultConfigYaml";

function viewWithProperties(properties: IndexFieldView["properties"]): IndexFieldView {
  return {
    view: "CogniteAsset",
    viewSpace: "cdf_cdm",
    version: "v1",
    instanceSpaces: [],
    filters: [],
    properties,
    propertiesByScope: {},
  };
}

describe("scope resolve_from YAML round-trip", () => {
  it("preserves newly added per-view entries before paths are configured", () => {
    const doc = docFromYaml("scope:\n  levels: [site, unit]\n");
    const scope = {
      ...emptyScopeConfig(),
      levels: ["site", "unit"],
      resolveFrom: { CogniteAsset: {} },
    };
    mergeScopeIntoDoc(doc, scope);

    const roundTripped = scopeFromDoc(docFromYaml(yamlFromDoc(doc)));
    expect(roundTripped.resolveFrom).toEqual({ CogniteAsset: {} });
  });
});

describe("index field config YAML round-trip", () => {
  it("preserves draft property rows with empty path during editing", () => {
    const doc = docFromYaml("index_field_config: []\n");
    const views = [
      viewWithProperties([
        { ...emptyIndexFieldProperty(), path: "name" },
        { ...emptyIndexFieldProperty(), path: "" },
      ]),
    ];
    mergeIndexFieldsIntoDoc(doc, views);

    const roundTripped = indexFieldsFromDoc(docFromYaml(yamlFromDoc(doc)));
    expect(roundTripped).toHaveLength(1);
    expect(roundTripped[0].properties).toHaveLength(2);
    expect(roundTripped[0].properties[0].path).toBe("name");
    expect(roundTripped[0].properties[1].path).toBe("");
  });

  it("sanitizeIndexFieldsForPersist removes empty-path properties", () => {
    const doc = docFromYaml("index_field_config: []\n");
    mergeIndexFieldsIntoDoc(doc, [
      viewWithProperties([
        { ...emptyIndexFieldProperty(), path: "name" },
        { ...emptyIndexFieldProperty(), path: "" },
      ]),
    ]);

    sanitizeIndexFieldsForPersist(doc);

    const persisted = indexFieldsFromDoc(doc);
    expect(persisted[0].properties).toEqual([
      { ...emptyIndexFieldProperty(), path: "name" },
    ]);
  });

  it("round-trips properties_by_scope overrides", () => {
    const doc = docFromYaml("index_field_config: []\n");
    const scopeOverride = {
      ...emptyScopePropertyOverride(),
      mode: "replace" as const,
      properties: [
        {
          ...emptyIndexFieldProperty(),
          path: "description",
          extractPattern: "\\bEQ-\\d+\\b",
          extractMode: "regex" as const,
        },
      ],
    };
    mergeIndexFieldsIntoDoc(doc, [
      {
        ...viewWithProperties([{ ...emptyIndexFieldProperty(), path: "name" }]),
        propertiesByScope: {
          "site:Rotterdam|unit:*": scopeOverride,
        },
      },
    ]);

    const roundTripped = indexFieldsFromDoc(docFromYaml(yamlFromDoc(doc)));
    expect(roundTripped[0].propertiesByScope["site:Rotterdam|unit:*"]).toEqual(scopeOverride);
  });
});

describe("annotation identity YAML round-trip", () => {
  it("preserves YAML-only Jinja templates when merging UI identity fields", () => {
    const doc = docFromYaml(`
annotation_index_config:
  view: CogniteDiagramAnnotation
  identity:
    annotation_external_id_prefix: idx_ann
    detection_key_template: "{{ page_label }}:custom"
    annotation_external_id_template: "{{ prefix }}_{{ digest16 }}"
`);
    const cfg = {
      ...emptyAnnotationIndexConfig(),
      identity: {
        ...emptyAnnotationIndexConfig().identity,
        annotationExternalIdPrefix: "proj_ann",
        detectionKeyTermPrefixLength: 16,
      },
    };
    mergeAnnotationIntoDoc(doc, cfg);

    const ann = doc.annotation_index_config as Record<string, unknown>;
    const identity = ann.identity as Record<string, unknown>;
    expect(identity.detection_key_template).toBe("{{ page_label }}:custom");
    expect(identity.annotation_external_id_template).toBe("{{ prefix }}_{{ digest16 }}");
    expect(identity.annotation_external_id_prefix).toBe("proj_ann");
    expect(identity.detection_key_term_prefix_length).toBe(16);

    const roundTripped = annotationFromDoc(docFromYaml(yamlFromDoc(doc)));
    expect(roundTripped.identity.annotationExternalIdPrefix).toBe("proj_ann");
    expect(roundTripped.identity.detectionKeyTermPrefixLength).toBe(16);
  });
});
