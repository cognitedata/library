import { describe, expect, it } from "vitest";
import {
  emptyIndexFieldProperty,
  type IndexFieldView,
} from "../types/invertedIndexConfig";
import {
  docFromYaml,
  indexFieldsFromDoc,
  mergeIndexFieldsIntoDoc,
  sanitizeIndexFieldsForPersist,
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
});
