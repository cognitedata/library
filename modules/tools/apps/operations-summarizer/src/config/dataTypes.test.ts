import { describe, expect, it } from "vitest";
import {
  DATA_TYPE_CONFIGS,
  formatAssetsList,
  getDataTypeConfig,
  getDefaultPromptTemplates,
  getDefaultSummaryContents,
  getDisplayName,
  getEnabledDataTypes,
  getPodcastPrefix,
  getPromptMapping,
  getSelectedContentTypeKeys,
  getSummarizerFunctionExternalId,
  getSummarySpace,
  getViewIdMapping,
} from "./dataTypes";

describe("dataTypes constants", () => {
  it("getSummarySpace() is ops_summaries", () => {
    expect(getSummarySpace()).toBe("ops_summaries");
  });

  it("getPodcastPrefix() is ops_podcast_", () => {
    expect(getPodcastPrefix()).toBe("ops_podcast_");
  });

  it("getSummarizerFunctionExternalId() matches the deployed function", () => {
    expect(getSummarizerFunctionExternalId()).toBe(
      "Operations_Summarizer_Function_v2",
    );
  });
});

describe("getDataTypeConfig", () => {
  it("returns config for valid data type", () => {
    const config = getDataTypeConfig("assets");
    expect(config).toBeDefined();
    expect(config!.name).toBe("assets");
    expect(config!.displayName).toBe("Assets");
  });

  it("returns undefined for unknown type", () => {
    expect(getDataTypeConfig("unknown")).toBeUndefined();
  });
});

describe("getEnabledDataTypes", () => {
  it("returns all data type names", () => {
    const types = getEnabledDataTypes();
    expect(types).toContain("assets");
    expect(types).toContain("files");
    expect(types).toContain("work_orders");
    expect(types).toContain("time_series");
    expect(types).toContain("equipment");
    expect(types.length).toBe(5);
  });
});

describe("getViewIdMapping", () => {
  it("returns view id per data type", () => {
    const mapping = getViewIdMapping();
    expect(mapping.assets).toEqual(DATA_TYPE_CONFIGS.assets.viewId);
    expect(mapping.files).toEqual(DATA_TYPE_CONFIGS.files.viewId);
  });
});

describe("getPromptMapping", () => {
  it("returns non-empty prompt per data type", () => {
    const mapping = getPromptMapping();
    expect(mapping.assets.length).toBeGreaterThan(0);
    expect(mapping.work_orders).toContain("work order");
  });
});

describe("getDisplayName", () => {
  it("returns display name for known type", () => {
    expect(getDisplayName("assets")).toBe("Assets");
    expect(getDisplayName("work_orders")).toBe("Work Orders");
  });

  it("formats unknown type with underscores as spaces", () => {
    expect(getDisplayName("some_type")).toBe("Some Type");
  });
});

describe("getDefaultSummaryContents", () => {
  it("returns all true", () => {
    const contents = getDefaultSummaryContents();
    expect(contents.assets).toBe(true);
    expect(contents.files).toBe(true);
    expect(contents.work_orders).toBe(true);
    expect(contents.time_series).toBe(true);
    expect(contents.equipment).toBe(true);
  });
});

describe("getDefaultPromptTemplates", () => {
  it("returns same keys as getPromptMapping", () => {
    const templates = getDefaultPromptTemplates();
    const mapping = getPromptMapping();
    expect(Object.keys(templates).sort()).toEqual(Object.keys(mapping).sort());
  });

  it("returns non-empty string per key", () => {
    const templates = getDefaultPromptTemplates();
    Object.values(templates).forEach((v) => {
      expect(typeof v).toBe("string");
      expect(v.length).toBeGreaterThan(0);
    });
  });
});

describe("getSelectedContentTypeKeys", () => {
  it("returns keys where value is true", () => {
    const contents = getDefaultSummaryContents();
    const keys = getSelectedContentTypeKeys(contents);
    expect(keys).toContain("assets");
    expect(keys).toContain("equipment");
    expect(keys.length).toBe(5);
  });

  it("returns only enabled keys when some are false", () => {
    const keys = getSelectedContentTypeKeys({
      assets: true,
      files: false,
      work_orders: true,
      time_series: false,
      equipment: true,
    });
    expect(keys).toEqual(["assets", "work_orders", "equipment"]);
  });

  it("returns empty array when all false", () => {
    const keys = getSelectedContentTypeKeys({
      assets: false,
      files: false,
      work_orders: false,
      time_series: false,
      equipment: false,
    });
    expect(keys).toEqual([]);
  });
});

describe("formatAssetsList", () => {
  it("returns N/A for null/undefined", () => {
    expect(formatAssetsList(null)).toBe("N/A");
    expect(formatAssetsList(undefined)).toBe("N/A");
  });

  it("joins array with comma", () => {
    expect(formatAssetsList(["a", "b"])).toBe("a, b");
  });

  it("returns N/A for empty array", () => {
    expect(formatAssetsList([])).toBe("N/A");
  });

  it("converts non-array to string", () => {
    expect(formatAssetsList(123)).toBe("123");
  });
});
