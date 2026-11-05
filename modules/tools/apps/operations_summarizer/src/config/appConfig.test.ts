import { afterEach, describe, expect, it } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import {
  AppConfigError,
  clearAppConfig,
  getAppConfig,
  parseAppConfig,
  setAppConfig,
} from "@/config/appConfig";
import { DATA_TYPE_CONFIGS } from "@/config/dataTypes";

describe("default config node", () => {
  it("matches the previous app.json config values", () => {
    const config = parseAppConfig(DEFAULT_NODE_PROPERTIES);
    expect(config.schemaVersion).toBe(1);
    expect(config.companyName).toBe("");
    expect(config.spaces).toEqual({
      summaries: "ops_summaries",
      dataInstances: "inst_data",
    });
    expect(config.externalIdPrefixes).toEqual({
      operationSummary: "ops_summary_",
      testReport: "test_report_",
      podcast: "ops_podcast_",
    });
    expect(config.functions.summarizer).toEqual({
      externalId: "Operations_Summarizer_Function_v2",
      pollIntervalMs: 3000,
      timeoutMs: 600_000,
      maxConsecutivePollErrors: 5,
    });
    expect(config.agent).toEqual({
      name: "Operation Summarizer Agent",
      externalIdSuffix: "_operation_summarizer_helper_agent",
      defaultLanguageModel: "cognite/balanced",
      maxConcurrentTasks: 3,
    });
    expect(config.dataModel.views.asset).toEqual({
      space: "cdf_cdm",
      externalId: "CogniteAsset",
      version: "v1",
    });
    expect(config.dataModel.views.summaryOutputFile.externalId).toBe(
      "CogniteFile",
    );
    expect(config.dataModel.summaryListViewProperties).toEqual([
      "name",
      "description",
      "mimeType",
      "uploadedTime",
    ]);
    expect(config.dataModel.cdfAlphaVersionHeader).toBe("20250919-alpha");
    expect(config.limits).toEqual({
      chunkSize: 50,
      instanceQueryLimit: 1000,
      maxInstancesPerDatatype: 15_000,
      summaryListPageSize: 15,
      operationSummaryPickerMax: 500,
      defaultPageSize: 5,
      retry: { baseMs: 1000, maxDelayMs: 30_000 },
      concurrency: { default: 8, delete: 2, agent: 3 },
    });
  });

  it("builds the same table columns as the previous data type configs", () => {
    setAppConfig(parseAppConfig(DEFAULT_NODE_PROPERTIES));
    expect(DATA_TYPE_CONFIGS.assets.tableColumns.map((c) => c.header)).toEqual([
      "id",
      "name",
      "description",
      "parent",
    ]);
    expect(
      DATA_TYPE_CONFIGS.files.tableColumns[2]?.formatter?.(["A", "B"]),
    ).toBe("A, B");
    expect(DATA_TYPE_CONFIGS.work_orders.isSpecialCase).toBe(true);
    expect(DATA_TYPE_CONFIGS.work_orders.subcategories).toEqual([
      "completed_work_orders",
      "ongoing_work_orders",
      "scheduled_work_orders",
    ]);
    expect(DATA_TYPE_CONFIGS.equipment.viewId.externalId).toBe(
      "CogniteEquipment",
    );
  });
});

describe("parseAppConfig", () => {
  it("requires a non-empty podcast prefix and ignores spaces.podcasts", () => {
    const raw = validConfig();
    const prefixes = raw.externalIdPrefixes as Record<string, string>;
    prefixes.podcast = "";
    expect(() => parseAppConfig(raw)).toThrow(/externalIdPrefixes\.podcast/);

    const withLegacy = validConfig();
    (withLegacy.spaces as Record<string, string>).podcasts = "legacy";
    const parsed = parseAppConfig(withLegacy);
    expect(parsed.spaces).toEqual({
      summaries: "ops_summaries",
      dataInstances: "inst_data",
    });
    expect("podcasts" in parsed.spaces).toBe(false);
  });

  it("throws when a required key is missing", () => {
    const raw = validConfig();
    delete raw.spaces;
    expect(() => parseAppConfig(raw)).toThrow(AppConfigError);
    expect(() => parseAppConfig(raw)).toThrow(/config node spaces/);
  });

  it("throws when a limit has the wrong type", () => {
    const raw = validConfig();
    raw.limits = { ...(raw.limits as object), chunkSize: "50" };
    expect(() => parseAppConfig(raw)).toThrow(/config node limits\.chunkSize/);
  });

  it("throws when instanceQueryLimit exceeds the DMS ceiling", () => {
    const raw = validConfig();
    raw.limits = { ...(raw.limits as object), instanceQueryLimit: 1001 };
    expect(() => parseAppConfig(raw)).toThrow(/instanceQueryLimit/);
  });

  it("throws on an unknown formatter name", () => {
    const raw = validConfig();
    const dataTypes = raw.dataTypes as Record<
      string,
      { tableColumns: { formatter?: string }[] }
    >;
    dataTypes.files.tableColumns[2].formatter = "notAFormatter";
    expect(() => parseAppConfig(raw)).toThrow(/unknown formatter/);
  });

  it("throws when data type keys do not match DataTypeName", () => {
    const raw = validConfig();
    const dataTypes = raw.dataTypes as Record<string, unknown>;
    dataTypes.pumps = dataTypes.assets;
    expect(() => parseAppConfig(raw)).toThrow(/dataTypes/);
  });

  it("throws when schemaVersion is unsupported", () => {
    const raw = validConfig();
    raw.schemaVersion = 2;
    expect(() => parseAppConfig(raw)).toThrow(/schemaVersion 2/);
  });

  it("throws when agent.maxConcurrentTasks does not match the agent concurrency limit", () => {
    const raw = validConfig();
    raw.agent = { ...(raw.agent as object), maxConcurrentTasks: 9 };
    expect(() => parseAppConfig(raw)).toThrow(/agent\.maxConcurrentTasks/);
  });
});

describe("getAppConfig", () => {
  afterEach(() => {
    setAppConfig(parseAppConfig(DEFAULT_NODE_PROPERTIES));
  });

  it("throws before the provider has loaded the node", () => {
    clearAppConfig();
    expect(() => getAppConfig()).toThrow(/has not been loaded/);
  });

  it("returns the node installed by the provider", () => {
    const config = parseAppConfig(DEFAULT_NODE_PROPERTIES);
    setAppConfig(config);
    expect(getAppConfig()).toBe(config);
  });
});

function validConfig(): Record<string, unknown> {
  const clone: unknown = structuredClone(DEFAULT_NODE_PROPERTIES);
  if (typeof clone !== "object" || clone === null) {
    throw new Error("expected config object");
  }
  return clone as Record<string, unknown>;
}
