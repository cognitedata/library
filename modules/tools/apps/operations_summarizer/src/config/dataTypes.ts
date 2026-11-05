/**
 * Centralized data type configuration for the Operations Summarizer.
 * Transpiled from Python data_type_config.py
 */

import { getAppConfig } from "@/config/appConfig";
import { VIEW_IDS } from "@/config/viewIds";
import type { AppConfig, FormatterName } from "@/types/appConfig";
import type {
  DataTypeConfig,
  DataTypeName,
  SummaryContents,
  ViewId,
} from "@/types/operations";
import { formatIsoDate } from "@/utils/formatting";

// ============================================================
// Values from the config node. Read at call time, after load.
// ============================================================

export function getSummarySpace(): string {
  return getAppConfig().spaces.summaries;
}

/** External-ID prefix for podcast script and audio files in the summary space. */
export function getPodcastPrefix(): string {
  return getAppConfig().externalIdPrefixes.podcast;
}

/** External ID of the deployed Operations Summarizer Cognite Function. */
export function getSummarizerFunctionExternalId(): string {
  return getAppConfig().functions.summarizer.externalId;
}

export function getInstanceQueryLimit(): number {
  return getAppConfig().limits.instanceQueryLimit;
}

// ============================================================
// Formatters
// ============================================================

export function formatAssetsList(assets: unknown): string {
  if (assets === null || assets === undefined || assets === "") return "N/A";
  if (Array.isArray(assets)) {
    const filtered = assets
      .filter((a) => a !== null && a !== undefined && a !== "")
      .map(String);
    return filtered.join(", ") || "N/A";
  }
  return String(assets);
}

function formatIsoDateCell(value: unknown): string {
  if (typeof value === "string" || value === null || value === undefined) {
    return formatIsoDate(value);
  }
  return formatIsoDate(String(value));
}

const FORMATTERS: Record<FormatterName, (value: unknown) => string> = {
  assetsList: formatAssetsList,
  isoDate: formatIsoDateCell,
};

// ============================================================
// Data Type Configurations
//
// Each entry in DATA_TYPE_CONFIGS describes how to fetch, format, and
// summarize one category of CDF data (assets, files, work orders, etc.).
// It binds together:
//   - viewId: the CDF Data Model view to query
//   - tableColumns: which instance properties to extract and how to format
//     them into the plain-text table that is sent to the AI prompt
//   - promptTemplate: the per-data-type instructions appended to PROMPT_HEADER
//   - isSpecialCase / subcategories: whether this type is split into
//     sub-buckets (e.g. completed / ongoing / scheduled work orders)
//     before summarisation
// ============================================================

const VIEW_ID_BY_CONFIG_KEY = {
  asset: () => VIEW_IDS.ASSET,
  file: () => VIEW_IDS.FILE,
  activity: () => VIEW_IDS.ACTIVITY,
  timeSeries: () => VIEW_IDS.TIME_SERIES,
  equipment: () => VIEW_IDS.EQUIPMENT,
} as const;

let cachedConfig: AppConfig | undefined;
let cachedDataTypes: Record<DataTypeName, DataTypeConfig> | undefined;

function loadDataTypeConfigs(): Record<DataTypeName, DataTypeConfig> {
  const config = getAppConfig();
  if (cachedDataTypes && cachedConfig === config) return cachedDataTypes;
  cachedConfig = config;
  cachedDataTypes = {
    assets: buildDataTypeConfig("assets", config),
    files: buildDataTypeConfig("files", config),
    work_orders: buildDataTypeConfig("work_orders", config),
    time_series: buildDataTypeConfig("time_series", config),
    equipment: buildDataTypeConfig("equipment", config),
  };
  return cachedDataTypes;
}

/** Property access reads the loaded config node. */
export const DATA_TYPE_CONFIGS: Record<DataTypeName, DataTypeConfig> =
  createLazyRecord(loadDataTypeConfigs);

function buildDataTypeConfig(
  name: DataTypeName,
  config: AppConfig,
): DataTypeConfig {
  const source = config.dataTypes[name];
  return {
    name,
    displayName: source.displayName,
    viewId: VIEW_ID_BY_CONFIG_KEY[source.view](),
    promptTemplate: config.prompts.dataTypes[name],
    tableColumns: source.tableColumns.map((column) => ({
      header: column.header,
      property: column.property,
      ...(column.default !== undefined ? { default: column.default } : {}),
      ...(column.formatter !== undefined
        ? { formatter: FORMATTERS[column.formatter] }
        : {}),
    })),
    isSpecialCase: source.isSpecialCase,
    ...(source.subcategories !== undefined
      ? { subcategories: source.subcategories }
      : {}),
  };
}

// ============================================================
// Helper Functions
// ============================================================

export function getDataTypeConfig(
  dataType: string,
): DataTypeConfig | undefined {
  return DATA_TYPE_CONFIGS[dataType as DataTypeName];
}

export function getEnabledDataTypes(): DataTypeName[] {
  return Object.keys(DATA_TYPE_CONFIGS) as DataTypeName[];
}

export function getViewIdMapping(): Record<DataTypeName, ViewId> {
  return Object.fromEntries(
    Object.entries(DATA_TYPE_CONFIGS).map(([name, config]) => [
      name,
      config.viewId,
    ]),
  ) as Record<DataTypeName, ViewId>;
}

export function getPromptMapping(): Record<DataTypeName, string> {
  return Object.fromEntries(
    Object.entries(DATA_TYPE_CONFIGS).map(([name, config]) => [
      name,
      config.promptTemplate,
    ]),
  ) as Record<DataTypeName, string>;
}

export function getDisplayName(dataType: string): string {
  const config = getDataTypeConfig(dataType);
  if (config) return config.displayName;
  return dataType.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase());
}

export function getDefaultSummaryContents(): SummaryContents {
  const dataTypes = getAppConfig().dataTypes;
  return {
    assets: dataTypes.assets.enabledByDefault,
    files: dataTypes.files.enabledByDefault,
    work_orders: dataTypes.work_orders.enabledByDefault,
    time_series: dataTypes.time_series.enabledByDefault,
    equipment: dataTypes.equipment.enabledByDefault,
  };
}

/** Keys of data types that are currently selected (enabled) in summary contents. */
export function getSelectedContentTypeKeys(
  contents: SummaryContents,
): DataTypeName[] {
  return (
    Object.entries(contents).filter(([, v]) => v) as [DataTypeName, boolean][]
  ).map(([key]) => key);
}

/** Prompt templates from the config node, keyed by data type. */
export function getDefaultPromptTemplates(): Record<string, string> {
  return getPromptMapping();
}

function createLazyRecord<K extends string, V>(
  load: () => Record<K, V>,
): Record<K, V> {
  return new Proxy({} as Record<K, V>, {
    get(_target, prop) {
      if (typeof prop !== "string") return undefined;
      return load()[prop as K];
    },
    has(_target, prop) {
      if (typeof prop !== "string") return false;
      return Object.hasOwn(load(), prop);
    },
    ownKeys() {
      return Object.keys(load());
    },
    getOwnPropertyDescriptor(_target, prop) {
      if (typeof prop !== "string") return undefined;
      const record = load();
      if (!Object.hasOwn(record, prop)) return undefined;
      return {
        configurable: true,
        enumerable: true,
        writable: false,
        value: record[prop as K],
      };
    },
  });
}
