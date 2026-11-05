import {
  CONFIG_NODE_PATH,
  SUPPORTED_SCHEMA_VERSION,
} from "@/config/configLocation";
import type {
  AppConfig,
  AppConfigDataType,
  AppConfigTableColumn,
  AppConfigViewIds,
  FormatterName,
} from "@/types/appConfig";
import type { DataTypeName, WorkOrderSubcategory } from "@/types/operations";

/**
 * Must stay equal to the `DataTypeName` union in `types/operations.ts`.
 * The compile-time check below fails if the two drift.
 */
const DATA_TYPE_KEYS = [
  "assets",
  "files",
  "work_orders",
  "time_series",
  "equipment",
] as const;

type AssertSame<A, B> = [A] extends [B]
  ? [B] extends [A]
    ? true
    : never
  : never;
type _DataTypeKeysMatch = AssertSame<
  (typeof DATA_TYPE_KEYS)[number],
  DataTypeName
>;
const _dataTypeKeysMatch: _DataTypeKeysMatch = true;
void _dataTypeKeysMatch;

const FORMATTER_NAMES: readonly FormatterName[] = ["assetsList", "isoDate"];

const VIEW_KEYS = [
  "summaryOutputFile",
  "file",
  "asset",
  "timeSeries",
  "activity",
  "equipment",
] as const satisfies readonly (keyof AppConfigViewIds)[];

const DATA_TYPE_VIEW_KEYS = [
  "file",
  "asset",
  "timeSeries",
  "activity",
  "equipment",
] as const;

const WORK_ORDER_SUBCATEGORIES = [
  "completed_work_orders",
  "ongoing_work_orders",
  "scheduled_work_orders",
] as const;

/** DMS instances.query limit ceiling. */
const MAX_INSTANCE_QUERY_LIMIT = 1000;

export class AppConfigError extends Error {
  public constructor(message: string, options?: { cause?: unknown }) {
    super(
      message,
      options?.cause !== undefined ? { cause: options.cause } : undefined,
    );
    this.name = "AppConfigError";
  }
}

function fail(path: string, message: string): never {
  const location = path === "" ? "config node" : `config node ${path}`;
  throw new AppConfigError(`${location}: ${message}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, path: string): Record<string, unknown> {
  if (!isRecord(value)) fail(path, "expected an object");
  return value;
}

function requireString(value: unknown, path: string): string {
  if (typeof value !== "string") fail(path, "expected a string");
  return value;
}

function requireNonEmptyString(value: unknown, path: string): string {
  const text = requireString(value, path);
  if (text.trim() === "") fail(path, "expected a non-empty string");
  return text;
}

function requirePositiveInt(
  value: unknown,
  path: string,
  max?: number,
): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    fail(path, "expected a positive integer");
  }
  if (max !== undefined && value > max) {
    fail(path, `expected an integer <= ${max}`);
  }
  return value;
}

function requireInt(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    fail(path, "expected an integer");
  }
  return value;
}

function requireBoolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") fail(path, "expected a boolean");
  return value;
}

function parseViewId(value: unknown, path: string) {
  const record = requireRecord(value, path);
  return {
    space: requireNonEmptyString(record.space, `${path}.space`),
    externalId: requireNonEmptyString(record.externalId, `${path}.externalId`),
    version: requireNonEmptyString(record.version, `${path}.version`),
  };
}

function parseTableColumn(value: unknown, path: string): AppConfigTableColumn {
  const record = requireRecord(value, path);
  const column: AppConfigTableColumn = {
    header: requireNonEmptyString(record.header, `${path}.header`),
    property: requireNonEmptyString(record.property, `${path}.property`),
  };
  if ("default" in record && record.default !== undefined) {
    column.default = requireString(record.default, `${path}.default`);
  }
  if ("formatter" in record && record.formatter !== undefined) {
    const name = requireString(record.formatter, `${path}.formatter`);
    if (!FORMATTER_NAMES.includes(name as FormatterName)) {
      fail(
        `${path}.formatter`,
        `unknown formatter "${name}". Expected one of: ${FORMATTER_NAMES.join(", ")}`,
      );
    }
    column.formatter = name as FormatterName;
  }
  return column;
}

function parseDataType(value: unknown, path: string): AppConfigDataType {
  const record = requireRecord(value, path);
  const view = requireString(record.view, `${path}.view`);
  if (
    !DATA_TYPE_VIEW_KEYS.includes(view as (typeof DATA_TYPE_VIEW_KEYS)[number])
  ) {
    fail(
      `${path}.view`,
      `unknown view "${view}". Expected one of: ${DATA_TYPE_VIEW_KEYS.join(", ")}`,
    );
  }
  const isSpecialCase = requireBoolean(
    record.isSpecialCase,
    `${path}.isSpecialCase`,
  );
  const columnsRaw = record.tableColumns;
  if (!Array.isArray(columnsRaw) || columnsRaw.length === 0) {
    fail(`${path}.tableColumns`, "expected a non-empty array");
  }
  const dataType: AppConfigDataType = {
    displayName: requireNonEmptyString(
      record.displayName,
      `${path}.displayName`,
    ),
    view: view as AppConfigDataType["view"],
    enabledByDefault: requireBoolean(
      record.enabledByDefault,
      `${path}.enabledByDefault`,
    ),
    isSpecialCase,
    tableColumns: columnsRaw.map((column, index) =>
      parseTableColumn(column, `${path}.tableColumns[${index}]`),
    ),
  };
  if (isSpecialCase) {
    if (
      !Array.isArray(record.subcategories) ||
      record.subcategories.length === 0
    ) {
      fail(
        `${path}.subcategories`,
        "expected a non-empty array when isSpecialCase is true",
      );
    }
    dataType.subcategories = record.subcategories.map((item, index) => {
      const name = requireString(item, `${path}.subcategories[${index}]`);
      if (
        !WORK_ORDER_SUBCATEGORIES.includes(
          name as (typeof WORK_ORDER_SUBCATEGORIES)[number],
        )
      ) {
        fail(
          `${path}.subcategories[${index}]`,
          `unknown subcategory "${name}"`,
        );
      }
      return name as WorkOrderSubcategory;
    });
  }
  return dataType;
}

/**
 * Validates config-node properties.
 * Throws `AppConfigError` naming the bad path.
 */
export function parseAppConfig(raw: unknown): AppConfig {
  const root = requireRecord(raw, "");

  const schemaVersion = requirePositiveInt(root.schemaVersion, "schemaVersion");
  if (schemaVersion !== SUPPORTED_SCHEMA_VERSION) {
    fail(
      "schemaVersion",
      `unsupported schemaVersion ${schemaVersion}. This release supports schemaVersion ${SUPPORTED_SCHEMA_VERSION}`,
    );
  }

  const spaces = requireRecord(root.spaces, "spaces");
  const prefixes = requireRecord(root.externalIdPrefixes, "externalIdPrefixes");
  const functions = requireRecord(root.functions, "functions");
  const summarizer = requireRecord(
    functions.summarizer,
    "functions.summarizer",
  );
  const agent = requireRecord(root.agent, "agent");
  const dataModel = requireRecord(root.dataModel, "dataModel");
  const viewsRaw = requireRecord(dataModel.views, "dataModel.views");
  const limits = requireRecord(root.limits, "limits");
  const retry = requireRecord(limits.retry, "limits.retry");
  const concurrency = requireRecord(limits.concurrency, "limits.concurrency");
  const dataTypesRaw = requireRecord(root.dataTypes, "dataTypes");

  const views = {} as AppConfigViewIds;
  for (const key of VIEW_KEYS) {
    views[key] = parseViewId(viewsRaw[key], `dataModel.views.${key}`);
  }

  const properties = dataModel.summaryListViewProperties;
  if (!Array.isArray(properties) || properties.length === 0) {
    fail(
      "dataModel.summaryListViewProperties",
      "expected a non-empty array of strings",
    );
  }
  const summaryListViewProperties = properties.map((item, index) =>
    requireNonEmptyString(
      item,
      `dataModel.summaryListViewProperties[${index}]`,
    ),
  );

  const dataTypeKeys = Object.keys(dataTypesRaw);
  const expected = [...DATA_TYPE_KEYS];
  const missing = expected.filter((key) => !dataTypeKeys.includes(key));
  const extra = dataTypeKeys.filter(
    (key) => !expected.includes(key as (typeof DATA_TYPE_KEYS)[number]),
  );
  if (missing.length > 0 || extra.length > 0) {
    fail(
      "dataTypes",
      `keys must be exactly [${expected.join(", ")}]. Missing: [${missing.join(", ")}]. Extra: [${extra.join(", ")}]`,
    );
  }

  const dataTypes = {} as AppConfig["dataTypes"];
  for (const key of DATA_TYPE_KEYS) {
    dataTypes[key] = parseDataType(dataTypesRaw[key], `dataTypes.${key}`);
  }

  const promptsRaw = requireRecord(root.prompts, "prompts");
  const promptTypes = requireRecord(promptsRaw.dataTypes, "prompts.dataTypes");
  const prompts = {
    header: requireNonEmptyString(promptsRaw.header, "prompts.header"),
    holistic: requireNonEmptyString(promptsRaw.holistic, "prompts.holistic"),
    agentInstructions: requireNonEmptyString(
      promptsRaw.agentInstructions,
      "prompts.agentInstructions",
    ),
    dataTypes: {} as AppConfig["prompts"]["dataTypes"],
  };
  for (const key of DATA_TYPE_KEYS) {
    prompts.dataTypes[key] = requireNonEmptyString(
      promptTypes[key],
      `prompts.dataTypes.${key}`,
    );
  }

  const defaultsRaw = requireRecord(root.defaults, "defaults");
  const presetsRaw = requireRecord(
    defaultsRaw.personaPresets,
    "defaults.personaPresets",
  );
  const personaPresets: Record<string, string[]> = {};
  for (const [key, value] of Object.entries(presetsRaw)) {
    if (!Array.isArray(value) || value.length === 0) {
      fail(
        `defaults.personaPresets.${key}`,
        "expected a non-empty array of data type names",
      );
    }
    personaPresets[key] = value.map((item, index) =>
      requireNonEmptyString(item, `defaults.personaPresets.${key}[${index}]`),
    );
  }

  const agentConcurrency = requirePositiveInt(
    concurrency.agent,
    "limits.concurrency.agent",
  );
  const agentMaxTasks = requirePositiveInt(
    agent.maxConcurrentTasks,
    "agent.maxConcurrentTasks",
  );
  if (agentMaxTasks !== agentConcurrency) {
    fail(
      "agent.maxConcurrentTasks",
      `must match limits.concurrency.agent (${agentConcurrency})`,
    );
  }

  return {
    schemaVersion,
    companyName: requireString(root.companyName, "companyName"),
    spaces: {
      summaries: requireNonEmptyString(spaces.summaries, "spaces.summaries"),
      dataInstances: requireNonEmptyString(
        spaces.dataInstances,
        "spaces.dataInstances",
      ),
    },
    externalIdPrefixes: {
      operationSummary: requireNonEmptyString(
        prefixes.operationSummary,
        "externalIdPrefixes.operationSummary",
      ),
      testReport: requireNonEmptyString(
        prefixes.testReport,
        "externalIdPrefixes.testReport",
      ),
      podcast: requireNonEmptyString(
        prefixes.podcast,
        "externalIdPrefixes.podcast",
      ),
    },
    functions: {
      summarizer: {
        externalId: requireNonEmptyString(
          summarizer.externalId,
          "functions.summarizer.externalId",
        ),
        pollIntervalMs: requirePositiveInt(
          summarizer.pollIntervalMs,
          "functions.summarizer.pollIntervalMs",
        ),
        timeoutMs: requirePositiveInt(
          summarizer.timeoutMs,
          "functions.summarizer.timeoutMs",
        ),
        maxConsecutivePollErrors: requirePositiveInt(
          summarizer.maxConsecutivePollErrors,
          "functions.summarizer.maxConsecutivePollErrors",
        ),
      },
    },
    agent: {
      name: requireNonEmptyString(agent.name, "agent.name"),
      externalIdSuffix: requireNonEmptyString(
        agent.externalIdSuffix,
        "agent.externalIdSuffix",
      ),
      defaultLanguageModel: requireNonEmptyString(
        agent.defaultLanguageModel,
        "agent.defaultLanguageModel",
      ),
      maxConcurrentTasks: agentMaxTasks,
    },
    dataModel: {
      views,
      summaryListViewProperties,
      cdfAlphaVersionHeader: requireNonEmptyString(
        dataModel.cdfAlphaVersionHeader,
        "dataModel.cdfAlphaVersionHeader",
      ),
    },
    limits: {
      chunkSize: requirePositiveInt(limits.chunkSize, "limits.chunkSize"),
      instanceQueryLimit: requirePositiveInt(
        limits.instanceQueryLimit,
        "limits.instanceQueryLimit",
        MAX_INSTANCE_QUERY_LIMIT,
      ),
      maxInstancesPerDatatype: requirePositiveInt(
        limits.maxInstancesPerDatatype,
        "limits.maxInstancesPerDatatype",
      ),
      summaryListPageSize: requirePositiveInt(
        limits.summaryListPageSize,
        "limits.summaryListPageSize",
      ),
      operationSummaryPickerMax: requirePositiveInt(
        limits.operationSummaryPickerMax,
        "limits.operationSummaryPickerMax",
      ),
      defaultPageSize: requirePositiveInt(
        limits.defaultPageSize,
        "limits.defaultPageSize",
      ),
      retry: {
        baseMs: requirePositiveInt(retry.baseMs, "limits.retry.baseMs"),
        maxDelayMs: requirePositiveInt(
          retry.maxDelayMs,
          "limits.retry.maxDelayMs",
        ),
      },
      concurrency: {
        default: requirePositiveInt(
          concurrency.default,
          "limits.concurrency.default",
        ),
        delete: requirePositiveInt(
          concurrency.delete,
          "limits.concurrency.delete",
        ),
        agent: agentConcurrency,
      },
    },
    dataTypes,
    prompts,
    defaults: {
      timezoneOffsetHours: requireInt(
        defaultsRaw.timezoneOffsetHours,
        "defaults.timezoneOffsetHours",
      ),
      summaryPeriodDays: requirePositiveInt(
        defaultsRaw.summaryPeriodDays,
        "defaults.summaryPeriodDays",
      ),
      personaPresets,
    },
  };
}

let loadedAppConfig: AppConfig | undefined;

/** Called once by `AppConfigProvider` after the config node loads. */
export function setAppConfig(config: AppConfig): void {
  loadedAppConfig = config;
}

/** Test helper. Production code loads the node instead of clearing it. */
export function clearAppConfig(): void {
  loadedAppConfig = undefined;
}

export function getAppConfig(): AppConfig {
  if (loadedAppConfig === undefined) {
    throw new AppConfigError(
      `config node ${CONFIG_NODE_PATH} has not been loaded`,
    );
  }
  return loadedAppConfig;
}
