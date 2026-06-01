import type { JsonObject } from "../types/jsonConfig";

export type DataModelingTriggerFields = {
  batchSize: number;
  batchTimeout: number;
  dataModelingQueryText: string;
};

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_BATCH_TIMEOUT = 60;

function asRule(cfg: Record<string, unknown>): Record<string, unknown> {
  const raw = cfg.trigger_rule;
  if (raw && typeof raw === "object" && !Array.isArray(raw)) return raw as Record<string, unknown>;
  return {};
}

function readInt(raw: unknown, fallback: number, min: number, max: number): number {
  if (raw === undefined || raw === null || raw === "") return fallback;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

export function readDataModelingTriggerFromStart(cfg: Record<string, unknown>): DataModelingTriggerFields {
  const rule = asRule(cfg);
  const batchSize = readInt(cfg.batch_size ?? cfg.batchSize ?? rule.batchSize, DEFAULT_BATCH_SIZE, 1, 1000);
  const batchTimeout = readInt(
    cfg.batch_timeout ?? cfg.batchTimeout ?? rule.batchTimeout,
    DEFAULT_BATCH_TIMEOUT,
    10,
    86400
  );
  const queryRaw = cfg.data_modeling_query ?? cfg.dataModelingQuery ?? rule.dataModelingQuery;
  const dataModelingQueryText =
    queryRaw && typeof queryRaw === "object" && !Array.isArray(queryRaw)
      ? JSON.stringify(queryRaw, null, 2)
      : "";
  return { batchSize, batchTimeout, dataModelingQueryText };
}

export function mergeDataModelingTriggerIntoStart(
  cfg: Record<string, unknown>,
  fields: DataModelingTriggerFields
): Record<string, unknown> {
  const next: Record<string, unknown> = {
    ...cfg,
    trigger_type: "dataModeling",
    batch_size: fields.batchSize,
    batch_timeout: fields.batchTimeout,
    batchSize: fields.batchSize,
    batchTimeout: fields.batchTimeout,
  };

  const ruleBase = asRule(cfg);
  const rule: JsonObject = {
    ...ruleBase,
    triggerType: "dataModeling",
    batchSize: fields.batchSize,
    batchTimeout: fields.batchTimeout,
  };

  const queryText = fields.dataModelingQueryText.trim();
  if (queryText) {
    try {
      const parsed = JSON.parse(queryText);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        next.data_modeling_query = parsed as JsonObject;
        next.dataModelingQuery = parsed as JsonObject;
        rule.dataModelingQuery = parsed as JsonObject;
      }
    } catch {
      // Keep previous query if parsing fails while editing.
    }
  } else {
    delete next.data_modeling_query;
    delete next.dataModelingQuery;
    delete rule.dataModelingQuery;
  }

  next.trigger_rule = rule;
  return next;
}
