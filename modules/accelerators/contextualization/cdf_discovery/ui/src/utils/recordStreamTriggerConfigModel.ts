import type { JsonObject } from "../types/jsonConfig";
import { readRecordsFilter, readRecordsSources } from "./recordsQueryConfigModel";

export type RecordStreamTriggerFields = {
  streamExternalId: string;
  batchSize: number;
  batchTimeout: number;
  filter: JsonObject | null;
  sources: JsonObject[];
};

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_BATCH_TIMEOUT = 60;

export function readRecordStreamTriggerFromStart(cfg: Record<string, unknown>): RecordStreamTriggerFields {
  const rule =
    cfg.trigger_rule && typeof cfg.trigger_rule === "object" && !Array.isArray(cfg.trigger_rule)
      ? (cfg.trigger_rule as Record<string, unknown>)
      : {};
  const streamExternalId = String(cfg.stream_external_id ?? rule.streamExternalId ?? "").trim();
  const batchRaw = cfg.batch_size ?? rule.batchSize;
  let batchSize = DEFAULT_BATCH_SIZE;
  if (batchRaw !== undefined && batchRaw !== null && batchRaw !== "") {
    const n = typeof batchRaw === "number" ? batchRaw : parseInt(String(batchRaw), 10);
    if (Number.isFinite(n)) batchSize = Math.max(1, Math.min(1000, Math.floor(n)));
  }
  const timeoutRaw = cfg.batch_timeout ?? rule.batchTimeout;
  let batchTimeout = DEFAULT_BATCH_TIMEOUT;
  if (timeoutRaw !== undefined && timeoutRaw !== null && timeoutRaw !== "") {
    const n = typeof timeoutRaw === "number" ? timeoutRaw : parseInt(String(timeoutRaw), 10);
    if (Number.isFinite(n)) batchTimeout = Math.max(10, Math.min(86400, Math.floor(n)));
  }
  const filter =
    readRecordsFilter(cfg) ??
    (rule.filter && typeof rule.filter === "object" && !Array.isArray(rule.filter)
      ? (rule.filter as JsonObject)
      : null);
  const sources = readRecordsSources(cfg).length ? readRecordsSources(cfg) : readRecordsSources(rule);
  return { streamExternalId, batchSize, batchTimeout, filter, sources };
}

export function mergeRecordStreamTriggerIntoStart(
  cfg: Record<string, unknown>,
  fields: RecordStreamTriggerFields
): Record<string, unknown> {
  const next: Record<string, unknown> = {
    ...cfg,
    trigger_type: "recordStream",
    stream_external_id: fields.streamExternalId,
    batch_size: fields.batchSize,
    batch_timeout: fields.batchTimeout,
  };
  if (fields.filter && Object.keys(fields.filter).length) {
    next.filter = fields.filter;
  } else {
    delete next.filter;
  }
  if (fields.sources.length) {
    next.sources = fields.sources;
  } else {
    delete next.sources;
  }
  const ruleBase =
    cfg.trigger_rule && typeof cfg.trigger_rule === "object" && !Array.isArray(cfg.trigger_rule)
      ? { ...(cfg.trigger_rule as Record<string, unknown>) }
      : {};
  const rule: Record<string, unknown> = {
    ...ruleBase,
    triggerType: "recordStream",
    streamExternalId: fields.streamExternalId,
    batchSize: fields.batchSize,
    batchTimeout: fields.batchTimeout,
  };
  if (fields.filter && Object.keys(fields.filter).length) rule.filter = fields.filter;
  else delete rule.filter;
  if (fields.sources.length) rule.sources = fields.sources;
  else delete rule.sources;
  next.trigger_rule = rule;
  return next;
}
