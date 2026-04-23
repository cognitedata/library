import type { JsonObject } from "../../types/scopeConfig";
import {
  getAliasingTransformRuleRows,
  replaceAliasingTransformRulesInDoc,
} from "./aliasingScopeData";

/** Update `source_views[i].filters` in the workflow scope document. */
export function patchSourceViewFilters(
  doc: Record<string, unknown>,
  index: number,
  filters: JsonObject[]
): Record<string, unknown> {
  const svs = doc.source_views;
  if (!Array.isArray(svs) || index < 0 || index >= svs.length) return doc;
  const nextSvs = svs.map((v, i) => {
    if (i !== index) return v;
    if (!v || typeof v !== "object" || Array.isArray(v)) return { filters };
    return { ...(v as JsonObject), filters };
  });
  return { ...doc, source_views: nextSvs };
}

function patchKeyedRulesScopeFilters(
  doc: Record<string, unknown>,
  topKey: "key_extraction" | "aliasing",
  rulesKey: "extraction_rules" | "aliasing_rules",
  ruleName: string,
  scope_filters: Record<string, unknown>
): Record<string, unknown> {
  const block = doc[topKey];
  if (!block || typeof block !== "object" || Array.isArray(block)) return doc;
  const config = (block as Record<string, unknown>).config;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = (config as Record<string, unknown>).data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  const rules = data[rulesKey];
  if (!Array.isArray(rules)) return doc;
  let found = false;
  const nextRules = rules.map((r) => {
    if (!r || typeof r !== "object" || Array.isArray(r)) return r;
    const row = r as Record<string, unknown>;
    const name = row.name != null ? String(row.name) : "";
    if (name === ruleName) {
      found = true;
      return { ...row, scope_filters };
    }
    return r;
  });
  if (!found) return doc;
  return {
    ...doc,
    [topKey]: {
      ...(block as Record<string, unknown>),
      config: {
        ...(config as Record<string, unknown>),
        data: {
          ...(data as Record<string, unknown>),
          [rulesKey]: nextRules,
        },
      },
    },
  };
}

export function patchExtractionRuleScopeFilters(
  doc: Record<string, unknown>,
  ruleName: string,
  scope_filters: Record<string, unknown>
): Record<string, unknown> {
  return patchKeyedRulesScopeFilters(doc, "key_extraction", "extraction_rules", ruleName, scope_filters);
}

export function patchAliasingRuleScopeFilters(
  doc: Record<string, unknown>,
  ruleName: string,
  scope_filters: Record<string, unknown>
): Record<string, unknown> {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  const cfg = al?.config as Record<string, unknown> | undefined;
  const data = cfg?.data as Record<string, unknown> | undefined;
  if (!data) return doc;
  const rows = getAliasingTransformRuleRows(data);
  let found = false;
  const next = rows.map((r) => {
    if (!r || typeof r !== "object" || Array.isArray(r)) return r;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() !== ruleName) return r;
    found = true;
    return { ...row, scope_filters };
  });
  if (!found) return doc;
  return replaceAliasingTransformRulesInDoc(doc, next);
}

/** Set ``key_extraction.config.data.extraction_rules[name].aliasing_pipeline`` (per-extraction tag transform tree). */
export function patchExtractionRuleAliasingPipeline(
  doc: Record<string, unknown>,
  ruleName: string,
  aliasing_pipeline: unknown[]
): Record<string, unknown> {
  const ke = doc.key_extraction;
  if (!ke || typeof ke !== "object" || Array.isArray(ke)) return doc;
  const config = (ke as Record<string, unknown>).config;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = (config as Record<string, unknown>).data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  const rules = data.extraction_rules;
  if (!Array.isArray(rules)) return doc;
  let found = false;
  const nextRules = rules.map((r) => {
    if (!r || typeof r !== "object" || Array.isArray(r)) return r;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "") !== ruleName) return r;
    found = true;
    return { ...row, aliasing_pipeline };
  });
  if (!found) return doc;
  return {
    ...doc,
    key_extraction: {
      ...(ke as Record<string, unknown>),
      config: {
        ...(config as Record<string, unknown>),
        data: {
          ...data,
          extraction_rules: nextRules,
        },
      },
    },
  };
}

export function getExtractionRuleScopeFilters(
  doc: Record<string, unknown>,
  ruleName: string
): Record<string, unknown> | undefined {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  const data = ke?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  const rules = d?.extraction_rules;
  if (!Array.isArray(rules)) return undefined;
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "") === ruleName) {
      const sf = row.scope_filters;
      if (sf !== null && typeof sf === "object" && !Array.isArray(sf)) return sf as Record<string, unknown>;
      return {};
    }
  }
  return undefined;
}

export function getAliasingRuleScopeFilters(
  doc: Record<string, unknown>,
  ruleName: string
): Record<string, unknown> | undefined {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  const data = al?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  if (!d) return undefined;
  const rules = getAliasingTransformRuleRows(d);
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "") === ruleName) {
      const sf = row.scope_filters;
      if (sf !== null && typeof sf === "object" && !Array.isArray(sf)) return sf as Record<string, unknown>;
      return {};
    }
  }
  return undefined;
}

