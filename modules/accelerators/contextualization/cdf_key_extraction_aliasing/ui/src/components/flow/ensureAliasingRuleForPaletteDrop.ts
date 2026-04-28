import YAML from "yaml";
import type { JsonObject } from "../../types/scopeConfig";
import { defaultConfigYamlForAliasingHandler } from "../../utils/ruleHandlerTemplates";
import { nextSequentialRuleName, sanitizeRuleNamePrefix } from "../../utils/ruleNaming";
import { appendAliasingTransformRuleRow, getAliasingTransformRuleRows } from "./aliasingScopeData";

function getAliasingRulesArray(doc: Record<string, unknown>): JsonObject[] {
  const al = doc.aliasing;
  if (!al || typeof al !== "object" || Array.isArray(al)) return [];
  const config = (al as Record<string, unknown>).config;
  if (!config || typeof config !== "object" || Array.isArray(config)) return [];
  const data = (config as Record<string, unknown>).data;
  if (!data || typeof data !== "object" || Array.isArray(data)) return [];
  const rows = getAliasingTransformRuleRows(data as Record<string, unknown>);
  const out: JsonObject[] = [];
  for (const r of rows) {
    if (r !== null && typeof r === "object" && !Array.isArray(r)) out.push(r as JsonObject);
  }
  return out;
}

/**
 * Append a new aliasing transform rule row (``aliasing_rules`` or first sequential ``pathways`` step)
 * for a palette handler drop (same shape as AliasingRulesStructuredEditor serialization).
 */
export function appendAliasingRuleForHandler(
  doc: Record<string, unknown>,
  handlerId: string
): { doc: Record<string, unknown>; ruleName: string } {
  const existing = getAliasingRulesArray(doc);
  const nameSeeds = existing.map((r, i) => ({
    name: r.name != null ? String(r.name) : `aliasing_rule_${i + 1}`,
  }));
  const namePrefix = sanitizeRuleNamePrefix(handlerId, "aliasing_rule");
  const ruleName = nextSequentialRuleName(namePrefix, nameSeeds);

  let configParsed: JsonObject = {};
  try {
    const raw = YAML.parse(defaultConfigYamlForAliasingHandler(handlerId));
    configParsed =
      raw !== null && typeof raw === "object" && !Array.isArray(raw) ? (raw as JsonObject) : {};
  } catch {
    configParsed = {};
  }

  const priority = (existing.length + 1) * 10;

  const newRule: JsonObject = {
    name: ruleName,
    handler: handlerId,
    enabled: true,
    priority,
    preserve_original: true,
    config: configParsed,
    conditions: {},
    scope_filters: {},
  };

  return {
    doc: appendAliasingTransformRuleRow(doc, newRule),
    ruleName,
  };
}
