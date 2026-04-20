import YAML from "yaml";
import type { JsonObject } from "../../types/scopeConfig";
import { defaultConfigYamlForAliasingHandler } from "../../utils/ruleHandlerTemplates";
import { nextSequentialRuleName, sanitizeRuleNamePrefix } from "../../utils/ruleNaming";

function getAliasingRulesArray(doc: Record<string, unknown>): JsonObject[] {
  const al = doc.aliasing;
  if (!al || typeof al !== "object" || Array.isArray(al)) return [];
  const config = (al as Record<string, unknown>).config;
  if (!config || typeof config !== "object" || Array.isArray(config)) return [];
  const data = (config as Record<string, unknown>).data;
  if (!data || typeof data !== "object" || Array.isArray(data)) return [];
  const rules = (data as Record<string, unknown>).aliasing_rules;
  if (!Array.isArray(rules)) return [];
  const out: JsonObject[] = [];
  for (const r of rules) {
    if (r !== null && typeof r === "object" && !Array.isArray(r)) out.push(r as JsonObject);
  }
  return out;
}

function patchAliasingRulesArray(doc: Record<string, unknown>, rules: JsonObject[]): Record<string, unknown> {
  const al = doc.aliasing;
  if (!al || typeof al !== "object" || Array.isArray(al)) {
    return {
      ...doc,
      aliasing: {
        config: {
          data: {
            aliasing_rules: rules,
          },
        },
      },
    };
  }
  const alObj = al as Record<string, unknown>;
  const config = alObj.config;
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    return {
      ...doc,
      aliasing: {
        ...alObj,
        config: {
          data: {
            aliasing_rules: rules,
          },
        },
      },
    };
  }
  const cfgObj = config as Record<string, unknown>;
  const data = cfgObj.data;
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return {
      ...doc,
      aliasing: {
        ...alObj,
        config: {
          ...cfgObj,
          data: {
            aliasing_rules: rules,
          },
        },
      },
    };
  }
  const dataObj = data as Record<string, unknown>;
  return {
    ...doc,
    aliasing: {
      ...alObj,
      config: {
        ...cfgObj,
        data: {
          ...dataObj,
          aliasing_rules: rules,
        },
      },
    },
  };
}

/**
 * Append a new `aliasing.config.data.aliasing_rules[]` row for a palette handler drop
 * (same shape as AliasingRulesStructuredEditor serialization).
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
    doc: patchAliasingRulesArray(doc, [...existing, newRule]),
    ruleName,
  };
}
