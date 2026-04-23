/**
 * Read/write aliasing transform rules whether they live under ``aliasing.config.data.aliasing_rules``
 * or ``aliasing.config.data.pathways`` (sequential / parallel steps), matching the Python engine.
 */

import type { JsonObject } from "../../types/scopeConfig";

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/** True when ``pathways.steps`` is present (engine prefers pathways over flat ``aliasing_rules``). */
export function aliasingDataUsesPathways(data: Record<string, unknown> | undefined): boolean {
  if (!data) return false;
  const pw = data.pathways;
  if (!isRecord(pw)) return false;
  const steps = pw.steps;
  return Array.isArray(steps) && steps.length > 0;
}

function collectRulesFromPathwaysSteps(steps: unknown[]): unknown[] {
  const out: unknown[] = [];
  for (const step of steps) {
    if (!isRecord(step)) continue;
    const mode = String(step.mode || "sequential").trim().toLowerCase();
    if (mode === "sequential") {
      const rules = step.rules;
      if (Array.isArray(rules)) {
        for (const r of rules) out.push(r);
      }
    } else if (mode === "parallel") {
      const branches = step.branches;
      if (!Array.isArray(branches)) continue;
      for (const br of branches) {
        if (Array.isArray(br)) {
          for (const r of br) out.push(r);
        } else if (isRecord(br)) {
          const rules = br.rules;
          if (Array.isArray(rules)) {
            for (const r of rules) out.push(r);
          }
        }
      }
    }
  }
  return out;
}

/** All transform rule row objects (for seeding, lookup, reorder). */
export function getAliasingTransformRuleRows(data: Record<string, unknown> | undefined): unknown[] {
  if (!data) return [];
  if (aliasingDataUsesPathways(data)) {
    const steps = (data.pathways as Record<string, unknown>).steps;
    if (!Array.isArray(steps)) return [];
    return collectRulesFromPathwaysSteps(steps);
  }
  const flat = data.aliasing_rules;
  return Array.isArray(flat) ? [...flat] : [];
}

function clearStepRulesForRestShell(step: unknown): unknown {
  if (!isRecord(step)) return step;
  const mode = String(step.mode || "sequential").trim().toLowerCase();
  if (mode === "sequential") return { ...step, rules: [] };
  if (mode === "parallel") {
    const branches = step.branches;
    if (!Array.isArray(branches)) return { ...step };
    return {
      ...step,
      branches: branches.map((br) => {
        if (Array.isArray(br)) return [];
        if (isRecord(br)) return { ...br, rules: [] };
        return br;
      }),
    };
  }
  return step;
}

/**
 * ``aliasing.config.data`` with transform rule rows removed (for editors that split ``rest`` + rule list).
 * Drops ``aliasing_rules`` and clears rule arrays inside ``pathways`` steps.
 */
export function withoutTransformRuleRows(data: JsonObject): JsonObject {
  const d: Record<string, unknown> = { ...data };
  delete d.aliasing_rules;
  if (aliasingDataUsesPathways(d)) {
    const pw = d.pathways as Record<string, unknown>;
    const steps = Array.isArray(pw.steps) ? pw.steps.map(clearStepRulesForRestShell) : [];
    return { ...d, pathways: { ...pw, steps } } as JsonObject;
  }
  return d as JsonObject;
}

/** Replace transform rules on ``aliasing.config.data`` (flat list or first sequential ``pathways`` step). */
export function replaceAliasingTransformRulesInData(
  data: Record<string, unknown>,
  nextRules: unknown[]
): JsonObject {
  const d = { ...data };

  if (aliasingDataUsesPathways(d)) {
    const pw = { ...(d.pathways as Record<string, unknown>) };
    const stepsRaw = pw.steps;
    const steps = Array.isArray(stepsRaw) ? [...stepsRaw] : [];
    let applied = false;
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      if (!isRecord(step)) continue;
      const mode = String(step.mode || "sequential").trim().toLowerCase();
      if (mode === "sequential") {
        steps[i] = { ...step, rules: [...nextRules] };
        applied = true;
        break;
      }
    }
    if (!applied) {
      steps.unshift({ mode: "sequential", rules: [...nextRules] });
    }
    return { ...d, pathways: { ...pw, steps } } as JsonObject;
  }

  return { ...d, aliasing_rules: [...nextRules] } as JsonObject;
}

/** Replace the full transform rule list (flat array or first sequential ``pathways`` step rules). */
export function replaceAliasingTransformRulesInDoc(
  doc: Record<string, unknown>,
  nextRules: unknown[]
): Record<string, unknown> {
  const al = doc.aliasing;
  if (!isRecord(al)) return doc;
  const cfg = al.config;
  if (!isRecord(cfg)) return doc;
  const data = isRecord(cfg.data) ? { ...cfg.data } : {};
  const nextData = replaceAliasingTransformRulesInData(data, nextRules);
  return {
    ...doc,
    aliasing: {
      ...al,
      config: {
        ...cfg,
        data: nextData,
      },
    },
  };
}

/** Append one rule row to flat ``aliasing_rules`` or the first sequential ``pathways`` step. */
export function appendAliasingTransformRuleRow(doc: Record<string, unknown>, newRule: JsonObject): Record<string, unknown> {
  const al = doc.aliasing;
  if (!isRecord(al)) {
    return {
      ...doc,
      aliasing: {
        config: {
          data: {
            aliasing_rules: [newRule],
          },
        },
      },
    };
  }
  const cfg = al.config;
  if (!isRecord(cfg)) {
    return {
      ...doc,
      aliasing: {
        ...al,
        config: {
          data: {
            aliasing_rules: [newRule],
          },
        },
      },
    };
  }
  const data = cfg.data;
  if (!isRecord(data)) {
    return {
      ...doc,
      aliasing: {
        ...al,
        config: {
          ...cfg,
          data: {
            aliasing_rules: [newRule],
          },
        },
      },
    };
  }
  const existing = getAliasingTransformRuleRows(data) as JsonObject[];
  return replaceAliasingTransformRulesInDoc(doc, [...existing, newRule]);
}
