/** Utilities for trigger flow editing (merge trimmed trigger configuration with full scope roots). */

const SCOPE_ROOT_KEYS_FOR_FLOW_SEED = ["aliasing_rule_definitions", "aliasing_rule_sequences"] as const;

function isNonEmptyRecord(v: unknown): boolean {
  return v !== null && typeof v === "object" && !Array.isArray(v) && Object.keys(v as object).length > 0;
}

function isNonEmptyArray(v: unknown): boolean {
  return Array.isArray(v) && v.length > 0;
}

/**
 * WorkflowTrigger ``input.configuration`` is often a trimmed copy of the site scope. If
 * ``aliasing_rule_definitions`` / ``aliasing_rule_sequences`` are missing or empty there but present
 * on the loaded workflow scope or template doc, overlay them so the flow editor and
 * ``syncWorkflowScopeFromCanvas`` resolve the same rule names as the main scope editor.
 */
export function mergeScopeRootsForTriggerFlowSeed(
  triggerConfiguration: Record<string, unknown>,
  ...fallbacks: Array<Record<string, unknown> | undefined>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...triggerConfiguration };
  for (const key of SCOPE_ROOT_KEYS_FOR_FLOW_SEED) {
    const cur = out[key];
    const has =
      key === "aliasing_rule_sequences"
        ? isNonEmptyRecord(cur) || isNonEmptyArray(cur)
        : isNonEmptyRecord(cur);
    if (has) continue;
    for (const fb of fallbacks) {
      if (!fb) continue;
      const cand = fb[key];
      if (key === "aliasing_rule_sequences") {
        if (isNonEmptyRecord(cand) || isNonEmptyArray(cand)) {
          out[key] = cand;
          break;
        }
      } else if (isNonEmptyRecord(cand)) {
        out[key] = cand;
        break;
      }
    }
  }
  return out;
}
