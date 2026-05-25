/** Helpers for merge node ``data.config`` summaries and validation hints. */

export function readMergeFieldPolicies(config: Record<string, unknown>): unknown[] {
  const raw = config.field_policies ?? config.save_field_policies;
  return Array.isArray(raw) ? raw : [];
}

export function mergeFieldPolicyCount(config: Record<string, unknown>): number {
  return readMergeFieldPolicies(config).length;
}
