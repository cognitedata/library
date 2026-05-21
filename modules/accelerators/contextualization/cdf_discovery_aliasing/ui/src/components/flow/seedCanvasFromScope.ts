/** Utilities for trigger flow editing (merge trimmed trigger configuration with full scope roots). */

export function mergeScopeRootsForTriggerFlowSeed(
  triggerConfiguration: Record<string, unknown>,
  ...fallbacks: Array<Record<string, unknown> | undefined>
): Record<string, unknown> {
  return { ...triggerConfiguration };
}
