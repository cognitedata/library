/**
 * Strip a suffix from ``s`` when ``s`` ends with that suffix ignoring ASCII case.
 * Returns the prefix (without suffix) or null if no match. ``suffix`` should be lower-case.
 */
function stripSuffixAsciiCaseInsensitive(s: string, suffixLower: string): string | null {
  const t = s.trim();
  if (t.length < suffixLower.length) return null;
  if (!t.toLowerCase().endsWith(suffixLower)) return null;
  return t.slice(0, -suffixLower.length);
}

/**
 * Map a scoped WorkflowTrigger path to a synthetic ``*.config.yaml`` rel (same stem) for APIs
 * that accept a scope document path. Canvas lives under ``canvas`` inside ``input.configuration`` on
 * the trigger, not in a separate file on disk.
 *
 * Suffix matching is ASCII case-insensitive so paths align with ``main.py`` ``_is_workflow_trigger_path``.
 */
export function scopeConfigRelFromWorkflowTriggerPath(triggerPath: string): string | null {
  const yamlStem = stripSuffixAsciiCaseInsensitive(triggerPath, ".workflowtrigger.yaml");
  if (yamlStem != null) {
    return `${yamlStem}.config.yaml`;
  }
  const ymlStem = stripSuffixAsciiCaseInsensitive(triggerPath, ".workflowtrigger.yml");
  if (ymlStem != null) {
    return `${ymlStem}.config.yml`;
  }
  return null;
}
