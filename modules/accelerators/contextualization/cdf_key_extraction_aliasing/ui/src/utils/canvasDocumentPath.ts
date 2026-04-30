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
 * Leaf WorkflowTrigger and scoped canvas share the same stem; the build writes
 * `workflows/<suffix>/<base>.<suffix>.canvas.yaml` (see scope_canvas_copy). There is
 * not always a real `*.config.yaml` file under `workflows/`, but
 * `GET /api/canvas-document/model?rel=…` resolves the synthetic sibling
 * `…/<base>.<suffix>.config.yaml` to the same paired `…canvas.yaml` as local/template
 * (see server `_canvas_document_path`).
 *
 * Suffix matching is ASCII case-insensitive so paths align with ``main.py`` ``_is_workflow_trigger_path``
 * (e.g. ``.workflowtrigger.yaml`` on disk still maps to the sibling ``.config.yaml``).
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

/** ``workflows/site_01/key_extraction_aliasing.site_01.canvas.yaml`` → paired trigger path. */
export function workflowTriggerRelFromScopedCanvasRel(canvasPath: string): string | null {
  const t = canvasPath.trim();
  if (!t.toLowerCase().startsWith("workflows/")) return null;
  const yamlStem = stripSuffixAsciiCaseInsensitive(t, ".canvas.yaml");
  if (yamlStem != null) {
    return `${yamlStem}.WorkflowTrigger.yaml`;
  }
  const ymlStem = stripSuffixAsciiCaseInsensitive(t, ".canvas.yml");
  if (ymlStem != null) {
    return `${ymlStem}.WorkflowTrigger.yml`;
  }
  return null;
}

/**
 * Derive sibling canvas layout path from a scope document path:
 *   workflow.local.config.yaml → workflow.local.canvas.yaml
 *   workflow_template/workflow.template.config.yaml → workflow_template/workflow.template.canvas.yaml
 */

export function scopeRelToCanvasRel(scopeRel: string): string {
  const s = scopeRel.trim();
  if (/\.config\.yaml$/i.test(s)) {
    return s.replace(/\.config\.yaml$/i, ".canvas.yaml");
  }
  if (/\.config\.yml$/i.test(s)) {
    return s.replace(/\.config\.yml$/i, ".canvas.yml");
  }
  const m = s.match(/^(.*)\.(yaml|yml)$/i);
  if (m) {
    return `${m[1]}.canvas.${m[2].toLowerCase()}`;
  }
  return `${s}.canvas.yaml`;
}
