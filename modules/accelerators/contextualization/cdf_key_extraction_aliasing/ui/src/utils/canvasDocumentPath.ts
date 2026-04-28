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
