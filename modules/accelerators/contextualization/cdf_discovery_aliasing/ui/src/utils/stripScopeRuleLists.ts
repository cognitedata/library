/** Remove removed rule-list keys from a v1 scope document (scope save / canvas sync). */

export function stripScopeRuleListsFromScopeDoc(doc: Record<string, unknown>): Record<string, unknown> {
  const out = { ...doc };
  for (const k of [
    "aliasing_rule_definitions",
    "aliasing_rule_sequences",
    "extraction_rule_definitions",
    "extraction_rule_sequences",
  ]) {
    delete out[k];
  }
  delete out.associations;

  const ke = out.key_extraction;
  if (ke && typeof ke === "object" && !Array.isArray(ke)) {
    const cfg = (ke as Record<string, unknown>).config;
    if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
      const data = (cfg as Record<string, unknown>).data;
      if (data && typeof data === "object" && !Array.isArray(data)) {
        delete (data as Record<string, unknown>).extraction_rules;
      }
    }
  }

  const al = out.aliasing;
  if (al && typeof al === "object" && !Array.isArray(al)) {
    const cfg = (al as Record<string, unknown>).config;
    if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
      const data = (cfg as Record<string, unknown>).data;
      if (data && typeof data === "object" && !Array.isArray(data)) {
        const d = data as Record<string, unknown>;
        delete d.aliasing_rules;
        delete d.pathways;
      }
    }
  }

  return out;
}
