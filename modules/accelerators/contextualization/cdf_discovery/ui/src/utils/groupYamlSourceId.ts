import yaml from "js-yaml";

/** Read CDF group ``name`` from generated Group YAML (used for source_ids map keys). */
export function groupNameFromYaml(text: string): string | null {
  try {
    const doc = yaml.load(text);
    if (doc && typeof doc === "object" && doc !== null && "name" in doc) {
      const name = String((doc as { name: unknown }).name ?? "").trim();
      return name || null;
    }
  } catch {
    /* invalid yaml */
  }
  return null;
}

export function literalSourceIdFromGroupYaml(text: string): string | null {
  try {
    const doc = yaml.load(text);
    if (!doc || typeof doc !== "object") return null;
    const sid = (doc as { sourceId?: unknown }).sourceId;
    if (sid == null) return null;
    const s = String(sid).trim();
    if (!s || s.includes("{{")) return null;
    return s;
  } catch {
    return null;
  }
}
