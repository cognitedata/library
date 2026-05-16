/** Optional top-level `name` on module / workflow / trigger YAML for operator UI labels. */
export function displayNameFromRoot(doc: Record<string, unknown>): string | undefined {
  const n = doc.name;
  if (typeof n !== "string") return undefined;
  if (n.trim().length === 0) return undefined;
  return n;
}
