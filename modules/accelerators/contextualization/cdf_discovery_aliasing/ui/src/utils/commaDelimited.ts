/** Split a comma-delimited string into trimmed non-empty segments (same as Source Views filters / include properties). */
export function splitCommaSegments(s: string): string[] {
  return s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

/** Join string segments for display in a comma-delimited single-line editor. */
export function commaJoinSegments(parts: readonly string[]): string {
  return parts
    .map((x) => String(x).trim())
    .filter(Boolean)
    .join(", ");
}

/** Format ``split_join`` ``indexes[]`` for a comma-delimited editor. */
export function formatSplitJoinIndexes(indexes: unknown): string {
  if (!Array.isArray(indexes)) return "";
  return indexes
    .map((x) => String(x).trim())
    .filter((s) => s.length > 0)
    .join(", ");
}

/** Parse comma/semicolon-separated 0-based token indexes for ``split_join``. */
export function parseSplitJoinIndexes(raw: string): number[] | undefined {
  const s = raw.trim();
  if (!s) return undefined;
  const indexes = s
    .split(/[,;]+/)
    .map((x) => x.trim())
    .filter(Boolean)
    .map((x) => Number(x))
    .filter((n) => Number.isFinite(n));
  return indexes.length ? indexes : undefined;
}
