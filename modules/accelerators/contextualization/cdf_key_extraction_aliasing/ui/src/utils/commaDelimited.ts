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
