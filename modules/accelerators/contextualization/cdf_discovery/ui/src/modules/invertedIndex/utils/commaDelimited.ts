export function splitCommaSegments(s: string): string[] {
  return s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

export function commaJoinSegments(parts: readonly string[]): string {
  return parts
    .map((x) => String(x).trim())
    .filter(Boolean)
    .join(", ");
}
