import catalogJson from "../generated/scorePatternCatalog.json";

const catalog = catalogJson as Record<string, string>;

/** Optional area/unit prefix before ISA tag body (matches ``tag_patterns.yaml``). */
export const ISA_TAG_AREA_PREFIX = String.raw`(?:(?:[A-Za-z0-9]{1,12}|\d{1,8})[-_/]){0,4}`;

/** Trim pattern text as stored in scoring rule YAML. */
export function normalizeScorePatternKey(pattern: string): string {
  return pattern.trim();
}

export function shouldApplyIsaTagAreaPrefix(pattern: string): boolean {
  const p = normalizeScorePatternKey(pattern);
  if (!p || p.includes(ISA_TAG_AREA_PREFIX)) return false;
  if (p.startsWith("^") || p.startsWith("(?s)")) return false;
  if (p.startsWith("[") || p.startsWith("'[") || p.startsWith("'[^")) return false;
  return p.includes(String.raw`\b`);
}

/** Pattern used at score runtime (short ISA suffix → optional area prefix). */
export function withOptionalIsaTagAreaPrefix(pattern: string): string {
  const p = normalizeScorePatternKey(pattern);
  if (!p || !shouldApplyIsaTagAreaPrefix(p)) return p;
  return ISA_TAG_AREA_PREFIX + p;
}

/** Description from cdf_discovery_aliasing tag_patterns / validation workflows (exact pattern match). */
export function lookupScorePatternDescription(pattern: string): string | undefined {
  const key = normalizeScorePatternKey(pattern);
  if (!key) return undefined;
  const direct = catalog[key]?.trim();
  if (direct) return direct;
  const prefixed = withOptionalIsaTagAreaPrefix(key);
  if (prefixed !== key) {
    const fromPrefixed = catalog[prefixed]?.trim();
    if (fromPrefixed) return fromPrefixed;
  }
  if (key.startsWith(ISA_TAG_AREA_PREFIX)) {
    const suffix = key.slice(ISA_TAG_AREA_PREFIX.length).trim();
    const fromSuffix = catalog[suffix]?.trim();
    if (fromSuffix) return fromSuffix;
  }
  return undefined;
}

export function enrichExpressionDescriptions(
  expressions: Array<{ pattern: string; description: string }>,
): Array<{ pattern: string; description: string }> {
  return expressions.map((row) => {
    if (row.description.trim() || !row.pattern.trim()) return row;
    const desc = lookupScorePatternDescription(row.pattern);
    return desc ? { ...row, description: desc } : row;
  });
}
