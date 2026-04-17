/** Next unused `prefix_N` name from existing rules (for required rule names). */
export function nextSequentialRuleName(prefix: string, existing: readonly { name: string }[]): string {
  const used = new Set(existing.map((r) => r.name.trim().toLowerCase()).filter(Boolean));
  for (let n = 1; n < 10000; n++) {
    const c = `${prefix}_${n}`;
    if (!used.has(c.toLowerCase())) return c;
  }
  return `${prefix}_${Date.now()}`;
}

export function ruleNameOrDefault(raw: string, indexOneBased: number, fallbackPrefix: string): string {
  const s = raw.trim();
  return s || `${fallbackPrefix}_${indexOneBased}`;
}
