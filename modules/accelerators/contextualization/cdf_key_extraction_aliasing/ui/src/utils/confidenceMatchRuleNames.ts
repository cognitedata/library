/**
 * Resolve `validation.validation_rules` entries to rule ids for UI / canvas seeding.
 * Handles string refs, `{ ref }`, `{ name }`, `{ sequence: id }`, shorthand
 * `{ first_rule_id: [ tail... ] }`, and nested `{ hierarchy: { mode, children } }`.
 */

function validationRulesArrayFromBlock(v: Record<string, unknown>): unknown[] | undefined {
  const vr = v.validation_rules;
  if (Array.isArray(vr)) return vr;
  const legacy = v.confidence_match_rules;
  if (Array.isArray(legacy)) return legacy;
  return undefined;
}

const SHORTHAND_EXCLUDE_KEYS = new Set([
  "hierarchy",
  "ref",
  "sequence",
  "match",
  "name",
  "enabled",
  "priority",
  "expression_match",
  "confidence_modifier",
  "pipeline_input",
  "pipeline_output",
]);

/** Single-key `{ rule_id: [ tail... ] }` form (not an inline rule body). */
export function isShorthandConfidenceMatchChain(o: Record<string, unknown>): boolean {
  const keys = Object.keys(o);
  if (keys.length !== 1) return false;
  const k = keys[0]!;
  if (SHORTHAND_EXCLUDE_KEYS.has(k)) return false;
  return Array.isArray(o[k]);
}

function dedupePreserveOrder(names: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const n of names) {
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

function expandSequenceEntries(seqList: unknown[]): string[] {
  const out: string[] = [];
  for (const x of seqList) {
    if (typeof x === "string") {
      const s = x.trim();
      if (s) out.push(s);
      continue;
    }
    if (!x || typeof x !== "object" || Array.isArray(x)) continue;
    const xo = x as Record<string, unknown>;
    if (xo.ref != null && String(xo.ref).trim()) out.push(String(xo.ref).trim());
    else if (xo.name != null && String(xo.name).trim()) out.push(String(xo.name).trim());
  }
  return out;
}

function sequencesMap(scopeDoc: Record<string, unknown>): Record<string, unknown> {
  const raw = scopeDoc.confidence_match_rule_sequences;
  if (raw !== null && typeof raw === "object" && !Array.isArray(raw)) {
    return raw as Record<string, unknown>;
  }
  return {};
}

/**
 * Expand a `validation_rules` array using scope-level `confidence_match_rule_sequences`.
 * Walks `hierarchy` subtrees in order; returns leaf rule ids (deduped).
 */
export function expandConfidenceMatchRulesList(raw: unknown[], scopeDoc: Record<string, unknown>): string[] {
  const sequences = sequencesMap(scopeDoc);
  const out: string[] = [];

  function walk(items: unknown[]) {
    for (const r of items) {
      if (typeof r === "string") {
        const s = r.trim();
        if (s) out.push(s);
        continue;
      }
      if (!r || typeof r !== "object" || Array.isArray(r)) continue;
      const o = r as Record<string, unknown>;
      if (isShorthandConfidenceMatchChain(o)) {
        const k = Object.keys(o)[0]!;
        const tail = o[k] as unknown[];
        const head = String(k).trim();
        if (head) out.push(head);
        walk(tail as unknown[]);
        continue;
      }
      const hi = o.hierarchy;
      if (hi && typeof hi === "object" && !Array.isArray(hi)) {
        const h = hi as Record<string, unknown>;
        const ch = Array.isArray(h.children) ? h.children : [];
        walk(ch as unknown[]);
        continue;
      }
      const ref = o.ref;
      if (ref != null && String(ref).trim()) {
        out.push(String(ref).trim());
        continue;
      }
      const n = o.name;
      if (n != null && String(n).trim()) {
        out.push(String(n).trim());
        continue;
      }
      const seq = o.sequence;
      if (seq != null && String(seq).trim()) {
        const key = String(seq).trim();
        const seqList = sequences[key];
        if (Array.isArray(seqList)) {
          walk(expandSequenceEntries(seqList).map((id) => id));
        }
        continue;
      }
    }
  }

  walk(raw);
  return dedupePreserveOrder(out);
}

/** Canonical tree of resolved refs / sequences for clustering identical validation wiring. */
function canonicalConfidenceMatchRulesTree(
  raw: unknown[],
  scopeDoc: Record<string, unknown>
): unknown[] {
  const sequences = sequencesMap(scopeDoc);

  function canon(item: unknown): unknown {
    if (typeof item === "string") return item.trim();
    if (!item || typeof item !== "object" || Array.isArray(item)) return null;
    const o = item as Record<string, unknown>;
    if (isShorthandConfidenceMatchChain(o)) {
      const k = Object.keys(o)[0]!;
      const tail = o[k] as unknown[];
      return {
        hierarchy: {
          mode: "ordered",
          children: [
            String(k).trim(),
            ...(tail as unknown[]).map((x) => canon(x)).filter((x) => x != null),
          ],
        },
      };
    }
    const hi = o.hierarchy;
    if (hi && typeof hi === "object" && !Array.isArray(hi)) {
      const h = hi as Record<string, unknown>;
      const rawCh = Array.isArray(h.children) ? h.children : [];
      return {
        hierarchy: {
          mode: h.mode ?? "ordered",
          children: (rawCh as unknown[]).map((x) => canon(x)).filter((x) => x != null),
        },
      };
    }
    const seq = o.sequence;
    if (seq != null && String(seq).trim()) {
      const key = String(seq).trim();
      const seqList = sequences[key];
      if (Array.isArray(seqList)) {
        return {
          hierarchy: {
            mode: "ordered",
            children: expandSequenceEntries(seqList),
          },
        };
      }
      return { sequence: key };
    }
    if (o.ref != null && String(o.ref).trim()) return String(o.ref).trim();
    if (o.name != null && String(o.name).trim()) return String(o.name).trim();
    return o;
  }

  return raw.map((x) => canon(x)).filter((x) => x != null);
}

/** Stable key for clustering canvas chains that share the same validation structure. */
export function confidenceMatchRulesStructureKey(
  validation: unknown,
  scopeDoc: Record<string, unknown>
): string {
  if (!validation || typeof validation !== "object" || Array.isArray(validation)) return "";
  const raw = validationRulesArrayFromBlock(validation as Record<string, unknown>);
  if (!Array.isArray(raw)) return "";
  try {
    return JSON.stringify(canonicalConfidenceMatchRulesTree(raw, scopeDoc));
  } catch {
    return "";
  }
}

/** Names listed under a `validation:` block (per rule, source view, or global data.validation). */
export function resolveConfidenceMatchRuleNames(
  validation: unknown,
  scopeDoc: Record<string, unknown>
): string[] {
  if (!validation || typeof validation !== "object" || Array.isArray(validation)) return [];
  const raw = validationRulesArrayFromBlock(validation as Record<string, unknown>);
  if (!Array.isArray(raw)) return [];
  return expandConfidenceMatchRulesList(raw, scopeDoc);
}
