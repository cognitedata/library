/**
 * Detect identical and near-duplicate SQL fragments across transformations
 * to suggest reusable functions. Comparisons ignore whitespace differences.
 */

export type TransformationWithQuery = {
  id: string;
  name?: string;
  query: string;
};

/** Collapse whitespace to single space and trim. */
export function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, " ").trim();
}

/** Return indices in normalized string where a word starts (start of string or after space). */
function getWordStartIndices(norm: string): number[] {
  const out: number[] = [];
  for (let i = 0; i < norm.length; i++) {
    if (norm[i] !== " " && (i === 0 || norm[i - 1] === " ")) {
      out.push(i);
    }
  }
  return out;
}

/** Return indices in normalized string that are word ends (at space or end of string). */
function getWordEndIndices(norm: string): number[] {
  const out: number[] = [];
  for (let i = 0; i <= norm.length; i++) {
    if (i === norm.length || norm[i] === " ") {
      out.push(i);
    }
  }
  return out;
}

/** Word immediately before endIndex (exclusive): last word in norm[0..endIndex). */
function getWordBefore(norm: string, endIndex: number): string {
  if (endIndex <= 0) return "";
  const lastSpace = norm.lastIndexOf(" ", endIndex - 1);
  const start = lastSpace === -1 ? 0 : lastSpace + 1;
  return norm.slice(start, endIndex);
}

/** First word at or after startIndex (skip leading spaces). */
function getWordAfter(norm: string, startIndex: number): string {
  let i = startIndex;
  while (i < norm.length && norm[i] === " ") i++;
  if (i >= norm.length) return "";
  const firstSpace = norm.indexOf(" ", i);
  if (firstSpace === -1) return norm.slice(i);
  return norm.slice(i, firstSpace);
}

/** Restore single-space normalized form only (for display). */
export function normalizeSqlForDisplay(sql: string): string {
  return sql.replace(/\s+/g, " ").trim();
}

const MIN_IDENTICAL_LEN = 80;
const MAX_IDENTICAL_LEN = 800;
const IDENTICAL_STEP = 20;

export type IdenticalFragment = {
  /** Normalized fragment text (whitespace collapsed). */
  normalized: string;
  /** Display snippet (single-space, truncated if long). */
  snippet: string;
  /** Which transformations contain this fragment and approximate position. */
  occurrences: Array<{ id: string; name?: string; index: number }>;
};

export type OverlapProgress = (phase: string, current: number, total: number) => void;

/**
 * Find substrings that appear in at least 2 different transformations
 * (after normalizing whitespace). Uses sliding window over normalized text.
 */
export function findIdenticalFragments(
  items: TransformationWithQuery[],
  minLength: number = MIN_IDENTICAL_LEN,
  maxLength: number = MAX_IDENTICAL_LEN,
  step: number = IDENTICAL_STEP,
  onProgress?: OverlapProgress
): IdenticalFragment[] {
  const byNormalized = new Map<
    string,
    Array<{ id: string; name?: string; index: number; normalized: string }>
  >();

  const total = items.length;
  const report = onProgress
    ? (phase: string, current: number) => {
        onProgress(phase, current, total);
      }
    : () => {};

  const withQuery = items.filter((t) => t.query?.trim());
  report("Scanning for identical fragments", 0);
  for (let i = 0; i < withQuery.length; i++) {
    if (i > 0 && i % 10 === 0) report("Scanning for identical fragments", i);
    const item = withQuery[i];
    const q = item.query.trim();
    const norm = normalizeSql(q);
    if (norm.length < minLength) continue;

    const wordStarts = getWordStartIndices(norm);
    const wordEnds = getWordEndIndices(norm);
    const seen = new Set<string>();

    for (const start of wordStarts) {
      const validEnds = wordEnds.filter(
        (e) => e > start && e - start >= minLength && e - start <= maxLength
      );
      for (let j = 0; j < validEnds.length; j += Math.max(1, Math.floor(step / 2))) {
        const end = validEnds[j];
        const sub = norm.slice(start, end);
        if (seen.has(sub)) continue;
        seen.add(sub);
        const list = byNormalized.get(sub) ?? [];
        const alreadyFromThis = list.some((o) => o.id === item.id);
        if (!alreadyFromThis) {
          list.push({
            id: item.id,
            name: item.name,
            index: start,
            normalized: sub,
          });
          byNormalized.set(sub, list);
        }
      }
    }
  }

  const results: IdenticalFragment[] = [];
  for (const [normalized, occurrences] of byNormalized) {
    if (occurrences.length < 2) continue;
    const uniqueIds = new Set(occurrences.map((o) => o.id));
    if (uniqueIds.size < 2) continue;
    const snippet =
      normalized.length > 120 ? normalized.slice(0, 117) + "..." : normalized;
    results.push({
      normalized,
      snippet,
      occurrences: occurrences.map((o) => ({ id: o.id, name: o.name, index: o.index })),
    });
  }

  report("Scanning for identical fragments", withQuery.length);
  report("Growing identical overlaps", 0);
  const grown = growIdenticalFragments(results, items);
  report("Merging overlapping identical fragments", 0);
  return keepLongestOverlappingIdentical(grown);
}

/** Grow each fragment left and right by full words while it still appears in the same transformations. */
function growIdenticalFragments(
  fragments: IdenticalFragment[],
  items: TransformationWithQuery[]
): IdenticalFragment[] {
  const normById = new Map<string, string>();
  for (const t of items) {
    const q = t.query?.trim();
    if (q) normById.set(String(t.id), normalizeSql(q));
  }

  return fragments.map((frag) => {
    let normalized = frag.normalized;
    let occurrences = frag.occurrences.map((o) => ({ ...o }));

    for (;;) {
      const allNorms = occurrences.map((o) => normById.get(o.id)).filter(Boolean) as string[];
      if (allNorms.length !== occurrences.length) break;

      const wordBefore = getWordBefore(allNorms[0], occurrences[0].index);
      if (!wordBefore) break;
      const sameBefore = occurrences.every((o, i) => {
        const norm = normById.get(o.id)!;
        return getWordBefore(norm, o.index) === wordBefore;
      });
      if (!sameBefore) break;

      normalized = wordBefore + " " + normalized;
      for (const o of occurrences) {
        const norm = normById.get(o.id)!;
        const lastSpace = norm.lastIndexOf(" ", o.index - 1);
        o.index = lastSpace >= 0 ? lastSpace + 1 : 0;
      }
    }

    for (;;) {
      const first = occurrences[0];
      const norm0 = normById.get(first.id);
      if (!norm0) break;
      const end0 = first.index + normalized.length;
      const nextStart = end0 < norm0.length && norm0[end0] === " " ? end0 + 1 : end0;
      const wordAfter = getWordAfter(norm0, nextStart);
      if (!wordAfter) break;

      const sameAfter = occurrences.every((o) => {
        const norm = normById.get(o.id)!;
        const end = o.index + normalized.length;
        const next = end < norm.length && norm[end] === " " ? end + 1 : end;
        return getWordAfter(norm, next) === wordAfter;
      });
      if (!sameAfter) break;

      normalized = normalized + " " + wordAfter;
    }

    const snippet =
      normalized.length > 120 ? normalized.slice(0, 117) + "..." : normalized;
    return { normalized, snippet, occurrences };
  });
}

/** Merge overlapping identical fragments: keep the longest, merge occurrence lists by id. */
function keepLongestOverlappingIdentical(fragments: IdenticalFragment[]): IdenticalFragment[] {
  if (fragments.length <= 1) return fragments;
  const byLength = [...fragments].sort((a, b) => b.normalized.length - a.normalized.length);
  const merged: IdenticalFragment[] = [];

  for (const frag of byLength) {
    const container = merged.find(
      (r) => r.normalized.length > frag.normalized.length && r.normalized.includes(frag.normalized)
    );
    if (container) {
      const byId = new Map(container.occurrences.map((o) => [o.id, o]));
      for (const o of frag.occurrences) {
        if (!byId.has(o.id)) byId.set(o.id, o);
      }
      container.occurrences = Array.from(byId.values());
    } else {
      merged.push({ ...frag, occurrences: [...frag.occurrences] });
    }
  }

  return merged.sort((a, b) => b.occurrences.length - a.occurrences.length);
}

/** Replace string literals and numbers with placeholders for fingerprinting. */
export function fingerprintSql(normalized: string): string {
  let out = normalized;
  // Double-quoted strings
  out = out.replace(/"([^"\\]|\\.)*"/g, "<STR>");
  // Single-quoted strings
  out = out.replace(/'([^'\\]|\\.)*'/g, "<STR>");
  // Backtick-quoted identifiers
  out = out.replace(/`[^`]+`/g, "<ID>");
  // Numbers (integer or decimal)
  out = out.replace(/\b\d+\.?\d*\b/g, "<NUM>");
  return out;
}

const MIN_NEAR_LEN = 100;
const MAX_NEAR_LEN = 600;
const NEAR_STEP = 30;

export type NearDuplicateGroup = {
  /** Template form (literals/numbers replaced with placeholders). */
  template: string;
  /** Display snippet of template. */
  snippet: string;
  /** Example normalized fragments that map to this template. */
  examples: Array<{ normalized: string; id: string; name?: string }>;
  /** Number of distinct transformations. */
  count: number;
};

/**
 * Find groups of fragments that are the same after replacing string/numeric
 * literals (copy-paste with small edits, e.g. different IDs in WHERE).
 */
export function findNearDuplicateFragments(
  items: TransformationWithQuery[],
  minLength: number = MIN_NEAR_LEN,
  maxLength: number = MAX_NEAR_LEN,
  step: number = NEAR_STEP,
  onProgress?: OverlapProgress
): NearDuplicateGroup[] {
  const byTemplate = new Map<
    string,
    Array<{ normalized: string; id: string; name?: string }>
  >();

  const total = items.length;
  const report = onProgress
    ? (phase: string, current: number) => onProgress(phase, current, total)
    : () => {};

  const withQuery = items.filter((t) => t.query?.trim());
  report("Scanning for near-duplicate fragments", 0);
  for (let i = 0; i < withQuery.length; i++) {
    if (i > 0 && i % 10 === 0) report("Scanning for near-duplicate fragments", i);
    const item = withQuery[i];
    const q = item.query.trim();
    const norm = normalizeSql(q);
    if (norm.length < minLength) continue;

    const wordStarts = getWordStartIndices(norm);
    const wordEnds = getWordEndIndices(norm);
    const seen = new Set<string>();

    for (const start of wordStarts) {
      const validEnds = wordEnds.filter(
        (e) => e > start && e - start >= minLength && e - start <= maxLength
      );
      for (let j = 0; j < validEnds.length; j += Math.max(1, Math.floor(step / 2))) {
        const end = validEnds[j];
        const sub = norm.slice(start, end);
        const template = fingerprintSql(sub);
        if (template.length < minLength) continue;
        if (seen.has(template)) continue;
        seen.add(template);
        const list = byTemplate.get(template) ?? [];
        const alreadyFromThis = list.some((e) => e.id === item.id && e.normalized === sub);
        if (!alreadyFromThis) {
          list.push({ normalized: sub, id: item.id, name: item.name });
          byTemplate.set(template, list);
        }
      }
    }
  }

  report("Scanning for near-duplicate fragments", withQuery.length);

  const results: NearDuplicateGroup[] = [];
  for (const [template, examples] of byTemplate) {
    const uniqueIds = new Set(examples.map((e) => e.id));
    if (uniqueIds.size < 2) continue;
    const snippet = template.length > 140 ? template.slice(0, 137) + "..." : template;
    results.push({
      template,
      snippet,
      examples,
      count: uniqueIds.size,
    });
  }

  report("Growing near-duplicate overlaps", 0);
  const grown = growNearDuplicateGroups(results, items);
  report("Merging overlapping near-duplicate groups", 0);
  return keepLongestOverlappingNearDuplicate(grown);
}

/** Grow each group's examples by words; only extend when all keep the same template. */
function growNearDuplicateGroups(
  groups: NearDuplicateGroup[],
  items: TransformationWithQuery[]
): NearDuplicateGroup[] {
  const normById = new Map<string, string>();
  for (const t of items) {
    const q = t.query?.trim();
    if (q) normById.set(String(t.id), normalizeSql(q));
  }

  return groups.map((group) => {
    let examples = group.examples.map((e) => ({ ...e, normalized: e.normalized }));

    for (;;) {
      const withBefore = examples.map((ex) => {
        const norm = normById.get(ex.id);
        if (!norm) return null;
        const pos = norm.indexOf(ex.normalized);
        if (pos < 0) return null;
        const w = getWordBefore(norm, pos);
        return w ? { ...ex, normalized: w + " " + ex.normalized } : null;
      });
      if (withBefore.some((x) => x === null)) break;
      const templates = (withBefore as Array<{ normalized: string; id: string; name?: string }>).map(
        (x) => fingerprintSql(x.normalized)
      );
      if (new Set(templates).size > 1) break;
      examples = withBefore as Array<{ normalized: string; id: string; name?: string }>;
    }

    for (;;) {
      const withAfter = examples.map((ex) => {
        const norm = normById.get(ex.id);
        if (!norm) return null;
        const pos = norm.indexOf(ex.normalized);
        if (pos < 0) return null;
        const end = pos + ex.normalized.length;
        const nextStart = end < norm.length && norm[end] === " " ? end + 1 : end;
        const w = getWordAfter(norm, nextStart);
        return w ? { ...ex, normalized: ex.normalized + " " + w } : null;
      });
      if (withAfter.some((x) => x === null)) break;
      const templates = (withAfter as Array<{ normalized: string; id: string; name?: string }>).map(
        (x) => fingerprintSql(x.normalized)
      );
      if (new Set(templates).size > 1) break;
      examples = withAfter as Array<{ normalized: string; id: string; name?: string }>;
    }

    const template = fingerprintSql(examples[0].normalized);
    const snippet = template.length > 140 ? template.slice(0, 137) + "..." : template;
    return {
      template,
      snippet,
      examples,
      count: new Set(examples.map((e) => e.id)).size,
    };
  });
}

/** Merge overlapping near-duplicate groups: keep the longest template, merge examples by id. */
function keepLongestOverlappingNearDuplicate(
  groups: NearDuplicateGroup[]
): NearDuplicateGroup[] {
  if (groups.length <= 1) return groups;
  const byLength = [...groups].sort((a, b) => b.template.length - a.template.length);
  const merged: NearDuplicateGroup[] = [];

  for (const g of byLength) {
    const container = merged.find(
      (r) =>
        r.template.length > g.template.length && r.template.includes(g.template)
    );
    if (container) {
      const byId = new Map(container.examples.map((e) => [e.id, e]));
      for (const e of g.examples) {
        if (!byId.has(e.id)) byId.set(e.id, e);
      }
      container.examples = Array.from(byId.values());
      container.count = container.examples.length;
    } else {
      merged.push({
        template: g.template,
        snippet: g.snippet,
        examples: [...g.examples],
        count: g.count,
      });
    }
  }

  return merged.sort((a, b) => b.count - a.count);
}
