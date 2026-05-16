/**
 * Opaque / auto-generated version tokens (checksum chunks, implicit DMS versions): sorted after
 * normal semver-like versions in grids and lists.
 */
export function isChecksumLikeVersion(s: string): boolean {
  const t = String(s).trim();
  if (t.length === 0) return false;

  if (t.length > 11 && /^[a-zA-Z0-9]+$/.test(t)) return true;

  if (/^\d+$/.test(t)) {
    try {
      if (t.length >= 4 && BigInt(t) > 1000n) return true;
    } catch {
      /* ignore */
    }
    return false;
  }

  if (
    t.length >= 8 &&
    t.length <= 64 &&
    /^[0-9a-f]+$/i.test(t) &&
    /[a-f]/i.test(t)
  ) {
    return true;
  }

  return false;
}

/** Opaque / auto-generated view versions (typical when a data model references a view without an explicit version). */
export function countImplicitViewVersions(versionKeys: Iterable<string>): number {
  let n = 0;
  for (const v of versionKeys) {
    if (isChecksumLikeVersion(v)) n += 1;
  }
  return n;
}

/** Matrix column labels `i1`, `i2`, … for opaque checksum-style versions (after explicit versions). */
export function isVirtualImplicitVersionLabel(s: string): boolean {
  return /^i\d+$/i.test(String(s).trim());
}

function virtualImplicitSortIndex(s: string): number {
  const m = String(s).trim().match(/^i(\d+)$/i);
  return m ? parseInt(m[1]!, 10) : 0;
}

/** 1-based slot index for `i1` → 1, `i2` → 2. */
export function virtualImplicitSlotNumber(versionLabel: string): number | null {
  const m = String(versionLabel).trim().match(/^i(\d+)$/i);
  if (!m) return null;
  const n = parseInt(m[1]!, 10);
  return n >= 1 ? n : null;
}

export type VersionRowLike = {
  key: string;
  versions: Map<string, { createdTime?: number }>;
};

/** Implicit checksum versions on one row, oldest first (for matrix columns i1, i2, …). */
export function getSortedImplicitVersionsForRow(
  row: VersionRowLike,
  detailsMap: ReadonlyMap<string, { createdTime?: number }>
): string[] {
  const implicits: string[] = [];
  for (const v of row.versions.keys()) {
    if (isChecksumLikeVersion(v)) implicits.push(v);
  }
  implicits.sort((a, b) => {
    const ta =
      detailsMap.get(`${row.key}:${a}`)?.createdTime ?? row.versions.get(a)?.createdTime ?? 0;
    const tb =
      detailsMap.get(`${row.key}:${b}`)?.createdTime ?? row.versions.get(b)?.createdTime ?? 0;
    if (ta !== tb) return ta - tb;
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  });
  return implicits;
}

export function resolveMatrixColumnToRawForRow(
  row: VersionRowLike,
  columnToken: string,
  detailsMap: ReadonlyMap<string, { createdTime?: number }>
): string | null {
  if (!isVirtualImplicitVersionLabel(columnToken)) {
    return row.versions.has(columnToken) ? columnToken : null;
  }
  const slot = virtualImplicitSlotNumber(columnToken);
  if (slot == null) return null;
  const sorted = getSortedImplicitVersionsForRow(row, detailsMap);
  return sorted[slot - 1] ?? null;
}

/** Display label `iK` for a raw implicit on this row, or the raw token if not implicit / not on row. */
export function virtualImplicitLabelForRawOnRow(
  row: VersionRowLike,
  raw: string,
  detailsMap: ReadonlyMap<string, { createdTime?: number }>
): string {
  if (!isChecksumLikeVersion(raw)) return raw;
  const sorted = getSortedImplicitVersionsForRow(row, detailsMap);
  const idx = sorted.indexOf(raw);
  return idx >= 0 ? `i${idx + 1}` : raw;
}

export function maxImplicitSlotCountForRows(
  rows: Iterable<VersionRowLike>,
  detailsMap: ReadonlyMap<string, { createdTime?: number }>
): number {
  let m = 0;
  for (const row of rows) {
    m = Math.max(m, getSortedImplicitVersionsForRow(row, detailsMap).length);
  }
  return m;
}

export function implicitColumnLabelsForSlotCount(n: number): string[] {
  return Array.from({ length: Math.max(0, n) }, (_, i) => `i${i + 1}`);
}

function versionSortTier(s: string): 0 | 1 | 2 {
  if (isChecksumLikeVersion(s)) return 2;
  if (isVirtualImplicitVersionLabel(s)) return 1;
  return 0;
}

/** Parse version into comparable parts. Handles v1.0.1, v.0.0.1, 3_1_1 vs 3_12_0, v2, v10, alpha, beta, v1.0-alpha, v2.0-beta, etc. */
function parseVersionParts(s: string): { parts: number[]; isV: boolean } {
  const trimmed = s.trim();
  const isV = /^v/i.test(trimmed);
  let rest = isV ? trimmed.slice(1).trim() : trimmed;
  rest = rest.replace(/^[._]+/, "");

  if (/^alpha$/i.test(rest)) return { parts: [0, 9998], isV };
  if (/^beta$/i.test(rest)) return { parts: [0, 9999], isV };

  const segmentMatch = rest.match(/^\d+(?:[._]\d+)*/);
  if (!segmentMatch) return { parts: [Number.MAX_SAFE_INTEGER], isV };
  const afterNumeric = rest.slice(segmentMatch[0].length).toLowerCase();
  const hasPreReleaseTag = /[-.]alpha|[-.]beta/.test(afterNumeric);

  let parts = segmentMatch[0]
    .split(/[._]/)
    .map((x) => (x === "" ? 0 : parseInt(x, 10)))
    .filter((n) => !Number.isNaN(n));
  if (parts.length === 0) return { parts: [Number.MAX_SAFE_INTEGER], isV: false };

  if (hasPreReleaseTag) {
    if (parts[0] < 1) {
      parts = [0, 9999];
    } else {
      parts.push(-1);
    }
  }
  return { parts, isV };
}

/** Sort order: explicit semver-like first; virtual `iN` next; checksum-like opaque ids last. */
export function compareVersionStrings(a: string, b: string): number {
  const ta = versionSortTier(a);
  const tb = versionSortTier(b);
  if (ta !== tb) return ta - tb;
  if (ta === 2 && tb === 2) {
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  }
  if (ta === 1 && tb === 1) {
    return virtualImplicitSortIndex(a) - virtualImplicitSortIndex(b);
  }

  const pa = parseVersionParts(a);
  const pb = parseVersionParts(b);
  const maxLen = Math.max(pa.parts.length, pb.parts.length);
  for (let i = 0; i < maxLen; i++) {
    const va = pa.parts[i] ?? 0;
    const vb = pb.parts[i] ?? 0;
    if (va !== vb) return va - vb;
  }
  return (pa.isV ? 1 : 0) - (pb.isV ? 1 : 0); // plain before v-prefixed
}

/** Sort an array of version strings (v2 before v10). */
export function sortVersions(versions: string[]): string[] {
  return [...versions].sort(compareVersionStrings);
}

export type ModelVersionRef = {
  space: string;
  externalId: string;
  versions: Set<string>;
  hasUnspecified: boolean;
};

export type ModelVersionNotInUse = {
  space: string;
  externalId: string;
  version: string;
  label: string;
};

export type ViewVersionNotInUse = {
  space: string;
  externalId: string;
  version: string;
  label: string;
};

/** Consolidated: one row per model with versions list. */
export type ModelNotInUseConsolidated = {
  space: string;
  externalId: string;
  label: string;
  versions: string[];
  /** True if no transformation references this model at all */
  isCompletelyUnused: boolean;
};

/** Consolidated: one row per view with versions list. */
export type ViewNotInUseConsolidated = {
  space: string;
  externalId: string;
  label: string;
  versions: string[];
  /** True if no in-use model references any version of this view */
  isCompletelyUnused: boolean;
};

export type ViewVersionWouldBeOrphaned = {
  space: string;
  externalId: string;
  version: string;
  label: string;
  /** Model versions (not in use) that reference this view version */
  referencedBy: Array<{ space: string; externalId: string; version: string; label: string }>;
};

/** Consolidated: one row per view with versions list. */
export type ViewWouldBeOrphanedConsolidated = {
  space: string;
  externalId: string;
  label: string;
  versions: string[];
  /** Model versions (not in use) that reference these view versions */
  referencedBy: Array<{ space: string; externalId: string; version: string; label: string }>;
};

export type LegendFilterMode = "include" | "exclude";

export type LegendFilterState<T extends string> = { id: T; mode: LegendFilterMode } | null;

export function cycleLegendFilterState<T extends string>(
  prev: LegendFilterState<T>,
  nextId: T
): LegendFilterState<T> {
  if (!prev || prev.id !== nextId) return { id: nextId, mode: "include" };
  if (prev.mode === "include") return { id: nextId, mode: "exclude" };
  return null;
}
