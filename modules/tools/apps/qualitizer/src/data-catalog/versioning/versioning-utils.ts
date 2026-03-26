/**
 * Long opaque alphanumeric-only strings (e.g. customer checksum ids) are sorted after
 * normal semver-like versions in grids and lists.
 */
export function isChecksumLikeVersion(s: string): boolean {
  const t = String(s).trim();
  if (t.length <= 11) return false;
  return /^[a-zA-Z0-9]+$/.test(t);
}

/** Parse version into comparable parts. Handles v1.0.1, v2, v10, alpha, beta, v1.0-alpha, v2.0-beta, etc. */
function parseVersionParts(s: string): { parts: number[]; isV: boolean } {
  const trimmed = s.trim();
  const isV = /^v/i.test(trimmed);
  const rest = isV ? trimmed.slice(1).trim() : trimmed;

  if (/^alpha$/i.test(rest)) return { parts: [0, 9998], isV };
  if (/^beta$/i.test(rest)) return { parts: [0, 9999], isV };

  const segmentMatch = rest.match(/^[\d.]+/);
  if (!segmentMatch) return { parts: [Number.MAX_SAFE_INTEGER], isV: false };
  const afterNumeric = rest.slice(segmentMatch[0].length).toLowerCase();
  const hasPreReleaseTag = /[-.]alpha|[-.]beta/.test(afterNumeric);

  let parts = segmentMatch[0]
    .split(".")
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

/** Sort order: normal versions first; checksum-like opaque ids last; then v1.0.1 before v2, etc. */
export function compareVersionStrings(a: string, b: string): number {
  const ca = isChecksumLikeVersion(a);
  const cb = isChecksumLikeVersion(b);
  if (ca !== cb) return ca ? 1 : -1;
  if (ca && cb) {
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
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
