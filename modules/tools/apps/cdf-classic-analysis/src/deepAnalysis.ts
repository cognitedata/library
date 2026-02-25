/**
 * Deep analysis: filter key selection and report building.
 * Wrapper logic that mimics a user running analysis for multiple keys per resource type.
 */

import type { ResourceType } from "./analysis";

/** Primary filter keys per resource type (API-level or common dimensions). */
export const PRIMARY_FILTER_KEYS: Record<ResourceType, string[]> = {
  assets: [],
  timeseries: ["is step", "is string", "unit"],
  events: ["type"],
  sequences: [],
  files: ["type", "labels", "author", "source"],
};

/**
 * Substrings that indicate a metadata field has a "sorting/categorization" nature
 * as part of the field name (e.g. "assetType", "equipmentCategory").
 * Excludes "order" — too ambiguous (sort order vs work/business order).
 */
const SORTING_SUBSTRINGS = [
  "type",
  "category",
  "level",
  "class",
  "kind",
  "group",
  "classification",
  "tier",
  "grade",
  "tag",
  "sort",
  "family",
  "genre",
  "style",
  "variant",
  "division",
  "rank",
  "taxonomy",
  "rubric",
  "grouping",
];

/** Keys that indicate an individual identifier; exclude from deep analysis. */
const IDENTIFIER_KEY_PARTS: { term: string; exactOrSuffix?: boolean }[] = [
  { term: "name" },
  { term: "externalid" },
  { term: "external_id" },
  { term: "sourceid" },
  { term: "source_id" },
  { term: "uuid" },
  { term: "guid" },
  { term: "id", exactOrSuffix: true },
];

/**
 * Order-as-identifier: business/work order, not sort order. Exclude keys that
 * suggest order id, work order, purchase order, etc.
 */
const ORDER_IDENTIFIER_SUBSTRINGS = [
  "orderid",
  "order_id",
  "workorder",
  "work_order",
  "purchaseorder",
  "purchase_order",
  "salesorder",
  "sales_order",
  "joborder",
  "job_order",
];

/**
 * Date/time-like metadata; these tend to be per-item (individual data), not grouping.
 */
const DATETIME_SUBSTRINGS = [
  "date",
  "time",
  "datetime",
  "timestamp",
  "created",
  "modified",
  "updated",
  "due",
  "utc",
  "iso8601",
  "epoch",
];

function isSortingLikeKey(key: string): boolean {
  const lower = key.trim().toLowerCase();
  if (!lower) return false;
  return SORTING_SUBSTRINGS.some((term) => lower.includes(term));
}

function isIdentifierLikeKey(key: string): boolean {
  const lower = key.trim().toLowerCase();
  if (!lower) return true;
  if (lower === "order") return true;
  return IDENTIFIER_KEY_PARTS.some(({ term, exactOrSuffix }) => {
    if (exactOrSuffix && term === "id")
      return lower === "id" || lower.endsWith("_id");
    return lower === term || lower.includes(term);
  });
}

function isOrderIdentifierLikeKey(key: string): boolean {
  const lower = key.trim().toLowerCase();
  if (!lower) return false;
  if (lower === "order") return true;
  return ORDER_IDENTIFIER_SUBSTRINGS.some((term) => lower.includes(term));
}

function isDateTimeLikeKey(key: string): boolean {
  const lower = key.trim().toLowerCase();
  if (!lower) return false;
  return DATETIME_SUBSTRINGS.some((term) => lower.includes(term));
}

function isEligibleMetadataKey(key: string): boolean {
  return (
    !!key.trim() &&
    isSortingLikeKey(key) &&
    !isIdentifierLikeKey(key) &&
    !isOrderIdentifierLikeKey(key) &&
    !isDateTimeLikeKey(key)
  );
}

export interface MetadataKeyCount {
  key: string;
  count: number;
}

const TOP_METADATA_COUNT = 15;

/**
 * Select filter keys for deep analysis:
 * 1. Primary keys for the resource type.
 * 2. From actual loaded metadata keys: those whose name has a sorting concept (as part of the name)
 *    and are not identifier-like (name, externalId, sourceId, etc.). Take the top 15 by count,
 *    then add any further keys that meet the coverage threshold (same name rules).
 */
export function selectFilterKeysForDeepAnalysis(
  metadataList: MetadataKeyCount[],
  totalCount: number,
  resourceType: ResourceType,
  coveragePct = 0.6
): string[] {
  const primary = PRIMARY_FILTER_KEYS[resourceType];
  const seen = new Set<string>();
  const out: string[] = [];

  for (const k of primary) {
    const key = k.trim();
    if (key && !seen.has(key)) {
      seen.add(key);
      out.push(key);
    }
  }

  const eligible = metadataList.filter(({ key }) => isEligibleMetadataKey(key));
  const sorted = [...eligible].sort((a, b) => b.count - a.count);
  const threshold = totalCount > 0 ? coveragePct * totalCount : 0;

  for (let i = 0; i < sorted.length; i++) {
    const { key, count } = sorted[i]!;
    const k = key.trim();
    if (!k || seen.has(k)) continue;
    const inTop15 = i < TOP_METADATA_COUNT;
    const meetsThreshold = count >= threshold;
    if (inTop15 || meetsThreshold) {
      seen.add(k);
      out.push(k);
    }
  }

  return out;
}

/** Sanitize a string for use in a file name (alphanumeric, dash, underscore). */
export function slugForFileName(s: string, maxLen = 40): string {
  return String(s)
    .replace(/\s+/g, "-")
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, maxLen) || "unnamed";
}
