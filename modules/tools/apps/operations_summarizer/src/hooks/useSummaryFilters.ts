/**
 * Hook for filtering summaries by date and content type.
 * Transpiled from Python past_summaries_page.py
 *
 * **Content type filtering (DMS limitation):** Which sections a summary includes is encoded in the
 * file `description` (e.g. `OPS:<bitmap>` prefix and legacy “Content types: …” text), not as a
 * first-class view property. DMS filters cannot express that encoding, so content-type matching
 * runs **client-side** after instances are loaded (see also `useSummariesCdf` paged list). When the
 * data model is next revised, prefer a dedicated indexed property (e.g. flags or enum list) so
 * the server can filter and paginate accurately.
 */

import { useCallback, useMemo, useState } from "react";
import type { DataTypeName, SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { extractDateRangeFromContent } from "@/utils/formatting";
import { extractFileProperties } from "@/utils/nodeProperties";
import {
  SUMMARY_CONTENT_TYPE_BITMAP_ORDER,
  stripSummaryContentTypeBitmapLine,
} from "@/utils/summaryListQuery";

interface DateRange {
  start: Date;
  end: Date;
}

interface UseSummaryFiltersOptions {
  /** When omitted, `filteredSummaries` is empty — use server-paginated `items` from `useSummaryListPage` instead. */
  summaries?: SummaryInstance[];
}

interface UseSummaryFiltersResult {
  filteredSummaries: SummaryInstance[];

  // Date filtering
  dateFilterEnabled: boolean;
  dateRange: DateRange | null;
  toggleDateFilter: () => void;
  setDateRange: (range: DateRange | null) => void;

  // Content type filtering
  contentTypeFilterEnabled: boolean;
  selectedContentTypes: DataTypeName[];
  toggleContentTypeFilter: () => void;
  setSelectedContentTypes: (types: DataTypeName[]) => void;
  toggleContentType: (type: DataTypeName, value?: boolean) => void;

  // Utilities
  clearFilters: () => void;
  hasActiveFilters: boolean;
}

/**
 * Check if a summary contains actual data for specific data types.
 * Uses regex patterns to detect content sections.
 */
export function summaryContainsDataType(
  content: string,
  dataType: DataTypeName,
): boolean {
  const patterns: Record<DataTypeName, RegExp[]> = {
    work_orders: [
      /## Work orders/i,
      /Work Order Summary:/i,
      /work orders/i,
      /wo_\d+/i,
    ],
    assets: [
      /Assets \(\d+\):/i,
      /## Assets/i,
      /Asset Summary:/i,
      /new assets/i,
    ],
    files: [/Files \(\d+\):/i, /## Files/i, /File Summary:/i, /new files/i],
    time_series: [
      /Time Series \(\d+\):/i,
      /## Time Series/i,
      /Time Series Summary:/i,
      /new time series/i,
    ],
    equipment: [
      /Equipment \(\d+\):/i,
      /## Equipment/i,
      /Equipment Summary:/i,
      /new equipment/i,
    ],
  };

  const dataPatterns = patterns[dataType] || [];
  return dataPatterns.some((pattern) => pattern.test(content));
}

function matchesOpsBitmapBits(bits: string, types: DataTypeName[]): boolean {
  return types.some((t) => {
    const i = SUMMARY_CONTENT_TYPE_BITMAP_ORDER.indexOf(t);
    return i >= 0 && i < bits.length && bits[i] === "1";
  });
}

/** Legacy save line: `… Content types: assets, files` */
function descriptionListsContentType(
  description: string,
  dataType: DataTypeName,
): boolean {
  const lower = description.toLowerCase();
  const idx = lower.indexOf("content types:");
  if (idx < 0) return false;
  const tail = description.slice(idx);
  if (tail.toLowerCase().includes(dataType.toLowerCase())) return true;
  const spaced = dataType.replace(/_/g, " ").toLowerCase();
  return tail.toLowerCase().includes(spaced);
}

/**
 * True if the summary should appear when filtering by any of `types`.
 * Uses `OPS:` bitmap when present; otherwise markdown/sourceContext heuristics
 * and the human-readable `Content types:` suffix from saves.
 *
 * This logic must stay client-side until the schema exposes content types as a real property:
 * the bitmap lives inside `description` (and related text), which DMS instance filters cannot
 * query structurally. **Migration:** add a dedicated property on the summary output view and
 * switch list queries to filter server-side for accurate page sizes and cursors.
 * Until then, {@link collectFilteredSummaryPage} fills UI pages and preserves overflow
 * across cursor steps in `useSummaryListPage`.
 */
export function summaryInstanceMatchesContentTypes(
  summary: SummaryInstance,
  types: DataTypeName[],
): boolean {
  if (types.length === 0) return true;
  const viewId = VIEW_IDS.SUMMARY_OUTPUT_FILE;
  const fileProps = extractFileProperties(summary, viewId);
  const rawDesc = String(fileProps?.description ?? "");
  const ops = rawDesc.match(/^OPS:([01]+)\n/);
  if (ops) {
    return matchesOpsBitmapBits(ops[1], types);
  }
  const content = String(
    (fileProps?.content as string) ||
      (fileProps?.sourceContext as string) ||
      (fileProps?.source as string) ||
      "",
  );
  const desc = stripSummaryContentTypeBitmapLine(rawDesc);
  const text = [content, desc].filter(Boolean).join("\n\n");
  return types.some(
    (t) =>
      summaryContainsDataType(text, t) || descriptionListsContentType(desc, t),
  );
}

/**
 * Check if a summary's date range overlaps with the filter range.
 */
function summaryInDateRange(
  summary: SummaryInstance,
  filterRange: DateRange,
): boolean {
  const viewId = VIEW_IDS.SUMMARY_OUTPUT_FILE;
  const fileProps = extractFileProperties(summary, viewId);

  // Use the same content extraction logic as extractSummaryContent
  const content = String(
    (fileProps?.content as string) ||
      (fileProps?.sourceContext as string) ||
      (fileProps?.source as string) ||
      "",
  );
  const description = stripSummaryContentTypeBitmapLine(
    String(fileProps?.description ?? ""),
  );

  // Try to extract date range from content or description
  let summaryRange = extractDateRangeFromContent(content);

  if (!summaryRange) {
    // Try to parse from description (format: "Operations Summary: 2024-01-01 to 2024-01-31")
    const match = description.match(
      /(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})/,
    );
    if (match) {
      const [, startStr, endStr] = match;
      const start = new Date(startStr);
      const end = new Date(endStr);
      if (!Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime())) {
        summaryRange = { start, end };
      }
    }
  }

  if (!summaryRange) return false;

  // Check for overlap
  return (
    summaryRange.start <= filterRange.end &&
    summaryRange.end >= filterRange.start
  );
}

export function useSummaryFilters({
  summaries = [],
}: UseSummaryFiltersOptions = {}): UseSummaryFiltersResult {
  const [dateFilterEnabled, setDateFilterEnabled] = useState(false);
  const [dateRange, setDateRange] = useState<DateRange | null>(null);
  const [contentTypeFilterEnabled, setContentTypeFilterEnabled] =
    useState(false);
  const [selectedContentTypes, setSelectedContentTypes] = useState<
    DataTypeName[]
  >([]);

  const toggleDateFilter = useCallback(() => {
    setDateFilterEnabled((prev) => !prev);
  }, []);

  const toggleContentTypeFilter = useCallback(() => {
    setContentTypeFilterEnabled((prev) => !prev);
  }, []);

  const toggleContentType = useCallback(
    (type: DataTypeName, value?: boolean) => {
      setSelectedContentTypes((prev) => {
        const isCurrentlySelected = prev.includes(type);
        const shouldBeSelected =
          value !== undefined ? value : !isCurrentlySelected;

        if (shouldBeSelected && !isCurrentlySelected) {
          return [...prev, type];
        }
        if (!shouldBeSelected && isCurrentlySelected) {
          return prev.filter((t) => t !== type);
        }
        return prev;
      });
    },
    [],
  );

  const clearFilters = useCallback(() => {
    setDateFilterEnabled(false);
    setDateRange(null);
    setContentTypeFilterEnabled(false);
    setSelectedContentTypes([]);
  }, []);

  const hasActiveFilters = dateFilterEnabled || contentTypeFilterEnabled;

  const filteredSummaries = useMemo(() => {
    if (summaries.length === 0) {
      return [];
    }
    let result = [...summaries];

    // Apply date filter
    if (dateFilterEnabled && dateRange) {
      result = result.filter((summary) =>
        summaryInDateRange(summary, dateRange),
      );
    }

    // Client-side only: see module doc — DMS cannot filter on description-embedded bitmap.
    if (contentTypeFilterEnabled && selectedContentTypes.length > 0) {
      result = result.filter((summary) =>
        summaryInstanceMatchesContentTypes(summary, selectedContentTypes),
      );
    }

    return result;
  }, [
    summaries,
    dateFilterEnabled,
    dateRange,
    contentTypeFilterEnabled,
    selectedContentTypes,
  ]);

  return {
    filteredSummaries,
    dateFilterEnabled,
    dateRange,
    toggleDateFilter,
    setDateRange,
    contentTypeFilterEnabled,
    selectedContentTypes,
    toggleContentTypeFilter,
    setSelectedContentTypes,
    toggleContentType,
    clearFilters,
    hasActiveFilters,
  };
}
