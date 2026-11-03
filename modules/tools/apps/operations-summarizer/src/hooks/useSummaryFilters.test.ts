import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import {
  summaryInstanceMatchesContentTypes,
  useSummaryFilters,
} from "./useSummaryFilters";

const viewKey = `${VIEW_IDS.SUMMARY_OUTPUT_FILE.space}/${VIEW_IDS.SUMMARY_OUTPUT_FILE.externalId}/${VIEW_IDS.SUMMARY_OUTPUT_FILE.version}`;

function summary(overrides: Partial<SummaryInstance> = {}): SummaryInstance {
  return {
    externalId: "ops_summary_2024-01-01_2024-01-31_1",
    space: "ops_summaries",
    createdTime: new Date("2024-06-01").getTime(),
    properties: {},
    ...overrides,
  };
}

describe("summaryInstanceMatchesContentTypes", () => {
  it("matches OPS bitmap line when present", () => {
    const s = summary({
      properties: {
        [viewKey]: {
          description:
            "OPS:10100\nOperations summary. Content types: assets, files",
        },
      },
    });
    expect(summaryInstanceMatchesContentTypes(s, ["assets"])).toBe(true);
    expect(summaryInstanceMatchesContentTypes(s, ["equipment"])).toBe(false);
  });

  it("matches legacy Content types: suffix without OPS line", () => {
    const s = summary({
      properties: {
        [viewKey]: {
          description:
            "Operations summary, 2024-01-01 – 2024-01-31. Content types: assets, files",
        },
      },
    });
    expect(summaryInstanceMatchesContentTypes(s, ["files"])).toBe(true);
    expect(summaryInstanceMatchesContentTypes(s, ["equipment"])).toBe(false);
  });
});

describe("useSummaryFilters", () => {
  it("returns all summaries when filters disabled", () => {
    const summaries = [summary(), summary({ externalId: "other" })];
    const { result } = renderHook(() => useSummaryFilters({ summaries }));
    expect(result.current.filteredSummaries).toHaveLength(2);
    expect(result.current.hasActiveFilters).toBe(false);
  });

  it("filters by date range when enabled", () => {
    const summaries = [
      summary({
        createdTime: new Date("2024-01-15").getTime(),
        properties: {
          [viewKey]: {
            content: "Summary period: 2024-01-01 to 2024-01-31",
          },
        },
      }),
      summary({
        externalId: "far",
        createdTime: new Date("2025-01-01").getTime(),
        properties: {
          [viewKey]: {
            content: "Summary period: 2025-06-01 to 2025-06-30",
          },
        },
      }),
    ];

    const { result } = renderHook(() => useSummaryFilters({ summaries }));

    act(() => {
      result.current.toggleDateFilter();
      result.current.setDateRange({
        start: new Date("2024-01-01"),
        end: new Date("2024-12-31"),
      });
    });

    expect(result.current.filteredSummaries.length).toBeGreaterThanOrEqual(1);
    expect(result.current.hasActiveFilters).toBe(true);
  });

  it("toggles content type filter", () => {
    const summaries = [
      summary({
        properties: {
          [viewKey]: {
            content: "## Assets (2):\nSomething",
          },
        },
      }),
    ];

    const { result } = renderHook(() => useSummaryFilters({ summaries }));

    act(() => {
      result.current.toggleContentTypeFilter();
      result.current.toggleContentType("assets", true);
    });

    expect(result.current.contentTypeFilterEnabled).toBe(true);
    expect(result.current.filteredSummaries.length).toBe(1);
  });

  it("clearFilters resets state", () => {
    const { result } = renderHook(() =>
      useSummaryFilters({ summaries: [summary()] }),
    );

    act(() => {
      result.current.toggleDateFilter();
      result.current.clearFilters();
    });

    expect(result.current.dateFilterEnabled).toBe(false);
    expect(result.current.hasActiveFilters).toBe(false);
  });
});
