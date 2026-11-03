import { describe, expect, it } from "vitest";
import { queryResponseMock } from "@/test-utils/testDoubles";
import {
  buildLatestSummaryListRequest,
  buildSummaryInstancesQueryRequest,
  buildSummaryListFilter,
  buildSummaryListRequest,
  buildSummaryServerFilterFingerprint,
  formatSummaryContentTypeBitmapLine,
  getCogniteFileViewPropertiesSlim,
  parseSummaryQueryPageResponse,
  SUMMARY_LIST_QUERY_RESULT_SET,
  stripSummaryContentTypeBitmapLine,
} from "./summaryListQuery";

describe("summaryListQuery", () => {
  it("buildSummaryServerFilterFingerprint", () => {
    expect(
      buildSummaryServerFilterFingerprint({
        dateFilterEnabled: false,
        dateRange: null,
        contentTypeFilterEnabled: false,
        selectedContentTypes: [],
      }),
    ).toBe("none");

    const start = new Date("2024-01-01T00:00:00.000Z");
    const end = new Date("2024-01-31T00:00:00.000Z");
    const fp = buildSummaryServerFilterFingerprint({
      dateFilterEnabled: true,
      dateRange: { start, end },
      contentTypeFilterEnabled: true,
      selectedContentTypes: ["assets", "files"],
    });
    expect(fp).toContain("d");
    expect(fp).toContain("c");
    expect(fp).toContain("assets");
    expect(fp).toContain("files");
  });

  it("formatSummaryContentTypeBitmapLine and strip", () => {
    const line = formatSummaryContentTypeBitmapLine([
      "assets",
      "files",
      "work_orders",
      "time_series",
      "equipment",
    ]);
    expect(line.startsWith("OPS:")).toBe(true);
    expect(stripSummaryContentTypeBitmapLine(`${line}hello`)).toBe("hello");
  });

  it("podcast external ids do not match operation or test-report prefixes", () => {
    const podcastId = "ops_podcast_ops_summary_x_script";
    const empty = {
      dateFilterEnabled: false,
      dateRange: null,
      contentTypeFilterEnabled: false,
      selectedContentTypes: [],
    };
    for (const variant of ["operation", "test"] as const) {
      const filter = buildSummaryListFilter(variant, empty);
      const clauses = (filter as { and: { prefix?: { value: string } }[] }).and;
      const prefix =
        clauses.find((clause) => clause.prefix)?.prefix?.value ?? "";
      expect(prefix.length).toBeGreaterThan(0);
      expect(podcastId.startsWith(prefix)).toBe(false);
    }
  });

  it("buildSummaryListFilter includes space and prefix", () => {
    const f = buildSummaryListFilter("test", {
      dateFilterEnabled: false,
      dateRange: null,
      contentTypeFilterEnabled: false,
      selectedContentTypes: [],
    });
    expect(f).toHaveProperty("and");
    const and = (f as { and: unknown[] }).and;
    expect(and.length).toBeGreaterThanOrEqual(2);
  });

  it("buildSummaryListRequest has view source and limit", () => {
    const req = buildSummaryListRequest({
      variant: "operation",
      limit: 10,
      filters: {
        dateFilterEnabled: false,
        dateRange: null,
        contentTypeFilterEnabled: false,
        selectedContentTypes: [],
      },
    });
    expect(req.instanceType).toBe("node");
    expect(req.limit).toBe(10);
    expect(req.sources?.[0]?.source?.externalId).toBe("CogniteFile");
  });

  it("buildSummaryListRequest passes cursor when set", () => {
    const req = buildSummaryListRequest({
      variant: "test",
      limit: 5,
      cursor: "abc",
      filters: {
        dateFilterEnabled: false,
        dateRange: null,
        contentTypeFilterEnabled: false,
        selectedContentTypes: [],
      },
    });
    expect(req.cursor).toBe("abc");
  });

  it("buildLatestSummaryListRequest uses limit 1", () => {
    const req = buildLatestSummaryListRequest({ variant: "operation" });
    expect(req.limit).toBe(1);
  });

  it("buildSummaryInstancesQueryRequest uses query shape and slim properties", () => {
    const req = buildSummaryInstancesQueryRequest({
      limit: 15,
      cursor: "c1",
      filters: {
        dateFilterEnabled: false,
        dateRange: null,
        contentTypeFilterEnabled: false,
        selectedContentTypes: [],
      },
    });
    expect(req.with?.[SUMMARY_LIST_QUERY_RESULT_SET]).toBeDefined();
    expect(req.select?.[SUMMARY_LIST_QUERY_RESULT_SET]?.limit).toBe(15);
    expect(
      req.select?.[SUMMARY_LIST_QUERY_RESULT_SET]?.sources?.[0]?.properties,
    ).toEqual([...getCogniteFileViewPropertiesSlim()]);
    expect(req.cursors?.[SUMMARY_LIST_QUERY_RESULT_SET]).toBe("c1");
  });

  it("parseSummaryQueryPageResponse reads items and nextCursor", () => {
    const node = { instanceType: "node" as const, externalId: "x", space: "s" };
    const parsed = parseSummaryQueryPageResponse(
      queryResponseMock({
        items: { [SUMMARY_LIST_QUERY_RESULT_SET]: [node] },
        nextCursor: { [SUMMARY_LIST_QUERY_RESULT_SET]: "nxt" },
      }),
    );
    expect(parsed.items).toEqual([node]);
    expect(parsed.nextCursor).toBe("nxt");
  });

  it("parseSummaryQueryPageResponse treats empty next cursor as undefined", () => {
    expect(
      parseSummaryQueryPageResponse(
        queryResponseMock({ items: {}, nextCursor: {} }),
      ).nextCursor,
    ).toBeUndefined();
  });
});
