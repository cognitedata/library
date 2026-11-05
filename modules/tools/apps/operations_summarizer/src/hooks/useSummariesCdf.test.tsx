import type { CogniteClient } from "@cognite/sdk";
import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { createTestQueryClient } from "@/test-utils/wrappers";
import { SUMMARY_LIST_QUERY_RESULT_SET } from "@/utils/summaryListQuery";
import {
  UseSummariesCdfContext,
  useDeleteSummary,
  useLatestSummaryInstance,
  useOperationSummariesPicker,
  useSaveSummary,
  useSummaryListPage,
  useTestReportStatsStreaming,
} from "./useSummariesCdf";

const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

function fileNode(extId: string) {
  return {
    instanceType: "node" as const,
    externalId: extId,
    space: "ops_summaries",
    createdTime: 1,
    properties: {
      [VIEW_KEY]: {
        name: extId,
        content: "# x\nSimilarity Score: 80/100\nTotal Data: 5",
      },
    },
  };
}

function queryPageResponse(items: unknown[], next?: string) {
  const key = SUMMARY_LIST_QUERY_RESULT_SET;
  return {
    items: { [key]: items },
    nextCursor: next ? { [key]: next } : {},
  };
}

function setupWrapper(sdk: object) {
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <UseSummariesCdfContext.Provider
          value={{
            useCdfClient: () => ({
              sdk: sdk as CogniteClient,
              isAuthLoading: false,
            }),
            withCdfConcurrency: (fn) => fn(),
          }}
        >
          {children}
        </UseSummariesCdfContext.Provider>
      </QueryClientProvider>
    );
  }
  return { Wrapper };
}

describe("useSummariesCdf hooks", () => {
  it("useSummaryListPage loads operation summaries", async () => {
    const query = vi
      .fn()
      .mockResolvedValue(queryPageResponse([fileNode("ops_summary_a")]));
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(
      () =>
        useSummaryListPage({
          variant: "operation",
          serverFilters: {
            dateFilterEnabled: false,
            dateRange: null,
            contentTypeFilterEnabled: false,
            selectedContentTypes: [],
          },
        }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(result.current.items.length).toBe(1);
    });
    expect(result.current.items[0]?.externalId).toBe("ops_summary_a");
    expect(result.current.hasNextPage).toBe(false);
  });

  it("useSummaryListPage goToNextPage appends cursor", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce(
        queryPageResponse([fileNode("ops_summary_1")], "next1"),
      )
      .mockResolvedValueOnce(queryPageResponse([fileNode("ops_summary_2")]));

    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(
      () =>
        useSummaryListPage({
          variant: "operation",
          serverFilters: {
            dateFilterEnabled: false,
            dateRange: null,
            contentTypeFilterEnabled: false,
            selectedContentTypes: [],
          },
        }),
      { wrapper: Wrapper },
    );

    await waitFor(() =>
      expect(result.current.items[0]?.externalId).toBe("ops_summary_1"),
    );
    expect(result.current.hasNextPage).toBe(true);

    act(() => {
      result.current.goToNextPage();
    });

    await waitFor(() =>
      expect(result.current.items[0]?.externalId).toBe("ops_summary_2"),
    );
    expect(query).toHaveBeenCalledTimes(2);
  });

  it("useSummaryListPage fills filtered pages from multiple server batches", async () => {
    const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

    function fileNode(extId: string, description: string) {
      return {
        instanceType: "node" as const,
        externalId: extId,
        space: "ops_summaries",
        createdTime: 1,
        properties: {
          [VIEW_KEY]: { name: extId, description },
        },
      };
    }

    const query = vi
      .fn()
      .mockResolvedValueOnce(
        queryPageResponse(
          [
            fileNode("ops_summary_a", "OPS:10000\nAssets only"),
            fileNode("ops_summary_skip", "OPS:01000\nFiles only"),
            fileNode("ops_summary_skip2", "OPS:01000\nFiles only"),
          ],
          "next1",
        ),
      )
      .mockResolvedValueOnce(
        queryPageResponse([
          fileNode("ops_summary_b", "OPS:10000\nAssets only"),
          fileNode("ops_summary_c", "OPS:10000\nAssets only"),
        ]),
      );

    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(
      () =>
        useSummaryListPage({
          variant: "operation",
          pageSize: 3,
          serverFilters: {
            dateFilterEnabled: false,
            dateRange: null,
            contentTypeFilterEnabled: true,
            selectedContentTypes: ["assets"],
          },
        }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(result.current.items).toHaveLength(3);
    });
    expect(result.current.items.map((s) => s.externalId)).toEqual([
      "ops_summary_a",
      "ops_summary_b",
      "ops_summary_c",
    ]);
    expect(query).toHaveBeenCalledTimes(2);
  });

  it("useLatestSummaryInstance returns first node", async () => {
    const query = vi
      .fn()
      .mockResolvedValue(queryPageResponse([fileNode("ops_summary_x")]));
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useLatestSummaryInstance("operation"), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.data?.externalId).toBe("ops_summary_x");
    });
  });

  it("useOperationSummariesPicker maps and sorts", async () => {
    const query = vi
      .fn()
      .mockResolvedValue(
        queryPageResponse([
          fileNode("ops_summary_old"),
          fileNode("ops_summary_new"),
        ]),
      );
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useOperationSummariesPicker(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.data?.length).toBe(2);
    });
  });

  it("useSummaryListPage loads test reports with instances.query", async () => {
    const query = vi
      .fn()
      .mockResolvedValue(queryPageResponse([fileNode("test_report_a")]));
    const list = vi.fn();
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list, query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(
      () =>
        useSummaryListPage({
          variant: "test",
          serverFilters: {
            dateFilterEnabled: false,
            dateRange: null,
            contentTypeFilterEnabled: false,
            selectedContentTypes: [],
          },
        }),
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      expect(result.current.items[0]?.externalId).toBe("test_report_a");
    });
    expect(query).toHaveBeenCalledTimes(1);
    expect(list).not.toHaveBeenCalled();
  });

  it("useTestReportStatsStreaming reads the rollup node", async () => {
    const retrieve = vi.fn().mockResolvedValue({
      items: [
        {
          instanceType: "node",
          externalId: "ops_summarizer_test_report_stats",
          space: "ops_summaries",
          properties: {
            [VIEW_KEY]: {
              sourceContext: JSON.stringify({
                totalTests: 2,
                averageScore: 80,
                excellentCount: 1,
                goodCount: 1,
                poorCount: 0,
                averageDataVolume: 4,
                totalScore: 160,
                totalVolume: 8,
              }),
            },
          },
        },
      ],
    });
    const list = vi.fn();
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list, retrieve, upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useTestReportStatsStreaming(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.data?.totalTests).toBe(2);
    });
    expect(list).not.toHaveBeenCalled();
  });

  it("useDeleteSummary calls instances.delete", async () => {
    const del = vi.fn().mockResolvedValue({});
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), delete: del, upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDeleteSummary(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await result.current.mutateAsync({
        externalId: "ops_summary_z",
        space: "ops_summaries",
        createdTime: 1,
        properties: {},
      });
    });

    expect(del).toHaveBeenCalled();
  });

  it("useDeleteSummary subtracts test-report stats when the rollup exists", async () => {
    const del = vi.fn().mockResolvedValue({});
    const upsert = vi.fn().mockResolvedValue({});
    const retrieve = vi.fn().mockResolvedValue({
      items: [
        {
          instanceType: "node",
          externalId: "ops_summarizer_test_report_stats",
          space: "ops_summaries",
          properties: {
            [VIEW_KEY]: {
              sourceContext: JSON.stringify({
                totalTests: 1,
                averageScore: 80,
                excellentCount: 0,
                goodCount: 1,
                poorCount: 0,
                averageDataVolume: 5,
                totalScore: 80,
                totalVolume: 5,
              }),
            },
          },
        },
      ],
    });
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), delete: del, upsert, retrieve },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDeleteSummary(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await result.current.mutateAsync({
        externalId: "test_report_z",
        space: "ops_summaries",
        createdTime: 1,
        properties: {
          [VIEW_KEY]: {
            content: "Similarity Score: 80/100\nTotal Data: 5",
          },
        },
      });
    });

    expect(del).toHaveBeenCalled();
    expect(upsert).toHaveBeenCalled();
    const written = JSON.parse(
      (
        upsert.mock.calls[0][0] as {
          items: Array<{
            sources: Array<{ properties: { sourceContext: string } }>;
          }>;
        }
      ).items[0].sources[0].properties.sourceContext,
    ) as { totalTests: number };
    expect(written.totalTests).toBe(0);
  });

  it("useSaveSummary uploads PDF when module loads", async () => {
    const upsert = vi.fn().mockResolvedValue({});
    const post = vi.fn().mockResolvedValueOnce({
      data: {
        items: [
          {
            instanceId: { space: "ops_summaries", externalId: "x" },
            uploadUrl: "https://upload.example/put",
          },
        ],
      },
    });
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
    });

    const { Wrapper } = setupWrapper({
      project: "proj",
      instances: { list: vi.fn(), delete: vi.fn(), upsert },
      post,
    });

    const { result } = renderHook(() => useSaveSummary(), { wrapper: Wrapper });

    await act(async () => {
      await result.current.mutateAsync({
        content: "# Hello",
        startDate: new Date("2024-01-01"),
        endDate: new Date("2024-01-31"),
        contentTypes: ["assets"],
      });
    });

    expect(upsert).toHaveBeenCalled();
    expect(post).toHaveBeenCalled();
  });
});
