import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createQueryWithCdfWrapper } from "@/test-utils/wrappers";
import { SUMMARY_LIST_QUERY_RESULT_SET } from "@/utils/summaryListQuery";
import { useDefaultTestsPageCdfBundle } from "./useDefaultTestsPageCdfBundle";

const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

function testReportNode(extId: string) {
  return {
    instanceType: "node" as const,
    externalId: extId,
    space: "ops_summaries",
    createdTime: 1,
    properties: {
      [VIEW_KEY]: {
        name: extId,
        content: "# Report\nSimilarity Score: 75/100\nTotal Data: 3",
      },
    },
  };
}

function opsSummaryNode(extId: string) {
  return {
    instanceType: "node" as const,
    externalId: extId,
    space: "ops_summaries",
    createdTime: 2,
    properties: {
      [VIEW_KEY]: {
        name: extId,
        content: "# Summary",
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

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: vi.fn(),
}));

import { useCogniteSdk } from "@cognite/app-sdk/react";

function setupWrapper(sdk: object) {
  vi.mocked(useCogniteSdk).mockReturnValue(
    sdk as ReturnType<typeof useCogniteSdk>,
  );
  return createQueryWithCdfWrapper();
}

describe("useDefaultTestsPageCdfBundle", () => {
  beforeEach(() => {
    vi.mocked(useCogniteSdk).mockReset();
  });

  it("reflects loading state before list resolves", () => {
    const neverResolve = new Promise<never>(() => {});
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: {
        list: vi.fn().mockReturnValue(neverResolve),
        query: vi.fn().mockReturnValue(neverResolve),
        delete: vi.fn(),
        upsert: vi.fn(),
      },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDefaultTestsPageCdfBundle(), {
      wrapper: Wrapper,
    });

    expect(result.current.testReportsLoading).toBe(true);
  });

  it("sets isTestReportsError when the list call fails", async () => {
    const list = vi.fn().mockRejectedValue(new Error("list failed"));
    const query = vi.fn().mockResolvedValue(queryPageResponse([]));
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list, query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDefaultTestsPageCdfBundle(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.isTestReportsError).toBe(true);
    });
    expect(result.current.testReports).toHaveLength(0);
  });

  it("wires test reports list, summaries picker, stats, and delete mutation", async () => {
    const list = vi.fn().mockResolvedValue({
      items: [testReportNode("test_report_bundle_1")],
      nextCursor: undefined,
    });
    const query = vi
      .fn()
      .mockResolvedValue(
        queryPageResponse([opsSummaryNode("ops_summary_bundle")]),
      );

    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list, query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDefaultTestsPageCdfBundle(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.testReports.length).toBe(1);
    });
    await waitFor(() => {
      expect(result.current.summaries.length).toBe(1);
    });
    await waitFor(() => {
      expect(result.current.streamedTestStats?.totalTests).toBe(1);
    });

    expect(result.current.testReports[0]?.externalId).toBe(
      "test_report_bundle_1",
    );
    expect(result.current.summaries[0]?.externalId).toBe("ops_summary_bundle");
    expect(result.current.streamedTestStats?.averageScore).toBe(75);
    expect(result.current.deleteMutation.mutateAsync).toBeDefined();
    expect(result.current.testReportsLoading).toBe(false);
    expect(result.current.summariesPickerLoading).toBe(false);
    expect(result.current.testStatsLoading).toBe(false);
  });
});
