import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { createTestQueryClient } from "@/test-utils/wrappers";
import { SUMMARY_LIST_QUERY_RESULT_SET } from "@/utils/summaryListQuery";
import { useDefaultSummariesListPipeline } from "./useDefaultSummariesListPipeline";

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
        content: "# x",
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
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: vi.fn(),
}));

import { useCogniteSdk } from "@cognite/app-sdk/react";

function setupWrapper(sdk: object) {
  vi.mocked(useCogniteSdk).mockReturnValue(
    sdk as ReturnType<typeof useCogniteSdk>,
  );
  const queryClient = createTestQueryClient();
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <CdfClientProvider>{children}</CdfClientProvider>
      </QueryClientProvider>
    );
  }
  return { queryClient, Wrapper };
}

describe("useDefaultSummariesListPipeline", () => {
  it("exposes list data, mutations, and filter toggles from real wiring", async () => {
    const query = vi
      .fn()
      .mockResolvedValue(queryPageResponse([fileNode("ops_summary_pipe")]));
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDefaultSummariesListPipeline(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.items.length).toBe(1);
    });
    expect(result.current.items[0]?.externalId).toBe("ops_summary_pipe");
    expect(result.current.deleteMutation.mutateAsync).toBeDefined();
    expect(result.current.saveMutation.mutateAsync).toBeDefined();
    expect(result.current.dateFilterEnabled).toBe(false);

    act(() => {
      result.current.toggleDateFilter();
    });
    expect(result.current.dateFilterEnabled).toBe(true);
  });

  it("sets isError when the server query fails", async () => {
    const query = vi.fn().mockRejectedValue(new Error("CDF query failed"));
    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDefaultSummariesListPipeline(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });
    expect(result.current.items).toHaveLength(0);
  });

  it("clearFilters resets pagination after navigating to next page", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce(
        queryPageResponse([fileNode("ops_summary_p1")], "c1"),
      )
      .mockResolvedValue(queryPageResponse([fileNode("ops_summary_p2")]));

    const { Wrapper } = setupWrapper({
      project: "p",
      instances: { list: vi.fn(), query, delete: vi.fn(), upsert: vi.fn() },
      post: vi.fn(),
    });

    const { result } = renderHook(() => useDefaultSummariesListPipeline(), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(result.current.items[0]?.externalId).toBe("ops_summary_p1"),
    );
    expect(result.current.hasNextPage).toBe(true);

    act(() => {
      result.current.goToNextPage();
    });

    await waitFor(() =>
      expect(result.current.items[0]?.externalId).toBe("ops_summary_p2"),
    );
    expect(result.current.pageDisplay).toBe(2);

    act(() => {
      result.current.clearFilters();
    });

    expect(result.current.pageDisplay).toBe(1);
  });
});
