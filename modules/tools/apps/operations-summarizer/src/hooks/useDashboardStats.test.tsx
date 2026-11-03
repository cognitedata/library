import { renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { createOperationsWithQueryWrapper } from "@/test-utils/wrappers";
import { useDashboardStats } from "./useDashboardStats";

const { mockSdk } = vi.hoisted(() => ({
  mockSdk: {
    project: "p",
    get: vi.fn().mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
    post: vi.fn(),
    instances: {
      list: vi.fn().mockResolvedValue({ items: [], nextCursor: undefined }),
    },
  },
}));

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => mockSdk,
}));

describe("useDashboardStats", () => {
  it("aggregates empty summaries and exposes refetch", async () => {
    const { Wrapper } = createOperationsWithQueryWrapper();

    const { result } = renderHook(() => useDashboardStats(), {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.summaries).toEqual([]);
    expect(result.current.testReports).toEqual([]);
    expect(result.current.testStats.total).toBe(0);

    await expect(result.current.refetch()).resolves.toBeUndefined();
  });
});
