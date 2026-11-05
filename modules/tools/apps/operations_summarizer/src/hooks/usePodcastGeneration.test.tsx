import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { PodcastFunctionService } from "@/services/podcastFunctionService";
import { createTestQueryClient } from "@/test-utils/wrappers";
import type { SummaryInstance } from "@/types/operations";
import {
  UsePodcastGenerationContext,
  usePodcastGeneration,
} from "./usePodcastGeneration";

const summary: SummaryInstance = {
  externalId: "ops_summary_2026-01-01_2026-01-07_abc",
  space: "ops_summaries",
  createdTime: Date.now(),
  properties: {},
};

describe(usePodcastGeneration.name, () => {
  it("sets loading then success result", async () => {
    const generate = vi.fn().mockResolvedValue({
      success: true,
      script: "Generated",
      audio_generated: true,
    });
    const createService = vi.fn((): PodcastFunctionService => ({ generate }));
    const queryClient = createTestQueryClient();
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");

    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UsePodcastGenerationContext.Provider
            value={{
              useCdfClient: () =>
                ({
                  sdk: { project: "demo" },
                  isAuthLoading: false,
                }) as ReturnType<
                  typeof import("@/contexts/CdfClientContext").useCdfClient
                >,
              createService,
            }}
          >
            {children}
          </UsePodcastGenerationContext.Provider>
        </QueryClientProvider>
      );
    }

    const { result } = renderHook(() => usePodcastGeneration(), {
      wrapper: Wrapper,
    });

    let promise: Promise<unknown>;
    act(() => {
      promise = result.current.generate(summary);
    });
    expect(result.current.isGenerating).toBe(true);

    await act(async () => {
      await promise;
    });

    await waitFor(() => {
      expect(result.current.isGenerating).toBe(false);
    });
    expect(result.current.result?.script).toBe("Generated");
    expect(result.current.error).toBeNull();
    expect(generate).toHaveBeenCalledWith({
      summaryExternalId: summary.externalId,
      summarySpace: summary.space,
      sourceSummaryId: summary.externalId,
      startDate: "2026-01-01",
      endDate: "2026-01-07",
    });
    expect(invalidateSpy).toHaveBeenCalled();
  });

  it("exposes error state when generation fails", async () => {
    const generate = vi.fn().mockRejectedValue(new Error("call failed"));
    const queryClient = createTestQueryClient();

    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UsePodcastGenerationContext.Provider
            value={{
              useCdfClient: () =>
                ({
                  sdk: { project: "demo" },
                  isAuthLoading: false,
                }) as ReturnType<
                  typeof import("@/contexts/CdfClientContext").useCdfClient
                >,
              createService: () => ({ generate }),
            }}
          >
            {children}
          </UsePodcastGenerationContext.Provider>
        </QueryClientProvider>
      );
    }

    const { result } = renderHook(() => usePodcastGeneration(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await expect(result.current.generate(summary)).rejects.toThrow(
        "call failed",
      );
    });

    expect(result.current.isGenerating).toBe(false);
    expect(result.current.error).toBe("call failed");
  });
});
