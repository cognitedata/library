import { QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { SavedPodcast } from "@/services/podcastFiles";
import { createTestQueryClient } from "@/test-utils/wrappers";
import { UseSavedPodcastContext, useSavedPodcast } from "./useSavedPodcast";

describe(useSavedPodcast.name, () => {
  it("exposes loading then success with podcast data", async () => {
    const podcast: SavedPodcast = {
      script: "Hello",
      audioUrl: "https://dl.example/a.mp3",
      scriptExternalId: "sum_podcast_script",
      audioExternalId: "sum_podcast_audio",
    };
    const fetchSavedPodcast = vi.fn().mockResolvedValue(podcast);
    const queryClient = createTestQueryClient();

    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UseSavedPodcastContext.Provider
            value={{
              useCdfClient: () =>
                ({
                  sdk: { project: "demo" },
                  isAuthLoading: false,
                }) as ReturnType<
                  typeof import("@/contexts/CdfClientContext").useCdfClient
                >,
              fetchSavedPodcast,
            }}
          >
            {children}
          </UseSavedPodcastContext.Provider>
        </QueryClientProvider>
      );
    }

    const { result } = renderHook(() => useSavedPodcast("sum-1"), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.hasPodcast).toBe(true);
    });
    expect(result.current.podcast).toEqual(podcast);
    expect(result.current.isError).toBe(false);
    expect(fetchSavedPodcast).toHaveBeenCalled();
  });

  it("exposes not-found as null podcast without error", async () => {
    const fetchSavedPodcast = vi.fn().mockResolvedValue(null);
    const queryClient = createTestQueryClient();

    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UseSavedPodcastContext.Provider
            value={{
              useCdfClient: () =>
                ({
                  sdk: { project: "demo" },
                  isAuthLoading: false,
                }) as ReturnType<
                  typeof import("@/contexts/CdfClientContext").useCdfClient
                >,
              fetchSavedPodcast,
            }}
          >
            {children}
          </UseSavedPodcastContext.Provider>
        </QueryClientProvider>
      );
    }

    const { result } = renderHook(() => useSavedPodcast("sum-1"), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    expect(result.current.podcast).toBeNull();
    expect(result.current.hasPodcast).toBe(false);
    expect(result.current.isError).toBe(false);
  });

  it("exposes error state when fetch fails", async () => {
    const fetchSavedPodcast = vi
      .fn()
      .mockRejectedValue(new Error("network down"));
    const queryClient = createTestQueryClient();

    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UseSavedPodcastContext.Provider
            value={{
              useCdfClient: () =>
                ({
                  sdk: { project: "demo" },
                  isAuthLoading: false,
                }) as ReturnType<
                  typeof import("@/contexts/CdfClientContext").useCdfClient
                >,
              fetchSavedPodcast,
            }}
          >
            {children}
          </UseSavedPodcastContext.Provider>
        </QueryClientProvider>
      );
    }

    const { result } = renderHook(() => useSavedPodcast("sum-1"), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });
    expect(result.current.error?.message).toBe("network down");
  });
});
