import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import { PodcastTab, PodcastTabContext } from "./PodcastTab";

const summary: SummaryInstance = {
  externalId: "ops_summary_2026-01-01_2026-01-07_abc",
  space: "ops_summaries",
  createdTime: Date.now(),
  properties: {},
};

describe(PodcastTab.name, () => {
  it("renders empty state with generate button", () => {
    render(
      <PodcastTabContext.Provider
        value={{
          useSavedPodcast: () => ({
            podcast: null,
            isLoading: false,
            isError: false,
            error: null,
            refetch: vi.fn(),
            hasPodcast: false,
          }),
          usePodcastGeneration: () => ({
            generate: vi.fn(),
            isGenerating: false,
            error: null,
            result: null,
            clearError: vi.fn(),
          }),
        }}
      >
        <PodcastTab summary={summary} />
      </PodcastTabContext.Provider>,
    );

    expect(screen.getByText(/No podcast yet/i)).toBeTruthy();
    expect(
      screen.getByRole("button", { name: /Generate podcast/i }),
    ).toBeTruthy();
  });

  it("renders loading state while checking for an existing podcast", () => {
    render(
      <PodcastTabContext.Provider
        value={{
          useSavedPodcast: () => ({
            podcast: undefined,
            isLoading: true,
            isError: false,
            error: null,
            refetch: vi.fn(),
            hasPodcast: false,
          }),
          usePodcastGeneration: () => ({
            generate: vi.fn(),
            isGenerating: false,
            error: null,
            result: null,
            clearError: vi.fn(),
          }),
        }}
      >
        <PodcastTab summary={summary} />
      </PodcastTabContext.Provider>,
    );

    expect(screen.getByText(/Checking for an existing podcast/i)).toBeTruthy();
  });

  it("renders player and transcript when a podcast exists", () => {
    render(
      <PodcastTabContext.Provider
        value={{
          useSavedPodcast: () => ({
            podcast: {
              script: "Hello from the podcast",
              audioUrl: "https://dl.example/audio.mp3",
              scriptExternalId: "x_podcast_script",
              audioExternalId: "x_podcast_audio",
            },
            isLoading: false,
            isError: false,
            error: null,
            refetch: vi.fn(),
            hasPodcast: true,
          }),
          usePodcastGeneration: () => ({
            generate: vi.fn(),
            isGenerating: false,
            error: null,
            result: null,
            clearError: vi.fn(),
          }),
        }}
      >
        <PodcastTab summary={summary} />
      </PodcastTabContext.Provider>,
    );

    expect(screen.getByText("Transcript")).toBeTruthy();
    expect(screen.getByText(/Hello from the podcast/i)).toBeTruthy();
    expect(document.querySelector("audio")).toBeTruthy();
    expect(
      screen.getByRole("button", { name: /Regenerate podcast/i }),
    ).toBeTruthy();
  });

  it("renders error state", () => {
    render(
      <PodcastTabContext.Provider
        value={{
          useSavedPodcast: () => ({
            podcast: null,
            isLoading: false,
            isError: false,
            error: null,
            refetch: vi.fn(),
            hasPodcast: false,
          }),
          usePodcastGeneration: () => ({
            generate: vi.fn(),
            isGenerating: false,
            error: "Generation failed",
            result: null,
            clearError: vi.fn(),
          }),
        }}
      >
        <PodcastTab summary={summary} />
      </PodcastTabContext.Provider>,
    );

    expect(screen.getByText("Generation failed")).toBeTruthy();
  });

  it("calls generate when the button is clicked", () => {
    const generate = vi.fn().mockResolvedValue({ success: true });
    const clearError = vi.fn();

    render(
      <PodcastTabContext.Provider
        value={{
          useSavedPodcast: () => ({
            podcast: null,
            isLoading: false,
            isError: false,
            error: null,
            refetch: vi.fn(),
            hasPodcast: false,
          }),
          usePodcastGeneration: () => ({
            generate,
            isGenerating: false,
            error: null,
            result: null,
            clearError,
          }),
        }}
      >
        <PodcastTab summary={summary} />
      </PodcastTabContext.Provider>,
    );

    fireEvent.click(screen.getByRole("button", { name: /Generate podcast/i }));
    expect(clearError).toHaveBeenCalled();
    expect(generate).toHaveBeenCalledWith(summary);
  });
});
