/**
 * Podcast panel for a saved summary: generate/regenerate, player, and transcript.
 */

import { AlertCircle, RefreshCw } from "lucide-react";
import { createContext, useContext } from "react";
import { Loader } from "@/components/Loader";
import { Button } from "@/components/ui/button";
import { usePodcastGeneration } from "@/hooks/usePodcastGeneration";
import { useSavedPodcast } from "@/hooks/useSavedPodcast";
import type { SummaryInstance } from "@/types/operations";
import { markdownToHtml } from "@/utils/formatting";

export interface PodcastTabProps {
  summary: SummaryInstance;
}

type PodcastTabDeps = {
  useSavedPodcast: typeof useSavedPodcast;
  usePodcastGeneration: typeof usePodcastGeneration;
};

const defaultDeps: PodcastTabDeps = {
  useSavedPodcast,
  usePodcastGeneration,
};

export const PodcastTabContext = createContext<PodcastTabDeps>(defaultDeps);

export function PodcastTab({ summary }: PodcastTabProps) {
  const { useSavedPodcast: useSaved, usePodcastGeneration: useGeneration } =
    useContext(PodcastTabContext);

  const { podcast, isLoading, isError, error, refetch, hasPodcast } = useSaved(
    summary.externalId,
  );

  const {
    generate,
    isGenerating,
    error: generateError,
    clearError,
  } = useGeneration();

  const displayError =
    generateError ||
    (isError ? error?.message || "Failed to load podcast" : null);

  const script = podcast?.script ?? null;
  const audioUrl = podcast?.audioUrl ?? null;

  async function handleGenerate() {
    clearError();
    try {
      await generate(summary);
    } catch {
      // Error is surfaced via generateError state.
    }
  }

  return (
    <div className="max-w-4xl mx-auto py-4 px-3 sm:py-6 sm:px-6 space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => void handleGenerate()}
          disabled={isGenerating || isLoading}
          className="h-8"
        >
          {isGenerating ? (
            <>
              <Loader size={14} className="mr-1" />
              Generating…
            </>
          ) : (
            <>
              <RefreshCw className="h-4 w-4 mr-1" aria-hidden />
              {hasPodcast ? "Regenerate podcast" : "Generate podcast"}
            </>
          )}
        </Button>
        {isError && !isGenerating && (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => refetch()}
            className="h-8"
          >
            Retry load
          </Button>
        )}
      </div>

      {isGenerating && (
        <p className="text-sm text-muted-foreground">
          Generating script and audio. This can take a few minutes.
        </p>
      )}

      {displayError && (
        <div className="flex items-start gap-2 text-sm text-destructive">
          <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" aria-hidden />
          <p>{displayError}</p>
        </div>
      )}

      {isLoading && !hasPodcast && !isGenerating && (
        <div className="flex items-center gap-2 py-2 text-muted-foreground">
          <Loader size={18} />
          <span className="text-sm">Checking for an existing podcast…</span>
        </div>
      )}

      {!isLoading && !hasPodcast && !isGenerating && !displayError && (
        <p className="text-sm text-muted-foreground">
          No podcast yet for this summary. Generate one to create a spoken
          script and audio.
        </p>
      )}

      {hasPodcast && audioUrl && (
        <audio controls src={audioUrl} className="w-full" preload="metadata">
          <track kind="captions" />
        </audio>
      )}

      {hasPodcast && !audioUrl && script && (
        <p className="text-sm text-muted-foreground">
          Script is available, but audio was not found for this podcast.
        </p>
      )}

      {script && (
        <div className="bg-background border border-border rounded-lg px-4 py-3 overflow-x-auto">
          <p className="text-xs font-medium text-muted-foreground mb-2">
            Transcript
          </p>
          <div
            className="prose prose-sm dark:prose-invert max-w-none w-full leading-relaxed
              prose-p:text-sm prose-p:text-muted-foreground prose-p:my-2"
            /* biome-ignore lint/security/noDangerouslySetInnerHtml: rendered from app-controlled podcast script */
            dangerouslySetInnerHTML={{
              __html: markdownToHtml(script),
            }}
          />
        </div>
      )}
    </div>
  );
}
