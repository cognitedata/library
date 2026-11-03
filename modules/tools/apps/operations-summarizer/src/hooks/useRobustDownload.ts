/**
 * Robust Download Hook.
 * Provides error-resilient download functionality with state preservation and recovery options.
 */

import { useCallback, useState } from "react";
import type { SummaryInstance } from "@/types/operations";
import { getSavePdfError, logError } from "@/utils/errorHandling";
import {
  downloadSummaryAsMarkdown,
  downloadSummaryAsPdf,
  extractSummaryContent,
} from "@/utils/summaryDownload";

interface UseRobustDownloadOptions {
  preserveState?: boolean;
  fallbackToMarkdown?: boolean;
}

interface UseRobustDownloadResult {
  downloadPdf: (summary: SummaryInstance) => Promise<void>;
  downloadMarkdown: (summary: SummaryInstance) => Promise<void>;
  isDownloading: boolean;
  downloadError: string | null;
  lastFailedSummary: SummaryInstance | null;
  retryLastDownload: () => Promise<void>;
  clearError: () => void;
  canRetry: boolean;
}

interface DownloadState {
  summary: SummaryInstance | null;
  format: "pdf" | "markdown" | null;
  error: string | null;
}

/**
 * Enhanced download hook with error recovery and state preservation.
 * Provides fallback options and retry mechanisms for robust download handling.
 */
export function useRobustDownload({
  preserveState = true,
  fallbackToMarkdown = true,
}: UseRobustDownloadOptions = {}): UseRobustDownloadResult {
  const [isDownloading, setIsDownloading] = useState(false);
  const [downloadState, setDownloadState] = useState<DownloadState>({
    summary: null,
    format: null,
    error: null,
  });

  const clearError = useCallback(() => {
    setDownloadState((prev) => ({ ...prev, error: null }));
  }, []);

  const performDownload = useCallback(
    async (
      summary: SummaryInstance,
      format: "pdf" | "markdown",
      isRetry = false,
    ) => {
      setIsDownloading(true);

      if (preserveState && !isRetry) {
        setDownloadState({
          summary,
          format,
          error: null,
        });
      }

      try {
        if (format === "pdf") {
          // Enhanced PDF download with error detection
          await performPdfDownload(summary);
        } else {
          // Markdown download (more reliable fallback)
          downloadSummaryAsMarkdown(summary);
        }

        // Success - clear error state
        if (preserveState) {
          setDownloadState((prev) => ({ ...prev, error: null }));
        }
      } catch (error) {
        const errorMessage = getSavePdfError(error);
        logError(error, `Download ${format} failed`, {
          summaryId: summary.externalId,
          format,
          isRetry,
        });

        if (preserveState) {
          setDownloadState((prev) => ({
            ...prev,
            error: errorMessage,
          }));
        }

        // If PDF fails and fallback is enabled, try markdown
        if (format === "pdf" && fallbackToMarkdown && !isRetry) {
          try {
            downloadSummaryAsMarkdown(summary);

            // Update error to show fallback succeeded
            if (preserveState) {
              setDownloadState((prev) => ({
                ...prev,
                error:
                  "PDF download failed, but markdown version was downloaded successfully.",
              }));
            }
          } catch (fallbackError) {
            const fallbackErrorMessage = `PDF download failed, and markdown fallback also failed: ${getSavePdfError(fallbackError)}`;
            if (preserveState) {
              setDownloadState((prev) => ({
                ...prev,
                error: fallbackErrorMessage,
              }));
            }
          }
        } else {
          throw error; // Re-throw if not using fallback or if this was a retry
        }
      } finally {
        setIsDownloading(false);
      }
    },
    [preserveState, fallbackToMarkdown],
  );

  const downloadPdf = useCallback(
    async (summary: SummaryInstance) => {
      await performDownload(summary, "pdf");
    },
    [performDownload],
  );

  const downloadMarkdown = useCallback(
    async (summary: SummaryInstance) => {
      await performDownload(summary, "markdown");
    },
    [performDownload],
  );

  const retryLastDownload = useCallback(async () => {
    if (!downloadState.summary || !downloadState.format) {
      throw new Error("No previous download to retry");
    }

    await performDownload(downloadState.summary, downloadState.format, true);
  }, [downloadState.summary, downloadState.format, performDownload]);

  const canRetry = Boolean(
    downloadState.summary &&
    downloadState.format &&
    downloadState.error &&
    !isDownloading,
  );

  return {
    downloadPdf,
    downloadMarkdown,
    isDownloading,
    downloadError: downloadState.error,
    lastFailedSummary: downloadState.summary,
    retryLastDownload,
    clearError,
    canRetry,
  };
}

async function performPdfDownload(summary: SummaryInstance): Promise<void> {
  const { content } = extractSummaryContent(summary);
  if (!content || content.trim().length === 0) {
    throw new Error("Cannot download PDF: Summary content is empty");
  }
  downloadSummaryAsPdf(summary, content);
}
