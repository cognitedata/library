import { FileText } from "lucide-react";
import { ProgressBar } from "@/components/common/ProgressBar";
import type { GeneratedSummary, SummaryContents } from "@/types/operations";
import type { SummaryGenerationProgressUpdate } from "@/utils/summaryGenerationTypes";
import { type SavedPdfInfo, SummaryResultPanel } from "../SummaryResultPanel";

export interface CreateSummaryFeedbackProps {
  isGenerating: boolean;
  progress: SummaryGenerationProgressUpdate;
  elapsedSeconds: number;
  showSummary: boolean;
  generatedSummary: GeneratedSummary | null;
  periodStart: Date;
  periodEnd: Date;
  summaryContents: SummaryContents;
  savedPdfInfo: SavedPdfInfo | null;
  isSaving: boolean;
  onDownload: () => void;
  saveError: string | null;
}

/** Progress, generated result, or the idle prompt under the create form. */
export function CreateSummaryFeedback({
  isGenerating,
  progress,
  elapsedSeconds,
  showSummary,
  generatedSummary,
  periodStart,
  periodEnd,
  summaryContents,
  savedPdfInfo,
  isSaving,
  onDownload,
  saveError,
}: CreateSummaryFeedbackProps) {
  if (isGenerating) {
    return (
      <ProgressBar
        progress={progress.progress}
        showPercent={false}
        message={
          progress.message
            ? `${progress.message} (${elapsedSeconds}s)`
            : "Generating in CDF Function…"
        }
        currentStep={progress.currentStep}
      />
    );
  }

  if (showSummary && generatedSummary) {
    return (
      <SummaryResultPanel
        generatedSummary={generatedSummary}
        periodStart={periodStart}
        periodEnd={periodEnd}
        summaryContents={summaryContents}
        savedPdfInfo={savedPdfInfo}
        isSaving={isSaving}
        onDownload={onDownload}
        saveError={saveError}
      />
    );
  }

  if (!showSummary) {
    return (
      <div className="flex items-center justify-center py-8 text-muted-foreground">
        <FileText className="w-5 h-5 mr-2" />
        <span className="text-sm">
          Configure options above and click Generate to create a summary.
        </span>
      </div>
    );
  }

  return null;
}
