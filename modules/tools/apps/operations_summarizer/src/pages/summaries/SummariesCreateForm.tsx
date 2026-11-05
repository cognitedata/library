import type { ChangeEvent } from "react";
import { Card, CardContent } from "@/components/ui/card";
import type {
  DataTypeName,
  GeneratedSummary,
  SummaryContents,
} from "@/types/operations";
import type { SummaryGenerationProgressUpdate } from "@/utils/summaryGenerationTypes";
import type { SavedPdfInfo } from "../SummaryResultPanel";
import { CreateSummaryFeedback } from "./CreateSummaryFeedback";
import { CreateSummaryFieldGroups } from "./CreateSummaryFieldGroups";
import { CreateSummaryHeader } from "./CreateSummaryHeader";

export {
  SummariesListFilters,
  type SummariesListFiltersProps,
} from "./summaries-list-filters";

export interface SummariesCreateFormProps {
  hasSelectedAgent: boolean;
  generationError: string | null;
  periodStart: Date;
  periodEnd: Date;
  formatDateForInput: (date: Date) => string;
  onStartDateChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onEndDateChange: (event: ChangeEvent<HTMLInputElement>) => void;
  enabledDataTypes: string[];
  summaryContents: SummaryContents;
  onToggleContent: (dataType: DataTypeName) => void;
  daysSelected: number;
  isGenerating: boolean;
  onCancel: () => void;
  onGenerate: () => void;
  selectedCount: number;
  progress: SummaryGenerationProgressUpdate;
  elapsedSeconds: number;
  showSummary: boolean;
  generatedSummary: GeneratedSummary | null;
  savedPdfInfo: SavedPdfInfo | null;
  isSaving: boolean;
  onDownload: () => void;
  saveError: string | null;
  onClose: () => void;
}

export function SummariesCreateForm({
  hasSelectedAgent,
  generationError,
  periodStart,
  periodEnd,
  formatDateForInput,
  onStartDateChange,
  onEndDateChange,
  enabledDataTypes,
  summaryContents,
  onToggleContent,
  daysSelected,
  isGenerating,
  onCancel,
  onGenerate,
  selectedCount,
  progress,
  elapsedSeconds,
  showSummary,
  generatedSummary,
  savedPdfInfo,
  isSaving,
  onDownload,
  saveError,
  onClose,
}: SummariesCreateFormProps) {
  return (
    <Card className="border border-primary/20">
      <CardContent className="w-full items-center! space-y-4">
        <CreateSummaryHeader
          hasSelectedAgent={hasSelectedAgent}
          generationError={generationError}
          onClose={onClose}
        />
        <CreateSummaryFieldGroups
          startDate={formatDateForInput(periodStart)}
          endDate={formatDateForInput(periodEnd)}
          onStartDateChange={onStartDateChange}
          onEndDateChange={onEndDateChange}
          enabledDataTypes={enabledDataTypes}
          summaryContents={summaryContents}
          onToggleContent={onToggleContent}
          daysSelected={daysSelected}
          isGenerating={isGenerating}
          hasSelectedAgent={hasSelectedAgent}
          selectedCount={selectedCount}
          onCancel={onCancel}
          onGenerate={onGenerate}
        />
        <CreateSummaryFeedback
          isGenerating={isGenerating}
          progress={progress}
          elapsedSeconds={elapsedSeconds}
          showSummary={showSummary}
          generatedSummary={generatedSummary}
          periodStart={periodStart}
          periodEnd={periodEnd}
          summaryContents={summaryContents}
          savedPdfInfo={savedPdfInfo}
          isSaving={isSaving}
          onDownload={onDownload}
          saveError={saveError}
        />
      </CardContent>
    </Card>
  );
}
