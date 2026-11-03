import { FileText } from "lucide-react";
import { useId } from "react";
import type { SummaryInstance } from "@/types/operations";
import {
  extractSummaryContent,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";
import {
  encodeTestBaselineSelectionKey,
  summaryInstanceNodeKey,
} from "@/utils/summaryInstanceMapping";
import { getOperationSummaryPickerMax } from "@/utils/summaryListQuery";

export interface TestsCreateBaselineFieldsProps {
  summaries: SummaryInstance[];
  filteredSummaries: SummaryInstance[];
  filterStartDate: string;
  filterEndDate: string;
  onFilterStartDate: (value: string) => void;
  onFilterEndDate: (value: string) => void;
  selectedSummaryId: string;
  onSelectSummary: (value: string) => void;
}

export function TestsCreateBaselineFields({
  summaries,
  filteredSummaries,
  filterStartDate,
  filterEndDate,
  onFilterStartDate,
  onFilterEndDate,
  selectedSummaryId,
  onSelectSummary,
}: TestsCreateBaselineFieldsProps) {
  const summaryPickerFilterDateRangeId = useId();

  return (
    <>
      <div className="flex w-full flex-col items-center gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-center py-3 border-y border-border bg-muted/30 -mx-4 px-4">
        <fieldset
          aria-labelledby={`${summaryPickerFilterDateRangeId}-label`}
          className="border-0 p-0 m-0 min-w-0 flex items-center justify-center gap-2 flex-wrap"
        >
          <FileText className="h-3 w-3 text-muted-foreground" aria-hidden />
          <span
            id={`${summaryPickerFilterDateRangeId}-label`}
            className="text-xs text-muted-foreground"
          >
            Filter:
          </span>
          <input
            id={`${summaryPickerFilterDateRangeId}-start`}
            type="date"
            value={filterStartDate}
            onChange={(e) => onFilterStartDate(e.target.value)}
            aria-label="Filter baseline summaries from date"
            className="h-10 min-w-0 flex-1 sm:flex-initial max-w-full px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
          />
          <span className="text-muted-foreground text-sm shrink-0" aria-hidden>
            →
          </span>
          <input
            id={`${summaryPickerFilterDateRangeId}-end`}
            type="date"
            value={filterEndDate}
            onChange={(e) => onFilterEndDate(e.target.value)}
            aria-label="Filter baseline summaries to date"
            className="h-10 min-w-0 flex-1 sm:flex-initial max-w-full px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </fieldset>

        <div className="hidden sm:block h-4 w-px bg-border shrink-0" />

        <div className="flex flex-col items-center gap-1.5 sm:flex-row sm:items-center sm:justify-center min-w-0 w-full sm:w-auto sm:min-w-[200px]">
          <span
            className="text-xs text-muted-foreground whitespace-nowrap cursor-help shrink-0"
            title="The original summary to compare against"
          >
            Baseline:
          </span>
          <select
            aria-label="Select baseline summary"
            value={selectedSummaryId}
            onChange={(e) => onSelectSummary(e.target.value)}
            className="w-full min-w-0 flex-1 h-9 sm:h-7 px-2 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
          >
            <option value="">Select a summary...</option>
            {filteredSummaries.map((summary) => {
              const { name } = extractSummaryContent(summary);
              const date = getSummaryDisplayDate(summary);
              return (
                <option
                  key={summaryInstanceNodeKey(summary)}
                  value={encodeTestBaselineSelectionKey(summary)}
                >
                  {name} - {date}
                </option>
              );
            })}
          </select>
        </div>
      </div>
      {summaries.length > 0 && (
        <p className="w-full text-center text-xs text-muted-foreground px-6 pb-2">
          Showing up to {getOperationSummaryPickerMax()} most recent operation
          summaries (by creation time).
        </p>
      )}
    </>
  );
}
