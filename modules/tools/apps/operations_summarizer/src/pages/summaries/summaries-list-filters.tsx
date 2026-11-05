import { Calendar, X } from "lucide-react";
import { useId } from "react";
import { DataTypeControl } from "@/components/common/DataTypeControl";
import { Button } from "@/components/ui/button";
import type { DataTypeName } from "@/types/operations";

export interface SummariesListFiltersProps {
  startDate: string;
  endDate: string;
  onStartDate: (value: string) => void;
  onEndDate: (value: string) => void;
  selectedContentTypes: DataTypeName[];
  contentTypeFilterEnabled: boolean;
  enabledDataTypes: string[];
  onToggleContentType: (dataType: DataTypeName) => void;
  onToggleContentTypeFilter: () => void;
  onSetContentTypes: (types: DataTypeName[]) => void;
  hasActiveFilters: boolean;
  onClear: () => void;
  onApply: () => void;
}

export function SummariesListFilters({
  startDate,
  endDate,
  onStartDate,
  onEndDate,
  selectedContentTypes,
  contentTypeFilterEnabled,
  enabledDataTypes,
  onToggleContentType,
  onToggleContentTypeFilter,
  onSetContentTypes,
  hasActiveFilters,
  onClear,
  onApply,
}: SummariesListFiltersProps) {
  const listFilterDateRangeId = useId();
  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="flex flex-col md:flex-row md:items-center gap-3 py-3 bg-muted/30 px-4 sm:px-6">
        <fieldset
          aria-labelledby={`${listFilterDateRangeId}-label`}
          className="border-0 p-0 m-0 min-w-0 flex flex-col md:flex-row md:items-center gap-2 min-w-0"
        >
          <div className="flex items-center gap-2 shrink-0">
            <Calendar className="h-3 w-3 text-muted-foreground" aria-hidden />
            <span
              id={`${listFilterDateRangeId}-label`}
              className="text-xs text-muted-foreground"
            >
              Date Range:
            </span>
          </div>
          <div className="flex flex-wrap items-center gap-2 min-w-0 w-full md:w-auto">
            <input
              id={`${listFilterDateRangeId}-start`}
              type="date"
              value={startDate}
              onChange={(e) => onStartDate(e.target.value)}
              aria-label="Filter summaries from date"
              className="h-10 min-w-0 flex-1 sm:flex-initial px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring max-w-full"
            />
            <span
              className="text-muted-foreground text-sm shrink-0"
              aria-hidden
            >
              →
            </span>
            <input
              id={`${listFilterDateRangeId}-end`}
              type="date"
              value={endDate}
              onChange={(e) => onEndDate(e.target.value)}
              aria-label="Filter summaries to date"
              className="h-10 min-w-0 flex-1 sm:flex-initial px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring max-w-full"
            />
          </div>
        </fieldset>

        <div className="hidden md:block h-4 w-px bg-border" />

        <div className="min-w-0">
          <DataTypeControl
            selectedTypes={selectedContentTypes}
            onToggle={(dataType) => {
              if (
                !contentTypeFilterEnabled &&
                selectedContentTypes.length === 0
              ) {
                onToggleContentTypeFilter();
              }
              onToggleContentType(dataType as DataTypeName);
            }}
            onSelectionChange={(newSelection) => {
              if (
                !contentTypeFilterEnabled &&
                selectedContentTypes.length === 0 &&
                newSelection.length > 0
              ) {
                onToggleContentTypeFilter();
              }
              onSetContentTypes(newSelection as DataTypeName[]);
              if (newSelection.length === 0 && contentTypeFilterEnabled) {
                onToggleContentTypeFilter();
              }
            }}
            label="Data types"
            size="sm"
            availableTypes={enabledDataTypes}
            showBulkActions={true}
          />
        </div>

        <div className="flex flex-wrap items-center gap-2 w-full md:w-auto md:ml-auto">
          {hasActiveFilters && (
            <button
              type="button"
              onClick={onClear}
              className="flex items-center gap-1 rounded-md px-1.5 py-0.5 -mx-1 text-xs text-muted-foreground transition-colors hover:bg-muted/70 hover:text-foreground shrink-0"
            >
              <X className="h-3 w-3" />
              Clear
            </button>
          )}
          <Button
            variant="secondary"
            size="sm"
            onClick={onApply}
            disabled={!startDate || !endDate}
            className="px-3 text-xs shrink-0"
          >
            Apply
          </Button>
        </div>
      </div>
    </div>
  );
}
