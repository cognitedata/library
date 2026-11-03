/**
 * Reusable Filter Bar Component
 *
 * Provides date range and data type filtering with consistent styling.
 */

import { Calendar, X } from "lucide-react";
import { useId } from "react";
import { DataTypeControl } from "@/components/common/DataTypeControl";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export interface FilterBarProps {
  /** Date range filters */
  dateRange?: {
    startDate: string;
    endDate: string;
    onStartDateChange: (date: string) => void;
    onEndDateChange: (date: string) => void;
  };
  /** Data type filters */
  dataTypes?: {
    selectedTypes: string[];
    onToggle: (dataType: string) => void;
    onSelectionChange?: (newSelection: string[]) => void;
    availableTypes?: string[];
  };
  /** Whether filters are active */
  hasActiveFilters?: boolean;
  /** Clear filters callback */
  onClearFilters?: () => void;
  /** Apply filters callback */
  onApplyFilters?: () => void;
  /** Whether apply button should be disabled */
  disableApply?: boolean;
  /** Custom actions */
  actions?: React.ReactNode;
  /** Layout variant */
  layout?: "horizontal" | "responsive";
  /** Custom className */
  className?: string;
}

export function FilterBar({
  dateRange,
  dataTypes,
  hasActiveFilters = false,
  onClearFilters,
  onApplyFilters,
  disableApply = false,
  actions,
  layout = "horizontal",
  className,
}: FilterBarProps) {
  const isResponsive = layout === "responsive";
  const dateRangeGroupId = useId();
  /** Period + data types share one flex row; actions stay right via ml-auto (md+). */
  const clusterFiltersWithActions = isResponsive && Boolean(actions);

  const dateRangeFieldset = dateRange ? (
    <fieldset
      aria-labelledby={`${dateRangeGroupId}-label`}
      className={cn(
        "border-0 p-0 m-0 min-w-0 flex items-center gap-2",
        isResponsive ? "flex-wrap md:flex-nowrap" : "",
      )}
    >
      <Calendar className="h-4 w-4 text-muted-foreground" aria-hidden />
      <span
        id={`${dateRangeGroupId}-label`}
        className="text-sm text-muted-foreground font-medium shrink-0"
      >
        Period:
      </span>
      <input
        id={`${dateRangeGroupId}-start`}
        type="date"
        value={dateRange.startDate}
        onChange={(e) => dateRange.onStartDateChange(e.target.value)}
        aria-label="Period start date"
        className="h-10 min-w-0 flex-1 sm:flex-initial max-w-full px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
      />
      <span className="text-muted-foreground text-sm shrink-0" aria-hidden>
        →
      </span>
      <input
        id={`${dateRangeGroupId}-end`}
        type="date"
        value={dateRange.endDate}
        onChange={(e) => dateRange.onEndDateChange(e.target.value)}
        aria-label="Period end date"
        className="h-10 min-w-0 flex-1 sm:flex-initial max-w-full px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
      />
    </fieldset>
  ) : null;

  const betweenDateAndTypesDivider = dateRange && dataTypes && (
    <div
      className={cn(
        clusterFiltersWithActions
          ? "hidden md:block h-4 w-px bg-border shrink-0"
          : isResponsive
            ? "hidden md:block h-4 w-px bg-border"
            : "h-4 w-px bg-border",
      )}
    />
  );

  const dataTypesEl = dataTypes ? (
    <DataTypeControl
      selectedTypes={dataTypes.selectedTypes}
      onToggle={dataTypes.onToggle}
      onSelectionChange={dataTypes.onSelectionChange}
      label="Data types"
      size="md"
      availableTypes={dataTypes.availableTypes}
      className={
        clusterFiltersWithActions ? "min-w-0 w-full md:flex-1" : undefined
      }
    />
  ) : null;

  return (
    <div
      className={cn(
        "border-y border-border bg-muted/30 -mx-4 px-4",
        isResponsive
          ? "flex flex-col md:flex-row md:items-center gap-3 py-3 w-full min-w-0"
          : "flex flex-wrap items-center gap-3 py-2",
        className,
      )}
    >
      {clusterFiltersWithActions ? (
        <>
          <div className="flex flex-col md:flex-row md:items-center gap-3 min-w-0 flex-1 w-full">
            {dateRangeFieldset}
            {betweenDateAndTypesDivider}
            {dataTypesEl}
          </div>
          <div className="hidden md:block h-4 w-px bg-border shrink-0" />
          <div className="flex shrink-0 flex-wrap items-center gap-2 justify-start md:justify-end w-full md:w-auto md:ml-auto min-w-0">
            {actions}
          </div>
        </>
      ) : (
        <>
          {dateRange && (
            <>
              {dateRangeFieldset}
              {betweenDateAndTypesDivider}
            </>
          )}
          {dataTypesEl}
          {actions && (
            <>
              <div
                className={cn(
                  isResponsive
                    ? "hidden md:block h-4 w-px bg-border"
                    : "h-4 w-px bg-border",
                )}
              />
              {actions}
            </>
          )}
        </>
      )}

      {/* Apply/Clear Actions */}
      {(onApplyFilters || onClearFilters) && (
        <div
          className={cn(
            "flex flex-wrap items-center gap-2 min-w-0 w-full sm:w-auto",
            isResponsive ? "mt-2 md:mt-0 md:ml-auto md:justify-end" : "ml-auto",
          )}
        >
          {hasActiveFilters && onClearFilters && (
            <button
              type="button"
              onClick={onClearFilters}
              className="flex items-center gap-1 min-h-10 py-2 px-3 text-sm text-muted-foreground rounded-md transition-colors hover:bg-muted/70 hover:text-foreground"
            >
              <X className="h-4 w-4" />
              Clear
            </button>
          )}
          {onApplyFilters && (
            <Button
              variant="secondary"
              size="sm"
              onClick={onApplyFilters}
              disabled={disableApply}
              className="min-h-10 px-4 text-sm"
            >
              Apply
            </Button>
          )}
        </div>
      )}
    </div>
  );
}
