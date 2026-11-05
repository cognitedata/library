import { Calendar } from "lucide-react";
import { type ChangeEvent, useId } from "react";

export interface CreateSummaryDateFieldsProps {
  startDate: string;
  endDate: string;
  onStartDateChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onEndDateChange: (event: ChangeEvent<HTMLInputElement>) => void;
}

/** Period start/end inputs for the create-summary form. */
export function CreateSummaryDateFields({
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
}: CreateSummaryDateFieldsProps) {
  const dateRangeGroupId = useId();

  return (
    <fieldset
      aria-labelledby={`${dateRangeGroupId}-label`}
      className="border-0 p-0 m-0 min-w-0 flex items-center gap-2 flex-wrap md:flex-nowrap"
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
        value={startDate}
        onChange={onStartDateChange}
        aria-label="Period start date"
        className="h-10 min-w-0 flex-1 sm:flex-initial max-w-full px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
      />
      <span className="text-muted-foreground text-sm shrink-0" aria-hidden>
        →
      </span>
      <input
        id={`${dateRangeGroupId}-end`}
        type="date"
        value={endDate}
        onChange={onEndDateChange}
        aria-label="Period end date"
        className="h-10 min-w-0 flex-1 sm:flex-initial max-w-full px-3 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring"
      />
    </fieldset>
  );
}
