import { Play, StopCircle } from "lucide-react";
import type { ChangeEvent } from "react";
import { DataTypeControl } from "@/components/common/DataTypeControl";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { DataTypeName, SummaryContents } from "@/types/operations";
import { CreateSummaryDateFields } from "./CreateSummaryDateFields";

const DATA_TYPE_NAMES: readonly DataTypeName[] = [
  "assets",
  "files",
  "work_orders",
  "time_series",
  "equipment",
];

function isDataTypeName(value: string): value is DataTypeName {
  return DATA_TYPE_NAMES.some((name) => name === value);
}

export interface CreateSummaryFieldGroupsProps {
  startDate: string;
  endDate: string;
  onStartDateChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onEndDateChange: (event: ChangeEvent<HTMLInputElement>) => void;
  enabledDataTypes: string[];
  summaryContents: SummaryContents;
  onToggleContent: (dataType: DataTypeName) => void;
  daysSelected: number;
  isGenerating: boolean;
  hasSelectedAgent: boolean;
  selectedCount: number;
  onCancel: () => void;
  onGenerate: () => void;
}

/** Data-type selection and generate/stop actions beside the period fields. */
export function CreateSummaryFieldGroups({
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
  enabledDataTypes,
  summaryContents,
  onToggleContent,
  daysSelected,
  isGenerating,
  hasSelectedAgent,
  selectedCount,
  onCancel,
  onGenerate,
}: CreateSummaryFieldGroupsProps) {
  const selectedTypes = enabledDataTypes.filter(
    (type): type is DataTypeName =>
      isDataTypeName(type) && summaryContents[type],
  );
  const generateDisabled =
    !hasSelectedAgent || selectedCount === 0 || daysSelected <= 0;

  return (
    <div className="border-y border-border bg-muted/30 -mx-4 px-4 flex flex-col md:flex-row md:items-center gap-3 py-3 w-full min-w-0">
      <div className="flex flex-col md:flex-row md:items-center gap-3 min-w-0 flex-1 w-full">
        <CreateSummaryDateFields
          startDate={startDate}
          endDate={endDate}
          onStartDateChange={onStartDateChange}
          onEndDateChange={onEndDateChange}
        />
        <div className="hidden md:block h-4 w-px bg-border shrink-0" />
        <DataTypeControl
          selectedTypes={selectedTypes}
          onToggle={(dataType) => {
            if (isDataTypeName(dataType)) onToggleContent(dataType);
          }}
          label="Data types"
          size="md"
          availableTypes={enabledDataTypes}
          className="min-w-0 w-full md:flex-1"
        />
      </div>
      <div className="hidden md:block h-4 w-px bg-border shrink-0" />
      <div className="flex shrink-0 flex-wrap items-center gap-2 justify-start md:justify-end w-full md:w-auto md:ml-auto min-w-0">
        <div className="flex flex-wrap items-center gap-2 min-w-0">
          <Badge variant="secondary" className="text-xs">
            {daysSelected}d
          </Badge>
          {isGenerating ? (
            <Button
              onClick={onCancel}
              variant="destructive"
              size="sm"
              className="min-h-10 px-4"
            >
              <StopCircle className="w-4 h-4 mr-2" />
              Stop waiting
            </Button>
          ) : (
            <Button
              onClick={onGenerate}
              disabled={generateDisabled}
              size="sm"
              className="min-h-10 px-4"
            >
              <Play className="w-4 h-4 mr-2" />
              Generate ({selectedCount})
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}
