/**
 * Test Configuration Card — shown when a baseline summary is selected
 * and no test result exists yet.
 */

import { AlertCircle, FileText, Play } from "lucide-react";
import { useId } from "react";
import { DataTypeControl } from "@/components/common/DataTypeControl";
import { ProgressBar } from "@/components/common/ProgressBar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { getDisplayName, getEnabledDataTypes } from "@/config/dataTypes";
import type {
  DataTypeName,
  SummaryInstance,
  TestGenerationProgress,
} from "@/types/operations";
import { formatDateShort } from "@/utils/formatting";
import {
  extractSummaryContent,
  getSummaryDisplayDate,
} from "@/utils/summaryDownload";

export interface TestConfigCardProps {
  selectedSummary: SummaryInstance;
  selectedDataTypes: DataTypeName[];
  testStartDate: string;
  testEndDate: string;
  isRunning: boolean;
  generationProgress: TestGenerationProgress;
  testError: string | null;
  onChangeStartDate: (date: string) => void;
  onChangeEndDate: (date: string) => void;
  onToggleDataType: (dataType: DataTypeName) => void;
  onSetDataTypes: (dataTypes: DataTypeName[]) => void;
  onChangeSummary: () => void;
  onRunTest: () => void;
  onDismissError: () => void;
}

export function TestConfigCard({
  selectedSummary,
  selectedDataTypes,
  testStartDate,
  testEndDate,
  isRunning,
  generationProgress,
  testError,
  onChangeStartDate,
  onChangeEndDate,
  onToggleDataType,
  onSetDataTypes,
  onChangeSummary,
  onRunTest,
  onDismissError,
}: TestConfigCardProps) {
  const testPeriodFieldId = useId();

  return (
    <Card className="border !bg-white dark:!bg-card-background">
      <CardContent className="pt-6 space-y-4 items-center">
        {/* Selected Summary Info */}
        <div className="flex w-full flex-col items-center gap-3 sm:flex-row sm:items-center sm:justify-center">
          <div className="space-y-2 min-w-0 flex flex-col items-center text-center">
            <div className="flex items-center justify-center gap-2">
              <FileText className="h-5 w-5 text-purple-600 shrink-0 mt-0.5" />
              <h3 className="font-semibold text-foreground break-words">
                {extractSummaryContent(selectedSummary).name ||
                  "Selected Summary"}
              </h3>
            </div>
            <div className="flex flex-wrap items-center justify-center gap-4 text-sm text-muted-foreground">
              <span>
                <strong>Period:</strong>{" "}
                {getSummaryDisplayDate(selectedSummary) || "—"}
              </span>
              <span>
                <strong>Created:</strong>{" "}
                {formatDateShort(new Date(selectedSummary.createdTime))}
              </span>
            </div>
            <div className="flex flex-wrap items-center justify-center gap-2">
              <span className="text-sm text-muted-foreground">
                <strong>Content:</strong>
              </span>
              {selectedDataTypes.map((dt) => (
                <Badge key={dt} variant="secondary" className="text-xs">
                  {getDisplayName(dt)}
                </Badge>
              ))}
              {selectedDataTypes.length === 0 && (
                <span className="text-sm text-muted-foreground italic">
                  None selected
                </span>
              )}
            </div>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={onChangeSummary}
            disabled={isRunning}
            className="h-7 text-xs text-muted-foreground disabled:opacity-50 shrink-0"
          >
            Change
          </Button>
        </div>

        <div className="w-full border-t pt-4 space-y-4">
          {/* Test Period */}
          <div className="flex w-full flex-col items-center gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:justify-center sm:gap-4">
            <fieldset
              aria-labelledby={`${testPeriodFieldId}-label`}
              className="border-0 p-0 m-0 min-w-0 flex flex-wrap items-center justify-center gap-2"
            >
              <span
                id={`${testPeriodFieldId}-label`}
                className="text-sm font-medium text-foreground sm:w-24 shrink-0"
              >
                Test Period:
              </span>
              <input
                id={`${testPeriodFieldId}-start`}
                type="date"
                value={testStartDate}
                onChange={(e) => onChangeStartDate(e.target.value)}
                disabled={isRunning}
                aria-label="Test period start date"
                className="h-9 sm:h-8 min-w-0 flex-1 sm:flex-initial px-2 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring disabled:opacity-50 max-w-full"
              />
              <span className="text-muted-foreground shrink-0" aria-hidden>
                →
              </span>
              <input
                id={`${testPeriodFieldId}-end`}
                type="date"
                value={testEndDate}
                onChange={(e) => onChangeEndDate(e.target.value)}
                disabled={isRunning}
                aria-label="Test period end date"
                className="h-9 sm:h-8 min-w-0 flex-1 sm:flex-initial px-2 text-sm border rounded bg-background focus:outline-none focus:ring-1 focus:ring-ring disabled:opacity-50 max-w-full"
              />
            </fieldset>
          </div>

          {/* Data Types */}
          <div className="flex w-full flex-col items-center gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:justify-center sm:gap-4">
            <span className="text-sm font-medium text-foreground sm:w-24 pt-0 sm:pt-1 shrink-0">
              Data Types:
            </span>
            <div className="min-w-0 w-full sm:w-auto flex justify-center">
              <DataTypeControl
                selectedTypes={selectedDataTypes}
                onToggle={(dataType) =>
                  !isRunning && onToggleDataType(dataType as DataTypeName)
                }
                onSelectionChange={(newSelection) =>
                  !isRunning && onSetDataTypes(newSelection as DataTypeName[])
                }
                label=""
                size="md"
                availableTypes={getEnabledDataTypes()}
                showBulkActions={true}
                disabled={isRunning}
                className="justify-center"
              />
            </div>
          </div>
        </div>

        {/* Progress Bar / Error / Run Button */}
        <div className="w-full border-t pt-4">
          {isRunning ? (
            <ProgressBar
              progress={generationProgress.progress}
              showPercent={false}
              message={generationProgress.message || "Running test..."}
              currentStep={generationProgress.currentStep}
            />
          ) : testError ? (
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between p-3 bg-red-500/10 rounded-lg border border-red-500/20">
              <div className="flex items-start gap-2 min-w-0">
                <AlertCircle className="h-4 w-4 text-red-600 shrink-0 mt-0.5" />
                <span className="text-sm text-red-700 dark:text-red-400 break-words">
                  {testError}
                </span>
              </div>
              <Button
                variant="ghost"
                size="sm"
                onClick={onDismissError}
                className="h-7 text-xs shrink-0 self-end sm:self-auto"
              >
                Dismiss
              </Button>
            </div>
          ) : (
            <Button
              onClick={onRunTest}
              disabled={
                !testStartDate || !testEndDate || selectedDataTypes.length === 0
              }
              className="w-full"
            >
              <Play className="h-4 w-4 mr-2" />
              Run Comparison Test
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
