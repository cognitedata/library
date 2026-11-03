import {
  AlertCircle,
  Copy,
  Download,
  FileText,
  FlaskConical,
  Save,
} from "lucide-react";
import { Loader } from "@/components/Loader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { getDisplayName } from "@/config/dataTypes";
import {
  type ComparisonTestResult,
  DATA_COUNT_KEYS,
  type DataTypeName,
} from "@/types/operations";
import { formatTimestamp } from "@/utils/formatting";
import {
  getScoreBadgeVariant,
  vividScoreBadgeClassName,
} from "@/utils/testReportUtils";

export interface TestResultsHeaderProps {
  testResult: ComparisonTestResult;
  testStartDate: string;
  testEndDate: string;
  selectedDataTypes: DataTypeName[];
  isSaving: boolean;
  yamlExported: boolean;
  saveError?: string | null;
  onSave: () => Promise<void>;
  onExportYaml: () => void;
  onCopyYaml: () => Promise<void>;
  onDownloadYaml: () => void;
  onClear: () => void;
}

export function TestResultsHeader({
  testResult,
  testStartDate,
  testEndDate,
  selectedDataTypes,
  isSaving,
  yamlExported,
  saveError,
  onSave,
  onExportYaml,
  onCopyYaml,
  onDownloadYaml,
  onClear,
}: TestResultsHeaderProps) {
  const similarityVariant = getScoreBadgeVariant(testResult.similarityScore);
  const accuracyVariant = getScoreBadgeVariant(testResult.dataAccuracyScore);
  return (
    <div className="bg-muted/50 border border-border rounded-t-lg px-3 py-3 sm:px-4 shrink-0">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1 min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <FlaskConical className="w-5 h-5 text-primary shrink-0" />
            <h2 className="text-base font-semibold text-foreground">
              Test Results
            </h2>
            <Badge
              variant={similarityVariant}
              className={`text-xs ${vividScoreBadgeClassName(similarityVariant)}`}
            >
              {testResult.similarityScore.toFixed(0)}% Similarity
            </Badge>
            <Badge
              variant={accuracyVariant}
              className={`text-xs ${vividScoreBadgeClassName(accuracyVariant)}`}
            >
              {testResult.dataAccuracyScore.toFixed(0)}% Accuracy
            </Badge>
          </div>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
            <span className="flex flex-wrap items-center gap-1 min-w-0">
              <span className="font-medium text-foreground shrink-0">
                Period:
              </span>
              <span className="break-words">
                {testStartDate} — {testEndDate}
              </span>
            </span>
            <span className="flex flex-wrap items-center gap-1">
              <span className="font-medium text-foreground shrink-0">
                Tested:
              </span>
              {formatTimestamp(testResult.timestamp)}
            </span>
            <span className="flex flex-wrap items-center gap-1 min-w-0">
              <span className="font-medium text-foreground shrink-0">
                Data Types:
              </span>
              <span className="break-words">
                {selectedDataTypes
                  .map((type) => getDisplayName(type))
                  .join(", ")}
              </span>
            </span>
          </div>
        </div>
        <div className="flex flex-col gap-2 sm:items-end shrink-0 w-full sm:w-auto">
          {saveError && (
            <span className="flex items-start gap-1 text-xs text-destructive sm:text-right">
              <AlertCircle className="w-3 h-3 shrink-0 mt-0.5" />
              {saveError}
            </span>
          )}
          <div className="flex flex-wrap items-stretch sm:items-center gap-2 w-full sm:w-auto sm:justify-end">
            <Button
              onClick={onSave}
              disabled={isSaving}
              size="sm"
              className="h-8 flex-1 sm:flex-initial min-w-0"
            >
              {isSaving ? (
                <Loader size={14} className="mr-1" />
              ) : (
                <Save className="w-4 h-4 mr-1 shrink-0" />
              )}
              Save
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={onExportYaml}
              className="h-8 flex-1 sm:flex-initial min-w-0"
            >
              <FileText className="w-4 h-4 mr-1 shrink-0" />
              Download report
            </Button>
            {yamlExported && (
              <>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onCopyYaml}
                  className="h-8 flex-1 sm:flex-initial min-w-0"
                >
                  <Copy className="w-4 h-4 mr-1 shrink-0" />
                  Copy
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onDownloadYaml}
                  className="h-8 flex-1 sm:flex-initial min-w-0"
                >
                  <Download className="w-4 h-4 mr-1 shrink-0" />
                  Download
                </Button>
              </>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={onClear}
              className="h-8 text-muted-foreground flex-1 sm:flex-initial min-w-0"
            >
              Clear
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

export function TestDataCountTable({
  testResult,
}: {
  testResult: ComparisonTestResult;
}) {
  return (
    <div className="mt-3 overflow-x-auto min-w-0 -mx-1 px-1">
      <table className="w-full text-sm min-w-[520px]">
        <thead>
          <tr className="border-b">
            <th className="text-left py-2 px-3 font-medium text-muted-foreground">
              Data Type
            </th>
            <th className="text-right py-2 px-3 font-medium text-muted-foreground">
              Original
            </th>
            <th className="text-right py-2 px-3 font-medium text-muted-foreground">
              New
            </th>
            <th className="text-right py-2 px-3 font-medium text-muted-foreground">
              Change
            </th>
            <th className="text-center py-2 px-3 font-medium text-muted-foreground">
              Status
            </th>
          </tr>
        </thead>
        <tbody>
          {DATA_COUNT_KEYS.map((key) => {
            const origVal = testResult.originalDataCounts[key];
            const newVal = testResult.newDataCounts[key];
            const diff = newVal - origVal;
            return (
              <tr key={key} className="border-b last:border-0">
                <td className="py-2 px-3">
                  {getDisplayName(key as DataTypeName)}
                </td>
                <td className="py-2 px-3 text-right font-mono">{origVal}</td>
                <td className="py-2 px-3 text-right font-mono">{newVal}</td>
                <td
                  className={`py-2 px-3 text-right font-mono ${diff === 0 ? "text-muted-foreground" : diff > 0 ? "text-emerald-600" : "text-red-600"}`}
                >
                  {diff === 0 ? "0" : diff > 0 ? `+${diff}` : diff}
                </td>
                <td className="py-2 px-3 text-center">
                  {diff === 0 ? (
                    <Badge
                      variant="outline"
                      className="text-emerald-600 border-emerald-600/30 bg-emerald-500/10"
                    >
                      ✓ Match
                    </Badge>
                  ) : (
                    <Badge
                      variant="outline"
                      className="text-amber-600 border-amber-600/30 bg-amber-500/10"
                    >
                      Δ Change
                    </Badge>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
