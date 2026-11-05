import { Card, CardContent } from "@/components/ui/card";
import type {
  ComparisonTestResult,
  DataTypeName,
  SummaryInstance,
  TestGenerationProgress,
} from "@/types/operations";
import { TestConfigCard } from "../TestConfigCard";
import { TestResultsPanel } from "../TestResultsPanel";
import { TestsCreateBaselineFields } from "./TestsCreateBaselineFields";
import { TestsCreateFormHeader } from "./TestsCreateFormHeader";
import { TestsCreateFormPrompts } from "./TestsCreateFormPrompts";

export interface TestsCreateFormProps {
  summaries: SummaryInstance[];
  filteredSummaries: SummaryInstance[];
  filterStartDate: string;
  filterEndDate: string;
  onFilterStartDate: (value: string) => void;
  onFilterEndDate: (value: string) => void;
  selectedSummaryId: string;
  onSelectSummary: (value: string) => void;
  selectedSummary: SummaryInstance | undefined;
  testResult: ComparisonTestResult | null;
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
  onClearSelectedSummary: () => void;
  onRunTest: () => void;
  onDismissError: () => void;
  isSaving: boolean;
  yamlExported: boolean;
  yamlContent: string;
  onSave: () => Promise<void>;
  onExportYaml: () => void;
  onCopyYaml: () => Promise<void>;
  onDownloadYaml: () => void;
  onClearResults: () => void;
  saveError: string | null;
  onClose: () => void;
  onCreateSummaryFirst: () => void;
}

export function TestsCreateForm({
  summaries,
  filteredSummaries,
  filterStartDate,
  filterEndDate,
  onFilterStartDate,
  onFilterEndDate,
  selectedSummaryId,
  onSelectSummary,
  selectedSummary,
  testResult,
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
  onClearSelectedSummary,
  onRunTest,
  onDismissError,
  isSaving,
  yamlExported,
  yamlContent,
  onSave,
  onExportYaml,
  onCopyYaml,
  onDownloadYaml,
  onClearResults,
  saveError,
  onClose,
  onCreateSummaryFirst,
}: TestsCreateFormProps) {
  return (
    <Card className="border border-primary/20">
      <CardContent className="space-y-4 items-center">
        <TestsCreateFormHeader onClose={onClose} />

        <TestsCreateBaselineFields
          summaries={summaries}
          filteredSummaries={filteredSummaries}
          filterStartDate={filterStartDate}
          filterEndDate={filterEndDate}
          onFilterStartDate={onFilterStartDate}
          onFilterEndDate={onFilterEndDate}
          selectedSummaryId={selectedSummaryId}
          onSelectSummary={onSelectSummary}
        />

        <TestsCreateFormPrompts
          hasSummaries={summaries.length > 0}
          hasSelectedSummary={selectedSummary != null}
          hasTestResult={testResult != null}
          onCreateSummaryFirst={onCreateSummaryFirst}
        />

        {selectedSummary && !testResult && (
          <TestConfigCard
            selectedSummary={selectedSummary}
            selectedDataTypes={selectedDataTypes}
            testStartDate={testStartDate}
            testEndDate={testEndDate}
            isRunning={isRunning}
            generationProgress={generationProgress}
            testError={testError}
            onChangeStartDate={onChangeStartDate}
            onChangeEndDate={onChangeEndDate}
            onToggleDataType={onToggleDataType}
            onSetDataTypes={onSetDataTypes}
            onChangeSummary={onClearSelectedSummary}
            onRunTest={onRunTest}
            onDismissError={onDismissError}
          />
        )}

        {testResult && (
          <TestResultsPanel
            testResult={testResult}
            testStartDate={testStartDate}
            testEndDate={testEndDate}
            selectedDataTypes={selectedDataTypes}
            isSaving={isSaving}
            yamlExported={yamlExported}
            yamlContent={yamlContent}
            onSave={onSave}
            onExportYaml={onExportYaml}
            onCopyYaml={onCopyYaml}
            onDownloadYaml={onDownloadYaml}
            onClear={onClearResults}
            saveError={saveError}
          />
        )}
      </CardContent>
    </Card>
  );
}
