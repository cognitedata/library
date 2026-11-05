import type { CogniteClient } from "@cognite/sdk";
import type { TestsPageDeleteMutationRef } from "@/hooks/useDefaultTestsPageCdfBundle";
import type {
  ComparisonTestResult,
  DataTypeName,
  SummaryInstance,
} from "@/types/operations";
import {
  getDeleteError,
  getErrorMessage,
  logError,
} from "@/utils/errorHandling";
import { buildTestingPageYaml } from "@/utils/testingYaml";

interface SaveTestReportParams {
  sdk: CogniteClient;
  testResult: ComparisonTestResult;
  testStartDate: string;
  testEndDate: string;
}

type SaveTestReportToCdf = (params: SaveTestReportParams) => Promise<void>;

export function downloadYamlFile(yamlContent: string): void {
  const blob = new Blob([yamlContent], { type: "text/yaml" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `test_case_${new Date().toISOString().slice(0, 10)}.yaml`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export async function copyYamlToClipboard(yamlContent: string): Promise<void> {
  await navigator.clipboard.writeText(yamlContent);
}

interface TestFlowActionsParams {
  testResult: ComparisonTestResult | null;
  sdk: CogniteClient | null;
  testStartDate: string;
  testEndDate: string;
  selectedDataTypes: DataTypeName[];
  yamlContent: string;
  saveTestReportToCdf: SaveTestReportToCdf;
  refetch: () => Promise<void>;
  setIsSaving: (value: boolean) => void;
  setSavingError: (value: string | null) => void;
  setShowCreateForm: (value: boolean) => void;
  setYamlContent: (value: string) => void;
  setYamlExported: (value: boolean) => void;
  clearResults: () => void;
}

export async function saveTestResultsAction(
  params: TestFlowActionsParams,
): Promise<void> {
  const {
    testResult,
    sdk,
    testStartDate,
    testEndDate,
    saveTestReportToCdf,
    refetch,
    setIsSaving,
    setSavingError,
    setShowCreateForm,
    clearResults,
  } = params;

  if (!testResult || !sdk) return;
  setIsSaving(true);
  setSavingError(null);
  try {
    await saveTestReportToCdf({
      sdk,
      testResult,
      testStartDate,
      testEndDate,
    });
    await refetch();
    setShowCreateForm(false);
    clearResults();
  } catch (error) {
    logError(error, "Save test results");
    setSavingError(getErrorMessage(error, "Saving test results"));
  } finally {
    setIsSaving(false);
  }
}

export function exportTestResultToYaml(
  testResult: ComparisonTestResult | null,
  testStartDate: string,
  testEndDate: string,
  selectedDataTypes: DataTypeName[],
  setYamlContent: (value: string) => void,
  setYamlExported: (value: boolean) => void,
): void {
  if (!testResult) return;
  const yaml = buildTestingPageYaml(
    testResult,
    testStartDate,
    testEndDate,
    selectedDataTypes,
  );
  setYamlContent(yaml);
  setYamlExported(true);
}

export async function deleteTestReportAction(
  deleteTarget: SummaryInstance | null,
  deleteMutation: TestsPageDeleteMutationRef,
  setDeleteError: (value: string | null) => void,
  setDeleteTarget: (value: SummaryInstance | null) => void,
): Promise<void> {
  if (!deleteTarget) return;
  setDeleteError(null);
  try {
    await deleteMutation.mutateAsync(deleteTarget);
    setDeleteTarget(null);
  } catch (err) {
    logError(err, "Delete test report");
    setDeleteError(getDeleteError(err, "report"));
  }
}
