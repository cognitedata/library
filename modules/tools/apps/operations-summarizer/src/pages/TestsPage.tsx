/**
 * Tests Page Component
 *
 * Unified test management with creation and history.
 * Combines functionality from Testing + Test History.
 */

import { AlertCircle } from "lucide-react";
import { DeleteConfirmation } from "@/components/common/ConfirmationDialog";
import { TestingInfoDialog } from "@/components/TestingInfoDialog";
import { TestsPageModelDepsProvider } from "@/contexts/TestsPageModelDepsContext";
import { useTestsPageModel } from "@/hooks/useTestsPageModel";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { TestReportModal } from "./TestReportModal";
import { TestsCreateForm } from "./tests/TestsCreateForm";
import {
  TestsFetchErrorBanner,
  TestsHistoryBar,
  TestsPageToolbar,
  TestsScoreLegend,
} from "./tests/TestsPageToolbar";
import { TestsReportsSection } from "./tests/TestsReportsSection";

export function TestsPage() {
  return (
    <TestsPageModelDepsProvider>
      <TestsPageContent />
    </TestsPageModelDepsProvider>
  );
}

function TestsPageContent() {
  const model = useTestsPageModel();

  return (
    <div className="min-h-full bg-background">
      <div className="max-w-7xl mx-auto px-3 py-3 space-y-3">
        <TestsPageToolbar
          pageDisplay={model.testReportPageDisplay}
          totalTests={model.stats.totalTests}
          statsUnavailable={model.stats.unavailable}
          onToggleCreateForm={() =>
            model.setShowCreateForm(!model.showCreateForm)
          }
          onRefresh={model.enhancedRefresh}
          isRefreshDisabled={model.isLoading || model.isRefreshingData}
          refreshSuccess={model.refreshSuccess}
          refreshError={model.refreshError}
          refreshMessage={model.refreshMessage}
          onShowInfo={() => model.setShowInfoDialog(true)}
        />
        <TestsScoreLegend />

        {model.isFetchError && (
          <TestsFetchErrorBanner
            isTestReportsError={model.isTestReportsError}
            isSummariesPickerError={model.isSummariesPickerError}
            isTestStatsError={model.isTestStatsError}
          />
        )}

        {model.deleteError && (
          <div className="flex items-center gap-2 p-3 rounded-md bg-destructive/10 text-destructive text-sm">
            <AlertCircle className="h-4 w-4 shrink-0" />
            {model.deleteError}
          </div>
        )}

        {model.showCreateForm && (
          <TestsCreateForm
            summaries={model.summaries}
            filteredSummaries={model.filteredSummaries}
            filterStartDate={model.filterStartDate}
            filterEndDate={model.filterEndDate}
            onFilterStartDate={model.setFilterStartDate}
            onFilterEndDate={model.setFilterEndDate}
            selectedSummaryId={model.selectedSummaryId}
            onSelectSummary={model.setSelectedSummaryId}
            selectedSummary={model.selectedSummary}
            testResult={model.testResult}
            selectedDataTypes={model.selectedDataTypes}
            testStartDate={model.testStartDate}
            testEndDate={model.testEndDate}
            isRunning={model.isRunning}
            generationProgress={model.generationProgress}
            testError={model.testError}
            onChangeStartDate={model.setTestStartDate}
            onChangeEndDate={model.setTestEndDate}
            onToggleDataType={model.toggleDataType}
            onSetDataTypes={model.setSelectedDataTypes}
            onClearSelectedSummary={() => model.setSelectedSummaryId("")}
            onRunTest={model.handleRunTest}
            onDismissError={model.clearResults}
            isSaving={model.isSaving}
            yamlExported={model.yamlExported}
            yamlContent={model.yamlContent}
            onSave={model.saveTestResults}
            onExportYaml={model.exportToYaml}
            onCopyYaml={model.copyYamlToClipboard}
            onDownloadYaml={model.downloadYaml}
            onClearResults={model.clearResults}
            saveError={model.savingError}
            onClose={() => model.setShowCreateForm(false)}
            onCreateSummaryFirst={model.handleCreateSummaryFirst}
          />
        )}

        {!model.showCreateForm && (
          <TestsHistoryBar
            statsUnavailable={model.stats.unavailable}
            testStatsLoading={model.testStatsLoading}
            hasStreamedStats={Boolean(model.streamedTestStats)}
            totalTests={model.stats.totalTests}
            averageScore={model.stats.averageScore}
            excellentCount={model.stats.excellentCount}
            goodCount={model.stats.goodCount}
            poorCount={model.stats.poorCount}
            minScore={model.minScore}
            maxScore={model.maxScore}
            onMinScoreChange={model.setMinScore}
            onMaxScoreChange={model.setMaxScore}
            hasActiveHistoryFilters={model.hasActiveHistoryFilters}
            onClearFilters={model.clearHistoryFilters}
          />
        )}

        <TestsReportsSection
          isLoading={model.isLoading}
          testReports={model.testReports}
          filteredReports={model.filteredReports}
          summariesCount={model.summaries.length}
          hasActiveHistoryFilters={model.hasActiveHistoryFilters}
          pageDisplay={model.testReportPageDisplay}
          hasNext={model.testReportsHasNext}
          hasPrev={model.testReportsHasPrev}
          onNext={model.testReportsGoNext}
          onPrev={model.testReportsGoPrev}
          onView={model.openReportModal}
          onDelete={model.setDeleteTarget}
          onCreateSummaryFirst={model.handleCreateSummaryFirst}
          onCreateTest={() => model.setShowCreateForm(true)}
        />
      </div>

      <TestReportModal
        isOpen={model.isModalOpen}
        onClose={() => model.setIsModalOpen(false)}
        selectedReport={model.selectedReport}
      />

      <DeleteConfirmation
        open={!!model.deleteTarget}
        onOpenChange={(open) => !open && model.setDeleteTarget(null)}
        itemType="Test Report"
        itemName={
          model.deleteTarget
            ? extractSummaryContent(model.deleteTarget).name ||
              "Untitled Report"
            : undefined
        }
        onConfirm={model.handleDelete}
        onCancel={() => model.setDeleteTarget(null)}
        isProcessing={model.deleteMutation.isPending}
        additionalConsequences={[
          "Test results and comparison data will be lost",
          "Historical performance metrics will be removed",
        ]}
      />

      <TestingInfoDialog
        open={model.showInfoDialog}
        onOpenChange={model.setShowInfoDialog}
      />
    </div>
  );
}
