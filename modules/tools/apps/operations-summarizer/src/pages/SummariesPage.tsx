/**
 * Summaries Page Component
 *
 * Unified summary management with creation and history.
 * Combines functionality from Generate + Past Summaries.
 */

import { AlertCircle, FileText, Plus, RefreshCw, Search } from "lucide-react";
import pdfWorkerUrl from "pdfjs-dist/build/pdf.worker.min.mjs?url";
import { pdfjs } from "react-pdf";
import { DeleteConfirmation } from "@/components/common/ConfirmationDialog";
import { DownloadErrorDialog } from "@/components/common/DownloadErrorDialog";
import { ErrorRecoveryDialog } from "@/components/common/ErrorRecoveryDialog";
import { SummariesTableSkeleton } from "@/components/common/list-table-skeletons";
import { SummaryTable } from "@/components/summaries/SummaryTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { SummariesPageModelDepsProvider } from "@/contexts/SummariesPageModelDepsContext";
import { useSummariesPageModel } from "@/hooks/useSummariesPageModel";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { SummaryDetailModal } from "./SummaryDetailModal";
import {
  SummariesCreateForm,
  SummariesListFilters,
} from "./summaries/SummariesCreateForm";

pdfjs.GlobalWorkerOptions.workerSrc = pdfWorkerUrl;

function refreshToneClass(refreshSuccess: boolean, refreshError: boolean) {
  if (refreshSuccess) return "text-emerald-600 dark:text-emerald-400";
  if (refreshError) return "text-red-600 dark:text-red-400";
  return "";
}

export function SummariesPage() {
  return (
    <SummariesPageModelDepsProvider>
      <SummariesPageContent />
    </SummariesPageModelDepsProvider>
  );
}

function SummariesPageContent() {
  const model = useSummariesPageModel();
  const tone = refreshToneClass(model.refreshSuccess, model.refreshError);
  const refreshBusy = model.isLoading || model.isRefreshingData;

  return (
    <div className="min-h-full bg-background">
      <div className="max-w-7xl mx-auto px-3 py-3 space-y-3">
        <div className="flex flex-col md:flex-row md:items-center gap-3">
          <div className="flex items-center gap-3 flex-1">
            <FileText className="w-6 h-6 text-primary shrink-0" />
            <h1 className="text-2xl font-bold text-foreground">Summaries</h1>
            <Badge variant="secondary" className="text-sm">
              Page {model.pageDisplay}
              {(model.serverHasNext || model.serverHasPrev) && " · paged"}
            </Badge>
          </div>
          <div className="flex items-center gap-2 md:ml-auto">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => model.setShowCreateForm(!model.showCreateForm)}
              className="h-8"
              title="Create new summary"
            >
              <Plus className="h-4 w-4 mr-1" />
              Create new summary
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={model.enhancedRefresh}
              disabled={refreshBusy}
              className={`h-8 transition-colors ${tone}`}
              title="Refresh summaries"
            >
              <RefreshCw
                className={`h-4 w-4 mr-1 ${refreshBusy ? "animate-spin" : ""} ${tone}`}
              />
              {model.refreshMessage}
            </Button>
          </div>
        </div>

        {model.isFetchError && (
          <div className="flex items-center gap-2 p-3 rounded-md bg-destructive/10 text-destructive text-sm">
            <AlertCircle className="h-4 w-4 shrink-0" />
            Failed to load summaries. Check your connection and try refreshing.
          </div>
        )}

        {model.deleteError && (
          <div className="flex items-center gap-2 p-3 rounded-md bg-destructive/10 text-destructive text-sm">
            <AlertCircle className="h-4 w-4 shrink-0" />
            {model.deleteError}
          </div>
        )}

        {model.showCreateForm && (
          <SummariesCreateForm
            hasSelectedAgent={Boolean(model.state.selectedAgent)}
            generationError={model.generationError}
            periodStart={model.state.summaryPeriodStart}
            periodEnd={model.state.summaryPeriodEnd}
            formatDateForInput={model.formatDateForInput}
            onStartDateChange={model.handleStartDateChange}
            onEndDateChange={model.handleEndDateChange}
            enabledDataTypes={model.enabledDataTypes}
            summaryContents={model.state.summaryContents}
            onToggleContent={model.toggleSummaryContent}
            daysSelected={model.daysSelected}
            isGenerating={model.isGenerating}
            onCancel={model.handleCancelGeneration}
            onGenerate={model.handleGenerateSummary}
            selectedCount={model.selectedCount}
            progress={model.progress}
            elapsedSeconds={model.elapsedSeconds}
            showSummary={model.state.showSummary}
            generatedSummary={model.state.generatedSummary}
            savedPdfInfo={model.savedPdfInfo}
            isSaving={model.isSaving}
            onDownload={model.handleDownloadMarkdown}
            saveError={model.saveError}
            onClose={() => model.setShowCreateForm(false)}
          />
        )}

        {!model.showCreateForm && (
          <SummariesListFilters
            startDate={model.startDate}
            endDate={model.endDate}
            onStartDate={model.setStartDate}
            onEndDate={model.setEndDate}
            selectedContentTypes={model.selectedContentTypes}
            contentTypeFilterEnabled={model.contentTypeFilterEnabled}
            enabledDataTypes={model.enabledDataTypes}
            onToggleContentType={model.toggleContentType}
            onToggleContentTypeFilter={model.toggleContentTypeFilter}
            onSetContentTypes={model.setSelectedContentTypes}
            hasActiveFilters={model.hasActiveFilters}
            onClear={() => {
              model.clearFilters();
              model.setStartDate("");
              model.setEndDate("");
            }}
            onApply={model.handleDateFilterApply}
          />
        )}

        {model.isLoading ? (
          <SummariesTableSkeleton />
        ) : model.summaries.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
            <Search className="h-10 w-10 text-muted-foreground/50 mb-3" />
            <p className="text-foreground font-medium">No summaries found</p>
            <p className="text-sm mt-1 mb-3">
              {model.hasActiveFilters
                ? "Try adjusting your filters"
                : "Create your first summary using the button above"}
            </p>
            {!model.hasActiveFilters && model.summaries.length === 0 && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => model.setShowCreateForm(true)}
                className="h-8"
              >
                <Plus className="h-4 w-4 mr-1" />
                Get Started
              </Button>
            )}
          </div>
        ) : (
          <SummaryTable
            summaries={model.summaries}
            currentPage={model.pageDisplay}
            totalPages={model.pageDisplay + (model.serverHasNext ? 1 : 0)}
            totalItems={model.summaries.length}
            hasNextPage={model.serverHasNext}
            hasPreviousPage={model.serverHasPrev}
            onNextPage={model.goToNextPage}
            onPreviousPage={model.goToPreviousPage}
            onView={model.handleViewSummary}
            onTest={model.handleCreateTest}
            onDelete={model.handleDeleteConfirm}
            isDeletePending={model.deleteMutation.isPending}
            isLoading={false}
            cursorMode
          />
        )}
      </div>

      <SummaryDetailModal
        isOpen={model.isModalOpen}
        onClose={model.handleCloseModal}
        selectedSummary={model.selectedSummary}
        onDownloadMd={model.handleDownloadMd}
        onDownloadPdf={model.handleDownloadPdf}
      />

      <DownloadErrorDialog
        open={!!model.downloadError}
        onOpenChange={(open) => !open && model.clearError()}
        error={model.downloadError || ""}
        summary={model.lastFailedSummary}
        onRetry={async () => {
          if (model.retryLastDownload) {
            await model.retryLastDownload();
          }
        }}
        onDownloadMarkdown={async () => {
          if (model.lastFailedSummary) {
            await model.robustDownloadMarkdown(model.lastFailedSummary);
          }
        }}
        onClose={model.clearError}
        isRetrying={model.isDownloadingFile}
        canRetry={model.canRetry}
      />

      <ErrorRecoveryDialog
        open={!!model.generationError && !model.isGenerating}
        onOpenChange={(open) => !open && model.recoverFromGenerationError()}
        error={model.generationError || ""}
        canRetry={model.canRetryGeneration}
        onRetry={model.handleErrorRetry}
        onResetToIdle={model.handleResetToIdle}
        onClose={model.recoverFromGenerationError}
        isRetrying={model.isGenerating}
        operationType="summary generation"
        preservedConfig={model.preservedConfig}
      />

      <DeleteConfirmation
        open={!!model.deleteCandidate}
        onOpenChange={(open) => !open && model.setDeleteCandidate(null)}
        itemType="Summary"
        itemName={
          model.deleteCandidate
            ? extractSummaryContent(model.deleteCandidate).name ||
              "Untitled Summary"
            : undefined
        }
        onConfirm={model.handleDeleteConfirm}
        onCancel={model.handleDeleteCancel}
        isProcessing={model.deleteMutation.isPending}
        additionalConsequences={["Associated reports and data will be removed"]}
      />
    </div>
  );
}
