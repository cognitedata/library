import { Card, CardContent } from "@/shared/components/ui/card";
import { useAppSdk } from "@/providers/AppSdkProvider";
import { useAutomaticPatterns, useManualPatterns } from "@/shared/hooks/useAnnotationData";
import { usePipelineConfig } from "@/shared/hooks/usePipelineConfig";
import { useLoadingDuration } from "@/shared/hooks/useLoadingDuration";
import { ManualPatternsSection } from "@/pages/PatternManagement/components/ManualPatternsSection";
import { ImportCsvSection } from "@/pages/PatternManagement/components/ImportCsvSection";
import { ProposePrimaryScopeSection } from "@/pages/PatternManagement/components/ProposePrimaryScopeSection";
import { AutomaticPatternsSection } from "@/pages/PatternManagement/components/AutomaticPatternsSection";
import { RefreshCacheSection } from "@/pages/PatternManagement/components/RefreshCacheSection";
import { PatternManagementHeader } from "@/pages/PatternManagement/components/PatternManagementHeader";
import { PatternManagementLoading } from "@/pages/PatternManagement/components/PatternManagementLoading";
import { PatternManagementMissingConfigWarning } from "@/pages/PatternManagement/components/PatternManagementMissingConfigWarning";
import { useAutomaticPatternsState } from "@/pages/PatternManagement/hooks/useAutomaticPatternsState.tsx";
import { useCsvImportState } from "@/pages/PatternManagement/hooks/useCsvImportState";
import { useManualPatternsState } from "@/pages/PatternManagement/hooks/useManualPatternsState.tsx";
import { useProposePreviewState } from "@/pages/PatternManagement/hooks/useProposePreviewState";
import { useRefreshCacheState } from "@/pages/PatternManagement/hooks/useRefreshCacheState";
import { PAGE_SIZE_OPTIONS } from "@/pages/PatternManagement/types";




interface PatternManagementPageProps {
  pipelineId: string | null;
}

export function PatternManagementPage({ pipelineId }: PatternManagementPageProps) {
  const { sdk } = useAppSdk();
  const {
    data: config,
    isLoading: isConfigLoading,
    isFetching: isConfigFetching,
    failureCount: configFailureCount,
  } = usePipelineConfig(sdk, pipelineId);
  const {
    data: manualPatterns,
    isLoading: isLoadingManual,
    isFetching: isFetchingManual,
    failureCount: manualFailureCount,
  } = useManualPatterns(
    sdk,
    config ?? null,
    pipelineId
  );
  const {
    data: automaticPatterns,
    isLoading: isLoadingAuto,
    isFetching: isFetchingAuto,
    failureCount: autoFailureCount,
  } = useAutomaticPatterns(
    sdk,
    config ?? null,
    pipelineId
  );

  const isConfigBlocking = (isConfigLoading || isConfigFetching) && !config;
  const isManualBlocking = (isLoadingManual || isFetchingManual) && !manualPatterns;
  const isAutoBlocking = (isLoadingAuto || isFetchingAuto) && !automaticPatterns;

  const manualPatternsData = manualPatterns ?? [];
  const automaticPatternsData = automaticPatterns ?? [];

  const manualState = useManualPatternsState({
    sdk,
    config: config ?? null,
    pipelineId,
    manualPatternsData,
    automaticPatternsData,
  });
  const proposeState = useProposePreviewState({
    automaticPatternsData,
    editablePatterns: manualState.editablePatterns,
    setEditablePatterns: manualState.setEditablePatterns,
    setHasChanges: manualState.setHasChanges,
    setSaveMessage: manualState.setSaveMessage,
    setLastManualUpdateInfo: manualState.setLastManualUpdateInfo,
  });
  const csvState = useCsvImportState({
    defaultScope: proposeState.primaryScopeInput,
    editablePatterns: manualState.editablePatterns,
    setEditablePatterns: manualState.setEditablePatterns,
    setHasChanges: manualState.setHasChanges,
    setSaveMessage: manualState.setSaveMessage,
    setLastManualUpdateInfo: manualState.setLastManualUpdateInfo,
  });
  const autoState = useAutomaticPatternsState({ automaticPatternsData });
  const refreshState = useRefreshCacheState({
    sdk,
    config: config ?? null,
    pipelineId,
    manualPatternsData,
    automaticPatternsData,
  });

  if (!pipelineId) {
    return (
      <Card className="border-dashed">
        <CardContent className="py-12">
          <div className="text-center text-muted-foreground">
            <p className="text-sm">Please select a pipeline to manage patterns.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  const isLoading = isConfigBlocking || isManualBlocking || isAutoBlocking;
  const loadDuration = useLoadingDuration(isLoading, pipelineId || "none", {
    keepRunningWhile: !config || !manualPatterns || !automaticPatterns,
  });
  const retryCount = Math.max(configFailureCount, manualFailureCount, autoFailureCount);

  if (isLoading) {
    return (
      <PatternManagementLoading
        elapsedLabel={loadDuration.elapsedLabel}
        retryCount={retryCount}
      />
    );
  }

  const showMissingConfigWarning = !config?.rawDb || (!config?.rawManualPatternsCatalog && !config?.rawTablePatternCache);

  return (
    <div className="space-y-6 animate-fade-in">
      <PatternManagementHeader lastDurationLabel={loadDuration.lastDurationLabel || null} />

      {showMissingConfigWarning && <PatternManagementMissingConfigWarning />}

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="space-y-6">
          <ManualPatternsSection
            isLoadingManual={isLoadingManual}
            hasChanges={manualState.hasChanges}
            editablePatterns={manualState.editablePatterns}
            lastManualUpdateInfo={manualState.lastManualUpdateInfo}
            manualSearchTerm={manualState.manualSearchTerm}
            setManualSearchTerm={(value) => manualState.setManualSearchTerm(value)}
            manualEntityFilter={manualState.manualEntityFilter}
            setManualEntityFilter={(value) => manualState.setManualEntityFilter(value)}
            manualScopeFilter={manualState.manualScopeFilter}
            setManualScopeFilter={(value) => manualState.setManualScopeFilter(value)}
            manualResourceTypeFilter={manualState.manualResourceTypeFilter}
            setManualResourceTypeFilter={(value) => manualState.setManualResourceTypeFilter(value)}
            manualEntityOptions={manualState.manualEntityOptions}
            manualScopeOptions={manualState.manualScopeOptions}
            manualResourceTypeOptions={manualState.manualResourceTypeOptions}
            manualTableRef={manualState.manualTableRef}
            manualRowRef={manualState.manualRowRef}
            filteredEditable={manualState.filteredEditable}
            manualTopSpacer={manualState.manualTopSpacer}
            manualBottomSpacer={manualState.manualBottomSpacer}
            manualVirtualRows={manualState.manualVirtualRows}
            pagedManualPatterns={manualState.pagedManualPatterns}
            handleUpdatePattern={manualState.handleUpdatePattern}
            handleDeletePattern={manualState.handleDeletePattern}
            selectedManualIds={manualState.selectedManualIds}
            handleToggleManualSelection={manualState.handleToggleManualSelection}
            handleSelectAllManual={manualState.handleSelectAllManual}
            handleBulkDeleteManual={manualState.handleBulkDeleteManual}
            toggleSort={manualState.toggleSort}
            renderSortIcon={manualState.renderSortIcon}
            manualSort={manualState.manualSort}
            setManualSort={manualState.setManualSort}
            manualFiltersActive={manualState.manualFiltersActive}
            manualRangeLabel={manualState.manualRangeLabel}
            manualCurrentPage={manualState.manualCurrentPage}
            manualTotalPages={manualState.manualTotalPages}
            setManualCurrentPage={manualState.setManualCurrentPage}
            manualPageSize={manualState.manualPageSize}
            setManualPageSize={(value) => manualState.setManualPageSize(value)}
            pageSizeOptions={PAGE_SIZE_OPTIONS}
            saveMessage={manualState.saveMessage}
            saveStatus={manualState.saveStatus}
            saveProgress={manualState.saveProgress}
            saveLogs={manualState.saveLogs}
            handleAddPattern={manualState.handleAddPattern}
            handleReset={manualState.handleReset}
            handleSave={manualState.handleSave}
            isSaving={manualState.isSaving}
            canEditManualPatterns={manualState.canEditManualPatterns}
          />

          <ImportCsvSection
            csvPreview={csvState.csvPreview}
            csvFileName={csvState.csvFileName}
            csvDefaultScope={csvState.csvDefaultScope}
            csvText={csvState.csvText}
            csvError={csvState.csvError}
            csvStageMessage={csvState.csvStageMessage}
            isCsvStaging={csvState.isCsvStaging}
            handleCsvFileChange={csvState.handleCsvFileChange}
            setCsvDefaultScope={(value) => csvState.setCsvDefaultScope(value)}
            handleCsvReparse={csvState.handleCsvReparse}
            handleCsvUpdate={csvState.handleCsvUpdate}
            handleCsvRemove={csvState.handleCsvRemove}
            handleCsvStage={csvState.handleCsvStage}
            handleCsvClear={csvState.handleCsvClear}
          />

          <ProposePrimaryScopeSection
            primaryScopeInput={proposeState.primaryScopeInput}
            setPrimaryScopeInput={(value) => proposeState.setPrimaryScopeInput(value)}
            proposeAnnotationType={proposeState.proposeAnnotationType}
            setProposeAnnotationType={(value) => proposeState.setProposeAnnotationType(value)}
            proposeResourceTypes={proposeState.proposeResourceTypes}
            setProposeResourceTypes={(value) => proposeState.setProposeResourceTypes(value)}
            proposeResourceTypeOptions={proposeState.proposeResourceTypeOptions}
            proposeMaxNew={proposeState.proposeMaxNew}
            applyProposeMaxAllowed={proposeState.applyProposeMaxAllowed}
            proposedPatterns={proposeState.proposedPatterns}
            isProposePreviewing={proposeState.isProposePreviewing}
            isProposeStaging={proposeState.isProposeStaging}
            proposePreviewProgress={proposeState.proposePreviewProgress}
            proposePreviewStatus={proposeState.proposePreviewStatus}
            proposePreviewLogs={proposeState.proposePreviewLogs}
            proposeStageMessage={proposeState.proposeStageMessage}
            proposePreviewInfo={proposeState.proposePreviewInfo}
            handlePreviewProposals={proposeState.handlePreviewProposals}
            handleStageProposals={proposeState.handleStageProposals}
            handleClearProposals={proposeState.handleClearProposals}
            handleProposedUpdate={proposeState.handleProposedUpdate}
          />
        </div>

        <div className="space-y-6">
          <AutomaticPatternsSection
            automaticPatternsCount={autoState.automaticPatternsCount}
            lastCacheWriteInfo={refreshState.lastCacheWriteInfo}
            autoSearchTerm={autoState.autoSearchTerm}
            setAutoSearchTerm={(value) => autoState.setAutoSearchTerm(value)}
            autoEntityFilter={autoState.autoEntityFilter}
            setAutoEntityFilter={(value) => autoState.setAutoEntityFilter(value)}
            autoScopeFilter={autoState.autoScopeFilter}
            setAutoScopeFilter={(value) => autoState.setAutoScopeFilter(value)}
            autoResourceTypeFilter={autoState.autoResourceTypeFilter}
            setAutoResourceTypeFilter={(value) => autoState.setAutoResourceTypeFilter(value)}
            autoEntityOptions={autoState.autoEntityOptions}
            autoScopeOptions={autoState.autoScopeOptions}
            autoResourceTypeOptions={autoState.autoResourceTypeOptions}
            isLoadingAuto={isLoadingAuto}
            filteredAutoCount={autoState.filteredAutoCount}
            autoTableRef={autoState.autoTableRef}
            autoRowRef={autoState.autoRowRef}
            autoTopSpacer={autoState.autoTopSpacer}
            autoBottomSpacer={autoState.autoBottomSpacer}
            autoVirtualRows={autoState.autoVirtualRows}
            pagedAutoPatterns={autoState.pagedAutoPatterns}
            toggleSort={autoState.toggleSort}
            renderSortIcon={autoState.renderSortIcon}
            autoSort={autoState.autoSort}
            setAutoSort={autoState.setAutoSort}
            autoFiltersActive={autoState.autoFiltersActive}
            autoRangeLabel={autoState.autoRangeLabel}
            autoCurrentPage={autoState.autoCurrentPage}
            autoTotalPages={autoState.autoTotalPages}
            setAutoCurrentPage={autoState.setAutoCurrentPage}
            autoPageSize={autoState.autoPageSize}
            setAutoPageSize={(value) => autoState.setAutoPageSize(value)}
            pageSizeOptions={PAGE_SIZE_OPTIONS}
          />

          <RefreshCacheSection
            finalPreview={refreshState.finalPreview}
            finalRowsCount={refreshState.finalRows.length}
            handleDiscoverScopes={refreshState.handleDiscoverScopes}
            handleGeneratePreview={refreshState.handleGeneratePreview}
            handleWriteCacheRows={refreshState.handleWriteCacheRows}
            handleClearCachePreview={refreshState.handleClearCachePreview}
            isRefreshingScopes={refreshState.isRefreshingScopes}
            isGeneratingPreview={refreshState.isGeneratingPreview}
            isWritingCache={refreshState.isWritingCache}
            refreshMessage={refreshState.refreshMessage}
            cacheStatus={refreshState.cacheStatus}
            cacheProgress={refreshState.cacheProgress}
            cacheProgressKnown={refreshState.cacheProgressKnown}
            cacheLogs={refreshState.cacheLogs}
            scopePreview={refreshState.scopePreview}
            isLocalMockMode={refreshState.isLocalMockMode}
          />
        </div>
      </div>
    </div>
  );
}






