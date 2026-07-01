import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { CogniteClient } from "@cognite/sdk";
import { ThresholdChart } from "@/pages/AnnotationQuality/components/ThresholdChart";
import { FilePreviewCanvas } from "./components/FilePreviewCanvas";
import { ManualPromotion } from "@/pages/AnnotationQuality/components/ManualPromotion";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent } from "@/shared/components/ui/card";
import { CheckCircle2, Loader2, Sparkles, X, Info } from "lucide-react";
import type { PipelineConfig } from "@/shared/utils/types";
import { useFilterState } from "@/pages/AnnotationQuality/hooks/useFilterState";
import { useLoadingDuration } from "@/shared/hooks/useLoadingDuration";
import { usePagination } from "@/shared/hooks/usePagination";
import { useAnnotations } from "@/shared/hooks/useAnnotationData";
import { StatusBadge } from "@/shared/components/StatusBadge";
import { usePerFileMetrics } from "@/pages/AnnotationQuality/hooks/usePerFileMetrics";
import {
  buildFileInfoByExternalId,
  resolveFileInfo as resolveFileInfoByExternalId,
} from "@/shared/utils/fileInfoResolver";
import { AnnotationTagsCard } from "./components/AnnotationTagsCard";
import { PerFileAggregationCard } from "./components/PerFileAggregationCard";
import { PerFileFiltersCard } from "./components/PerFileFiltersCard";

const COVERAGE_RANGES = [
  { value: "all", label: "All Coverage" },
  { value: "0-25", label: "0% - 25%" },
  { value: "25-50", label: "25% - 50%" },
  { value: "50-75", label: "50% - 75%" },
  { value: "75-100", label: "75% - 100%" },
];

const SORT_OPTIONS = [
  { value: "none", label: "No Sorting" },
  { value: "name-asc", label: "Name (A-Z)" },
  { value: "name-desc", label: "Name (Z-A)" },
  { value: "coverage-asc", label: "Coverage (Low -> High)" },
  { value: "coverage-desc", label: "Coverage (High -> Low)" },
  { value: "actual-desc", label: "Actual (High -> Low)" },
  { value: "actual-asc", label: "Actual (Low -> High)" },
  { value: "potential-desc", label: "Potential (High -> Low)" },
  { value: "potential-asc", label: "Potential (Low -> High)" },
];

const PAGE_SIZE_OPTIONS = [25, 50, 100, 200];

interface PerFileTabProps {
  sdk: CogniteClient | null;
  config: PipelineConfig | null;
  pipelineId: string | null;
}

export function PerFileTab({
  sdk,
  config,
  pipelineId,
}: PerFileTabProps) {
  const [selectedFileId, setSelectedFileId] = useState<string | null>(null);
  const [selectionClearedManually, setSelectionClearedManually] = useState(false);
  const [scrollToSelectedToken, setScrollToSelectedToken] = useState(0);
  const [viewAllActual, setViewAllActual] = useState(false);
  const [viewAllPotential, setViewAllPotential] = useState(false);

  const { filters, setFilter } = useFilterState({
    resourceTypeFilter: "all",
    primaryScopeFilter: "all",
    secondaryScopeFilter: "all",
    searchQuery: "",
    tagSearchActual: "",
    tagSearchPotential: "",
    coverageRange: "all",
    sortOption: "coverage-desc",
  });

  // Canvas preview state
  const [previewFileId, setPreviewFileId] = useState<string | null>(null);
  const [previewFileName, setPreviewFileName] = useState<string | undefined>(undefined);
  const [previewFileSourceId, setPreviewFileSourceId] = useState<string | undefined>(undefined);
  const [previewFocusToken, setPreviewFocusToken] = useState(0);

  // Manual promotion state
  const [selectedPotentialTags, setSelectedPotentialTags] = useState<Set<string>>(new Set());
  const selectedFilesArray = useMemo(() => (selectedFileId ? [selectedFileId] : []), [selectedFileId]);
  const hasFileSelection = selectedFilesArray.length > 0;
  const queryClient = useQueryClient();
  const previousPipelineRef = useRef<string | null>(null);
  const [tableProgress, setTableProgress] = useState<
    Record<string, { rows: number; attempts: number; done: boolean }>
  >({});

  const updateTableProgress = useCallback((message: string) => {
    const startMatch = message.match(/^Step:\s(.+)\slistRows start$/);
    if (startMatch) {
      const tableName = startMatch[1].trim();
      setTableProgress((prev) => ({
        ...prev,
        [tableName]: { rows: prev[tableName]?.rows ?? 0, attempts: 1, done: false },
      }));
      return;
    }

    const readMatch = message.match(/^(.+)\sread:\s([\d,]+)\srows$/);
    if (readMatch) {
      const tableName = readMatch[1].trim();
      const rows = Number.parseInt(readMatch[2].replace(/,/g, ""), 10) || 0;
      setTableProgress((prev) => ({
        ...prev,
        [tableName]: { rows, attempts: prev[tableName]?.attempts ?? 1, done: true },
      }));
      return;
    }

    const rowsMatch = message.match(/^(.+):\s([\d,]+)\srows$/);
    if (rowsMatch) {
      const tableName = rowsMatch[1].trim();
      const rows = Number.parseInt(rowsMatch[2].replace(/,/g, ""), 10) || 0;
      setTableProgress((prev) => ({
        ...prev,
        [tableName]: { rows, attempts: prev[tableName]?.attempts ?? 1, done: false },
      }));
    }
  }, []);

  const annotationsQuery = useAnnotations(sdk, config, pipelineId, {
    enabled: Boolean(sdk && config && pipelineId),
    onProgress: updateTableProgress,
  });

  useEffect(() => {
    setTableProgress({});
  }, [pipelineId]);

  useEffect(() => {
    const previousPipeline = previousPipelineRef.current;
    if (previousPipeline && previousPipeline !== pipelineId) {
      queryClient.cancelQueries({
        predicate: (query) =>
          (query.queryKey[0] === "annotations" || query.queryKey[0] === "annotationsByFile") &&
          query.queryKey.includes(previousPipeline),
      });
      queryClient.removeQueries({
        predicate: (query) =>
          (query.queryKey[0] === "annotations" || query.queryKey[0] === "annotationsByFile") &&
          query.queryKey.includes(previousPipeline),
      });
    }
    previousPipelineRef.current = pipelineId;
  }, [pipelineId, queryClient]);

  useEffect(() => {
    return () => {
      queryClient.cancelQueries({
        predicate: (query) =>
          query.queryKey[0] === "annotations" || query.queryKey[0] === "annotationsByFile",
      });
      queryClient.removeQueries({
        predicate: (query) =>
          query.queryKey[0] === "annotations" || query.queryKey[0] === "annotationsByFile",
      });
    };
  }, [queryClient]);

  useEffect(() => {
    setSelectedFileId(null);
    setSelectionClearedManually(false);
    setSelectedPotentialTags(new Set());
    setPreviewFileId(null);
    setPreviewFileName(undefined);
    setPreviewFileSourceId(undefined);
  }, [pipelineId]);

  const progressTables = useMemo(() => {
    const names = [
      config?.rawTableAssetTags,
      config?.rawTableFileTags,
      config?.rawTablePatternTags,
    ].filter((value): value is string => Boolean(value));
    return names;
  }, [config?.rawTableAssetTags, config?.rawTableFileTags, config?.rawTablePatternTags]);

  const detailedActualRecords = annotationsQuery.data?.actual ?? [];
  const detailedPotentialRecords = annotationsQuery.data?.potential ?? [];
  const isAnnotationsLoading = annotationsQuery.isLoading || annotationsQuery.isFetching;
  const annotationsLoad = useLoadingDuration(
    isAnnotationsLoading,
    `${pipelineId || "none"}:perfile-raw`,
    { keepRunningWhile: isAnnotationsLoading }
  );

  const {
    allRecords,
    resourceTypeOptions,
    primaryScopeOptions,
    secondaryScopeOptions,
    fileAggregationsRaw,
    fileAggregations,
    filteredActual,
    filteredPotential,
    thresholdBuckets,
    actualTagEntries,
    potentialTagEntries,
    previewFileActualAnnotations,
    previewFilePotentialAnnotations,
    hasCanvasAnnotations,
    hasPreviewForFile,
  } = usePerFileMetrics({
    config,
    detailedActualRecords,
    detailedPotentialRecords,
    selectedFileId,
    previewFileId,
    filters,
  });

  const fileInfoByExternalId = useMemo(() => buildFileInfoByExternalId(allRecords), [allRecords]);

  const resolveFileInfo = useCallback(
    (fileExternalId: string) => {
      return resolveFileInfoByExternalId(fileInfoByExternalId, fileExternalId);
    },
    [fileInfoByExternalId]
  );

  useEffect(() => {
    if (fileAggregations.length === 0) {
      if (selectedFileId !== null) {
        setSelectedFileId(null);
      }
      setSelectionClearedManually(false);
      return;
    }

    if (selectionClearedManually && selectedFileId === null) {
      return;
    }

    const hasValidSelection =
      selectedFileId != null &&
      fileAggregations.some((file) => file.fileExternalId === selectedFileId);

    if (!hasValidSelection && selectedFileId !== null) {
      setSelectedFileId(null);
      setSelectionClearedManually(true);
    }
  }, [fileAggregations, selectedFileId, selectionClearedManually]);

  const paginationResetToken = `${filters.searchQuery}|${filters.coverageRange}|${filters.sortOption}|${filters.resourceTypeFilter}|${filters.primaryScopeFilter}|${filters.secondaryScopeFilter}`;

  const {
    currentPage,
    pageSize,
    totalPages,
    pagedItems: pagedFileAggregations,
    pageRangeLabel,
    setCurrentPage,
    setPageSize,
    goToPreviousPage,
    goToNextPage,
  } = usePagination({
    items: fileAggregations,
    initialPageSize: 50,
    resetToken: paginationResetToken,
  });

  const previewRef = useRef<HTMLDivElement | null>(null);
  const selectedRowIndex = useMemo(() => {
    if (!selectedFileId) return -1;
    return pagedFileAggregations.findIndex((file) => file.fileExternalId === selectedFileId);
  }, [pagedFileAggregations, selectedFileId]);

  const handleFileSelection = (fileId: string, checked: boolean) => {
    setSelectedFileId(checked ? fileId : null);
    setSelectionClearedManually(!checked);
  };

  const handleAggregationSortChange = useCallback(
    (field: "name" | "coverage" | "actual" | "potential") => {
      const current = filters.sortOption;
      if (!current.startsWith(`${field}-`)) {
        setFilter("sortOption", `${field}-asc`);
        return;
      }

      if (current.endsWith("-asc")) {
        setFilter("sortOption", `${field}-desc`);
        return;
      }

      if (current.endsWith("-desc")) {
        setFilter("sortOption", "none");
        return;
      }

      setFilter("sortOption", `${field}-asc`);
    },
    [filters.sortOption, setFilter]
  );

  const handlePreviewFile = (fileId: string, fileName?: string, fileSourceId?: string) => {
    if (!hasPreviewForFile(fileId)) return;
    setPreviewFileId(fileId);
    setPreviewFileName(fileName);
    setPreviewFileSourceId(fileSourceId);
    setPreviewFocusToken((token) => token + 1);
  };

  const handleClosePreview = () => {
    setPreviewFileId(null);
    setPreviewFileName(undefined);
    setPreviewFileSourceId(undefined);
  };

  useEffect(() => {
    if (!previewFileId) return;
    const target = previewRef.current;
    if (!target) return;
    const header = document.querySelector("header");
    const headerHeight = header instanceof HTMLElement
      ? Math.ceil(header.getBoundingClientRect().height)
      : 0;
    const scrollOffset = Math.max(12, headerHeight + 12);
    const previousMargin = target.style.scrollMarginTop;
    target.style.scrollMarginTop = `${scrollOffset}px`;
    target.scrollIntoView({ behavior: "smooth", block: "start" });
    return () => {
      target.style.scrollMarginTop = previousMargin;
    };
  }, [previewFileId, previewFocusToken]);

  const renderStatusBadge = (status?: string) => <StatusBadge status={status} />;

  if (isAnnotationsLoading || annotationsQuery.isError) {
    return (
      <Card>
        <CardContent className="py-12">
          <div className="flex flex-col items-center gap-3 text-muted-foreground">
            <Loader2 className="h-6 w-6 animate-spin" />
            <p className="text-sm">Loading annotations...</p>
            <p className="text-xs">Elapsed: {annotationsLoad.elapsedLabel}</p>
            {annotationsQuery.error && (
              <div className="text-xs text-center space-y-1">
                {annotationsQuery.isError && (
                  <div className="text-rose-500">Failed to load annotations.</div>
                )}
              </div>
            )}
            {progressTables.length > 0 && (
              <div className="w-full max-w-xl rounded-md border bg-background/70 p-3 text-left text-xs text-muted-foreground">
                <p className="font-medium text-foreground">Loading status</p>
                <ul className="mt-2 space-y-1">
                  {progressTables.map((tableName) => {
                    const progress = tableProgress[tableName];
                    const rows = progress?.rows ?? 0;
                    const attempts = progress?.attempts ?? 1;
                    const done = progress?.done ?? false;
                    return (
                      <li key={tableName} className="flex items-center justify-between">
                        <span className="flex items-center gap-2">
                          {done ? (
                            <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                          ) : (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          )}
                          <span>Loading {tableName}</span>
                        </span>
                        <span className="text-muted-foreground">
                          {rows.toLocaleString()} rows · {attempts} attempt{attempts === 1 ? "" : "s"}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-5">
      {annotationsLoad.lastDurationLabel && (
        <div className="flex justify-end">
          <div className="text-right">
            <p className="text-[11px] text-muted-foreground">
              Last load time
            </p>
            <p className="text-sm font-medium">
              {annotationsLoad.lastDurationLabel}
            </p>
          </div>
        </div>
      )}

      <PerFileFiltersCard
        config={config}
        searchQuery={filters.searchQuery}
        onSearchQueryChange={(value) => setFilter("searchQuery", value)}
        coverageRange={filters.coverageRange}
        onCoverageRangeChange={(value) => setFilter("coverageRange", value)}
        sortOption={filters.sortOption}
        onSortOptionChange={(value) => setFilter("sortOption", value)}
        resourceTypeFilter={filters.resourceTypeFilter}
        onResourceTypeFilterChange={(value) => setFilter("resourceTypeFilter", value)}
        primaryScopeFilter={filters.primaryScopeFilter}
        onPrimaryScopeFilterChange={(value) => setFilter("primaryScopeFilter", value)}
        secondaryScopeFilter={filters.secondaryScopeFilter}
        onSecondaryScopeFilterChange={(value) => setFilter("secondaryScopeFilter", value)}
        coverageOptions={COVERAGE_RANGES}
        sortOptions={SORT_OPTIONS}
        resourceTypeOptions={resourceTypeOptions}
        primaryScopeOptions={primaryScopeOptions}
        secondaryScopeOptions={secondaryScopeOptions}
      />

      {/* Threshold Chart */}
      <ThresholdChart title="Coverage Distribution" data={thresholdBuckets} />

      <PerFileAggregationCard
        config={config}
        fileAggregations={fileAggregations}
        fileAggregationsRawLength={fileAggregationsRaw.length}
        pagedFileAggregations={pagedFileAggregations}
        selectedFileId={selectedFileId}
        selectedRowIndex={selectedRowIndex}
        scrollToSelectedToken={scrollToSelectedToken}
        hasCanvasAnnotations={hasCanvasAnnotations}
        pageSize={pageSize}
        pageSizeOptions={PAGE_SIZE_OPTIONS}
        currentPage={currentPage}
        totalPages={totalPages}
        pageRangeLabel={pageRangeLabel}
        sortOption={filters.sortOption}
        onSortChange={handleAggregationSortChange}
        onPageSizeChange={setPageSize}
        onPreviousPage={goToPreviousPage}
        onNextPage={goToNextPage}
        onClearSelection={() => {
          setSelectedFileId(null);
          setSelectionClearedManually(true);
        }}
        onJumpToSelected={() => {
          if (!selectedFileId) return;
          const selectedIndex = fileAggregations.findIndex(
            (file) => file.fileExternalId === selectedFileId
          );
          if (selectedIndex < 0) return;
          const targetPage = Math.floor(selectedIndex / pageSize) + 1;
          setCurrentPage(targetPage);
          setScrollToSelectedToken((prev) => prev + 1);
        }}
        onFileSelection={handleFileSelection}
        hasPreviewForFile={hasPreviewForFile}
        onPreviewFile={handlePreviewFile}
      />

      {/* Annotation Comparison */}
      <div className="grid gap-5 lg:grid-cols-2">
        <AnnotationTagsCard
          title="Actual Annotations"
          icon={<CheckCircle2 className="h-4 w-4 text-emerald-500" />}
          badgeVariant="success"
          totalCount={filteredActual.length}
          description="Successfully matched and created annotations"
          searchValue={filters.tagSearchActual}
          onSearchChange={(value: string) => setFilter("tagSearchActual", value)}
          hasFileSelection={hasFileSelection || viewAllActual}
          isLoading={isAnnotationsLoading}
          loadingText="Loading annotations..."
          emptyText="No actual annotations"
          entries={viewAllActual ? actualTagEntries : hasFileSelection ? actualTagEntries : []}
          renderStatusBadge={renderStatusBadge}
          resolveFileInfo={resolveFileInfo}
          extraNoSelectionContent={
            !hasFileSelection && filteredActual.length > 0 ? (
              <div className="flex flex-col items-center gap-2 mt-2">
                <Button
                  size="xs"
                  variant="outline"
                  onClick={() => setViewAllActual(true)}
                >
                  View all annotations
                </Button>
                {viewAllActual && (
                  <div className="flex items-center gap-1 text-xs text-blue-600 mt-1">
                    <Info className="h-3 w-3" />
                    May be slower for very large datasets.
                  </div>
                )}
              </div>
            ) : null
          }
          viewAll={viewAllActual}
          setViewAll={setViewAllActual}
        />

        <AnnotationTagsCard
          title="Potential Annotations"
          icon={<Sparkles className="h-4 w-4 text-amber-500" />}
          badgeVariant="warning"
          totalCount={filteredPotential.length}
          description="Pattern-detected but not yet matched"
          searchValue={filters.tagSearchPotential}
          onSearchChange={(value: string) => setFilter("tagSearchPotential", value)}
          hasFileSelection={hasFileSelection || viewAllPotential}
          isLoading={isAnnotationsLoading}
          loadingText="Loading annotations..."
          emptyText="No potential annotations"
          entries={viewAllPotential ? potentialTagEntries : hasFileSelection ? potentialTagEntries : []}
          renderStatusBadge={renderStatusBadge}
          resolveFileInfo={resolveFileInfo}
          extraNoSelectionContent={
            !hasFileSelection && filteredPotential.length > 0 ? (
              <div className="flex flex-col items-center gap-2 mt-2">
                <Button
                  size="xs"
                  variant="outline"
                  onClick={() => setViewAllPotential(true)}
                >
                  View all annotations
                </Button>
                {viewAllPotential && (
                  <div className="flex items-center gap-1 text-xs text-blue-600 mt-1">
                    <Info className="h-3 w-3" />
                    May be slower for very large datasets.
                  </div>
                )}
              </div>
            ) : null
          }
          viewAll={viewAllPotential}
          setViewAll={setViewAllPotential}
        />
      </div>

      {/* File Preview Canvas */}
      {previewFileId && sdk && (
        <div className="relative" ref={previewRef}>
          <Button
            variant="outline"
            size="sm"
            className="absolute top-3 right-3 z-10 h-7 gap-1.5"
            onClick={handleClosePreview}
          >
            <X className="h-3.5 w-3.5" />
            Close Preview
          </Button>
          <FilePreviewCanvas
            sdk={sdk}
            fileExternalId={previewFileId}
            fileSourceId={previewFileSourceId}
            fileName={previewFileName}
            actualAnnotations={previewFileActualAnnotations}
            potentialAnnotations={previewFilePotentialAnnotations}
            selectedFileId={selectedFileId}
            setSelectedFileId={setSelectedFileId}
          />
        </div>
      )}

      {/* Manual Promotion */}
      {config && (
        <ManualPromotion
          sdk={sdk}
          config={config}
          potentialAnnotations={filteredPotential}
          selectedPotentialTags={selectedPotentialTags}
          onTagSelectionChange={setSelectedPotentialTags}
        />
      )}
    </div>
  );
}
