import { useCallback, useMemo } from "react";
import { DataProcessor } from "@/shared/utils/dataProcessor";
import type { AnnotationRecord, PipelineConfig } from "@/shared/utils/types";

type SortField = "name" | "coverage" | "actual" | "potential";
type SortDirection = "asc" | "desc";

export interface PerFileFilters {
  resourceTypeFilter: string;
  primaryScopeFilter: string;
  secondaryScopeFilter: string;
  searchQuery: string;
  tagSearchActual: string;
  tagSearchPotential: string;
  coverageRange: string;
  sortOption: string;
}

interface UsePerFileMetricsParams {
  config: PipelineConfig | null;
  detailedActualRecords: AnnotationRecord[];
  detailedPotentialRecords: AnnotationRecord[];
  selectedFileId: string | null;
  previewFileId: string | null;
  filters: PerFileFilters;
}

export interface UsePerFileMetricsResult {
  allRecords: AnnotationRecord[];
  resourceTypeOptions: Array<{ value: string; label: string }>;
  primaryScopeOptions: Array<{ value: string; label: string }>;
  secondaryScopeOptions: Array<{ value: string; label: string }>;
  fileAggregationsRaw: ReturnType<typeof DataProcessor.aggregateByFile>;
  fileAggregations: ReturnType<typeof DataProcessor.aggregateByFile>;
  fileAggregationsFiltered: ReturnType<typeof DataProcessor.aggregateByFile>;
  filteredActual: AnnotationRecord[];
  filteredPotential: AnnotationRecord[];
  actualByTag: ReturnType<typeof DataProcessor.groupByTag>;
  potentialByTag: ReturnType<typeof DataProcessor.groupByTag>;
  actualTagEntries: Array<[string, { count: number; files: Set<string>; resourceType?: string; secondaryScope?: string; normalizedStatus?: string }]>;
  potentialTagEntries: Array<[string, { count: number; files: Set<string>; resourceType?: string; secondaryScope?: string; normalizedStatus?: string }]>;
  currentFileActual: AnnotationRecord[];
  currentFilePotential: AnnotationRecord[];
  previewFileActualAnnotations: AnnotationRecord[];
  previewFilePotentialAnnotations: AnnotationRecord[];
  hasCanvasAnnotations: boolean;
  hasPreviewForFile: (fileId: string) => boolean;
  thresholdBuckets: ReturnType<typeof DataProcessor.calculateThresholdBuckets>;
  filterByFileProperties: (records: AnnotationRecord[]) => AnnotationRecord[];
}

export function usePerFileMetrics({
  config,
  detailedActualRecords,
  detailedPotentialRecords,
  selectedFileId,
  previewFileId,
  filters,
}: UsePerFileMetricsParams): UsePerFileMetricsResult {
  const allRecords = useMemo(
    () => [...detailedActualRecords, ...detailedPotentialRecords],
    [detailedActualRecords, detailedPotentialRecords]
  );

  const resourceTypeOptions = useMemo(() => {
    const values = DataProcessor.getUniqueValues(allRecords, "fileResourceType");
    return [{ value: "all", label: "All" }, ...values.map((v) => ({ value: v, label: v }))];
  }, [allRecords]);

  const primaryScopeOptions = useMemo(() => {
    if (!config?.primaryScopeProperty) return [{ value: "all", label: "All" }];
    const values = DataProcessor.getUniqueValues(allRecords, "filePrimaryScope");
    return [{ value: "all", label: "All" }, ...values.map((v) => ({ value: v, label: v }))];
  }, [allRecords, config?.primaryScopeProperty]);

  const secondaryScopeOptions = useMemo(() => {
    if (!config?.secondaryScopeProperty) return [{ value: "all", label: "All" }];
    const values = DataProcessor.getUniqueValues(allRecords, "fileSecondaryScope");
    return [{ value: "all", label: "All" }, ...values.map((v) => ({ value: v, label: v }))];
  }, [allRecords, config?.secondaryScopeProperty]);

  const filterByFileProperties = useCallback(
    (records: AnnotationRecord[]) => {
      return records.filter((record) => {
        if (
          filters.resourceTypeFilter !== "all" &&
          record.fileResourceType !== filters.resourceTypeFilter
        ) {
          return false;
        }
        if (
          filters.primaryScopeFilter !== "all" &&
          record.filePrimaryScope !== filters.primaryScopeFilter
        ) {
          return false;
        }
        if (
          filters.secondaryScopeFilter !== "all" &&
          record.fileSecondaryScope !== filters.secondaryScopeFilter
        ) {
          return false;
        }
        return true;
      });
    },
    [filters.resourceTypeFilter, filters.primaryScopeFilter, filters.secondaryScopeFilter]
  );

  const filteredActualForAggregation = useMemo(
    () => filterByFileProperties(detailedActualRecords),
    [detailedActualRecords, filterByFileProperties]
  );

  const filteredPotentialForAggregation = useMemo(
    () => filterByFileProperties(detailedPotentialRecords),
    [detailedPotentialRecords, filterByFileProperties]
  );

  const fileAggregationsRaw = useMemo(() => {
    return DataProcessor.aggregateByFile(
      filteredActualForAggregation,
      filteredPotentialForAggregation
    );
  }, [filteredActualForAggregation, filteredPotentialForAggregation]);

  const fileAggregationsFiltered = useMemo(() => {
    const result = [...fileAggregationsRaw];

    if (filters.searchQuery.trim()) {
      const query = filters.searchQuery.toLowerCase().trim();
      const matchesQuery = (value: string | undefined) => value?.toLowerCase().includes(query);
      const containsQuery = (value: string) => value.toLowerCase().includes(query);

      const filtered = result.filter(
        (file) => matchesQuery(file.fileName) || containsQuery(file.fileExternalId)
      );
      result.splice(0, result.length, ...filtered);
    }

    if (filters.coverageRange !== "all") {
      const [minStr, maxStr] = filters.coverageRange.split("-");
      const min = parseInt(minStr, 10);
      const max = parseInt(maxStr, 10);
      const filtered = result.filter((file) => file.coveragePct >= min && file.coveragePct < max);
      result.splice(0, result.length, ...filtered);
    }

    return result;
  }, [fileAggregationsRaw, filters.searchQuery, filters.coverageRange]);

  const fileAggregations = useMemo(() => {
    if (filters.sortOption === "none") {
      return [...fileAggregationsFiltered];
    }

    const result = [...fileAggregationsFiltered];

    const [field, direction] = filters.sortOption.split("-") as [SortField, SortDirection];
    if (!field || !direction) {
      return result;
    }

    result.sort((a, b) => {
      let comparison = 0;
      switch (field) {
        case "name":
          comparison = (a.fileName || a.fileExternalId).localeCompare(b.fileName || b.fileExternalId);
          break;
        case "coverage":
          comparison = a.coveragePct - b.coveragePct;
          break;
        case "actual":
          comparison = a.actualCount - b.actualCount;
          break;
        case "potential":
          comparison = a.potentialCount - b.potentialCount;
          break;
      }
      return direction === "desc" ? -comparison : comparison;
    });

    return result;
  }, [fileAggregationsFiltered, filters.sortOption]);

  const selectedFilesArray = useMemo(
    () => (selectedFileId ? [selectedFileId] : []),
    [selectedFileId]
  );

  const filteredActual = useMemo(() => {
    const filtered = filterByFileProperties(detailedActualRecords);
    if (selectedFilesArray.length === 0) return filtered;
    return DataProcessor.filterRecords(filtered, { fileExternalIds: selectedFilesArray });
  }, [detailedActualRecords, filterByFileProperties, selectedFilesArray]);

  const filteredPotential = useMemo(() => {
    const filtered = filterByFileProperties(detailedPotentialRecords);
    if (selectedFilesArray.length === 0) return filtered;
    return DataProcessor.filterRecords(filtered, { fileExternalIds: selectedFilesArray });
  }, [detailedPotentialRecords, filterByFileProperties, selectedFilesArray]);

  const thresholdBuckets = useMemo(() => {
    // Coverage distribution is intentionally based on all globally filtered files
    // (search + coverage range + file property filters), independent from selection and pagination.
    return DataProcessor.calculateThresholdBuckets(fileAggregationsFiltered);
  }, [fileAggregationsFiltered]);

  const actualByTag = useMemo(() => DataProcessor.groupByTag(filteredActual), [filteredActual]);
  const potentialByTag = useMemo(() => DataProcessor.groupByTag(filteredPotential), [filteredPotential]);

  const actualTagEntries = useMemo(() => {
    const entries = Array.from(actualByTag.entries());
    if (!filters.tagSearchActual.trim()) return entries;
    const q = filters.tagSearchActual.toLowerCase().trim();
    return entries.filter(([tag, data]) => {
      return tag.toLowerCase().includes(q) || (data.resourceType || "").toLowerCase().includes(q);
    });
  }, [actualByTag, filters.tagSearchActual]);

  const potentialTagEntries = useMemo(() => {
    const entries = Array.from(potentialByTag.entries());
    if (!filters.tagSearchPotential.trim()) return entries;
    const q = filters.tagSearchPotential.toLowerCase().trim();
    return entries.filter(([tag, data]) => {
      return tag.toLowerCase().includes(q) || (data.resourceType || "").toLowerCase().includes(q);
    });
  }, [potentialByTag, filters.tagSearchPotential]);

  const currentFileActual = useMemo(() => {
    if (!selectedFileId) return [];
    return detailedActualRecords.filter((record) => record.fileExternalId === selectedFileId);
  }, [detailedActualRecords, selectedFileId]);

  const currentFilePotential = useMemo(() => {
    if (!selectedFileId) return [];
    return detailedPotentialRecords.filter((record) => record.fileExternalId === selectedFileId);
  }, [detailedPotentialRecords, selectedFileId]);

  const previewActual = useMemo(() => {
    if (!previewFileId) return [];
    return detailedActualRecords.filter((record) => record.fileExternalId === previewFileId);
  }, [detailedActualRecords, previewFileId]);

  const previewPotential = useMemo(() => {
    if (!previewFileId) return [];
    return detailedPotentialRecords.filter((record) => record.fileExternalId === previewFileId);
  }, [detailedPotentialRecords, previewFileId]);

  const hasCanvasAnnotations = useMemo(() => {
    return allRecords.some((record) => Boolean(record.boundingBox));
  }, [allRecords]);

  const hasPreviewForFile = useCallback(
    (fileId: string) => {
      return allRecords.some(
        (record) => record.fileExternalId === fileId && Boolean(record.boundingBox)
      );
    },
    [allRecords]
  );

  return {
    allRecords,
    resourceTypeOptions,
    primaryScopeOptions,
    secondaryScopeOptions,
    fileAggregationsRaw,
    fileAggregations,
    fileAggregationsFiltered,
    filteredActual,
    filteredPotential,
    actualByTag,
    potentialByTag,
    actualTagEntries,
    potentialTagEntries,
    currentFileActual,
    currentFilePotential,
    previewFileActualAnnotations: previewActual,
    previewFilePotentialAnnotations: previewPotential,
    hasCanvasAnnotations,
    hasPreviewForFile,
    thresholdBuckets,
    filterByFileProperties,
  };
}
