import { useCallback, useMemo } from "react";
import {
  type SummaryServerListFilters,
  useDeleteSummary,
  useSaveSummary,
  useSummaryListPage,
} from "@/hooks/useCdfData";
import { useSummaryFilters } from "@/hooks/useSummaryFilters";
import type { SummaryInstance } from "@/types/operations";

/** ViewModel/UI surface for delete summary mutation (tests can satisfy this shape). */
export interface SummariesListDeleteMutationRef {
  mutateAsync: (summary: SummaryInstance) => Promise<boolean>;
  isPending: boolean;
}

/** ViewModel/UI surface for save summary mutation (tests can satisfy this shape). */
export interface SummariesListSaveMutationRef {
  mutateAsync: (args: {
    content: string;
    startDate: Date;
    endDate: Date;
    contentTypes: string[];
  }) => Promise<{ fileName: string; externalId: string }>;
  isPending: boolean;
}

function useDefaultSummariesListPipelineImpl() {
  const {
    dateFilterEnabled,
    dateRange,
    contentTypeFilterEnabled,
    selectedContentTypes,
    setDateRange,
    toggleContentType,
    setSelectedContentTypes,
    clearFilters: clearFilterState,
    hasActiveFilters,
    toggleDateFilter,
    toggleContentTypeFilter,
  } = useSummaryFilters();

  const serverFilters: SummaryServerListFilters = useMemo(
    () => ({
      dateFilterEnabled,
      dateRange,
      contentTypeFilterEnabled,
      selectedContentTypes,
    }),
    [
      dateFilterEnabled,
      dateRange,
      contentTypeFilterEnabled,
      selectedContentTypes,
    ],
  );

  const {
    items,
    isLoading,
    isError,
    refetch,
    pageDisplay,
    hasNextPage,
    hasPreviousPage,
    goToNextPage,
    goToPreviousPage,
    resetPagination,
  } = useSummaryListPage({
    variant: "operation",
    serverFilters,
  });

  const deleteMutation = useDeleteSummary();
  const saveMutation = useSaveSummary();

  const clearFilters = useCallback(() => {
    clearFilterState();
    resetPagination();
  }, [clearFilterState, resetPagination]);

  return {
    dateFilterEnabled,
    dateRange,
    contentTypeFilterEnabled,
    selectedContentTypes,
    setDateRange,
    toggleContentType,
    setSelectedContentTypes,
    hasActiveFilters,
    toggleDateFilter,
    toggleContentTypeFilter,
    items,
    isLoading,
    isError,
    refetch,
    pageDisplay,
    hasNextPage,
    hasPreviousPage,
    goToNextPage,
    goToPreviousPage,
    deleteMutation,
    saveMutation,
    clearFilters,
  };
}

type SummariesListPipelineBase = ReturnType<
  typeof useDefaultSummariesListPipelineImpl
>;

export type SummariesListPipelineValue = Omit<
  SummariesListPipelineBase,
  "deleteMutation" | "saveMutation"
> & {
  deleteMutation: SummariesListDeleteMutationRef;
  saveMutation: SummariesListSaveMutationRef;
};

/**
 * Default summaries list + filters + mutations wiring for {@link SummariesPageModelDepsContext}.
 */
export function useDefaultSummariesListPipeline(): SummariesListPipelineValue {
  return useDefaultSummariesListPipelineImpl();
}
