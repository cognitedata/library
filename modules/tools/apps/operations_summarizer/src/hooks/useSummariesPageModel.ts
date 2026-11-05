import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { getEnabledDataTypes, getSummarySpace } from "@/config/dataTypes";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { useNavigation } from "@/contexts/NavigationContext";
import { useOperations } from "@/contexts/OperationsSummarizerContext";
import { useSummariesPageModelDeps } from "@/contexts/SummariesPageModelDepsContext";
import { useEnhancedRefresh } from "@/hooks/useEnhancedRefresh";
import type { SummaryInstance } from "@/types/operations";
import { resolveFusionFileUrl } from "@/utils/fusionFileUrl";
import {
  createSummariesCreationHandlers,
  derivePreservedConfig,
  formatDateForInput,
  useSummariesCreationComputed,
} from "./summariesPageModel/creationForm";
import {
  createSummariesErrorRecoveryHandlers,
  createSummariesTableHandlers,
} from "./summariesPageModel/tableActions";

type UseSummariesPageClientDeps = {
  useCdfClient: typeof useCdfClient;
};

const defaultSummariesPageClientDeps: UseSummariesPageClientDeps = {
  useCdfClient,
};

export const UseSummariesPageClientContext =
  createContext<UseSummariesPageClientDeps>(defaultSummariesPageClientDeps);

/**
 * Summaries page ViewModel: list filters, generation, downloads, modals.
 */
export function useSummariesPageModel() {
  const deps = useSummariesPageModelDeps();
  const { downloadOperationSummaryPdfWithSignedUrlFirst } = deps;
  const { useCdfClient: useClient } = useContext(UseSummariesPageClientContext);
  const { sdk } = useClient();
  const { navigate } = useNavigation();
  const { state, dispatch, toggleSummaryContent } = useOperations();
  const {
    generate,
    cancel: cancelGeneration,
    progress,
    isGenerating,
    elapsedSeconds,
    error: generationError,
    recoverFromError: recoverFromGenerationError,
    retryGeneration,
    canRetry: canRetryGeneration,
  } = deps.useSummaryGeneration();
  const {
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
    items: summaries,
    isLoading,
    isError: isFetchError,
    refetch,
    pageDisplay,
    hasNextPage: serverHasNext,
    hasPreviousPage: serverHasPrev,
    goToNextPage,
    goToPreviousPage,
    deleteMutation,
    saveMutation,
    clearFilters,
  } = deps.useSummariesListPipeline();

  const [showCreateForm, setShowCreateForm] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [selectedSummary, setSelectedSummary] =
    useState<SummaryInstance | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deleteCandidate, setDeleteCandidate] =
    useState<SummaryInstance | null>(null);
  const [savedPdfInfo, setSavedPdfInfo] = useState<{
    fileName: string;
    fileId: string | null;
    fusionUrl?: string;
  } | null>(null);
  const [isSaving] = useState(false);
  const [saveError] = useState<string | null>(null);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  const enabledDataTypes = useMemo(() => getEnabledDataTypes(), []);
  const { selectedCount, daysSelected } = useSummariesCreationComputed(state);

  const {
    refresh: enhancedRefresh,
    isRefreshing: isRefreshingData,
    showSuccess: refreshSuccess,
    showError: refreshError,
    message: refreshMessage,
  } = useEnhancedRefresh({
    onRefresh: async () => {
      await refetch();
    },
    successMessage: "Summaries refreshed",
    errorMessage: "Failed to refresh summaries",
  });

  const {
    downloadPdf: robustDownloadPdf,
    downloadMarkdown: robustDownloadMarkdown,
    isDownloading: isDownloadingFile,
    downloadError,
    lastFailedSummary,
    retryLastDownload,
    clearError,
    canRetry,
  } = deps.useSummariesRobustDownload();

  useEffect(() => {
    dispatch({ type: "SET_IS_GENERATING", payload: isGenerating });
  }, [isGenerating, dispatch]);

  const creationHandlers = createSummariesCreationHandlers({
    state,
    dispatch,
    generate,
    downloadAsMarkdown: deps.downloadAsMarkdown,
    setSavedPdfInfo,
    resolveFusionFileUrl: async (externalId) => {
      if (!sdk) return undefined;
      return resolveFusionFileUrl(sdk, {
        space: getSummarySpace(),
        externalId,
        searchQuery: `Summary period: ${formatDateForInput(state.summaryPeriodStart)} to ${formatDateForInput(state.summaryPeriodEnd)}`,
      });
    },
  });

  const tableHandlers = createSummariesTableHandlers({
    sdk,
    deleteMutation,
    robustDownloadPdf,
    robustDownloadMarkdown,
    downloadOperationSummaryPdfWithSignedUrlFirst,
    navigate,
    selectedSummary,
    deleteCandidate,
    setDeleteError,
    setDeleteCandidate,
    setSelectedSummary,
    setIsModalOpen,
  });

  const { handleErrorRetry, handleResetToIdle } =
    createSummariesErrorRecoveryHandlers(
      retryGeneration,
      recoverFromGenerationError,
      dispatch,
    );

  const handleDateFilterApply = useCallback(() => {
    if (startDate && endDate) {
      if (!dateFilterEnabled) {
        toggleDateFilter();
      }
      setDateRange({ start: new Date(startDate), end: new Date(endDate) });
    }
  }, [startDate, endDate, setDateRange, dateFilterEnabled, toggleDateFilter]);

  const preservedConfig = useMemo(
    () => derivePreservedConfig(generationError, state),
    [generationError, state],
  );

  return {
    state,
    dispatch,
    toggleSummaryContent,
    navigate,
    generate,
    progress,
    isGenerating,
    elapsedSeconds,
    generationError,
    recoverFromGenerationError,
    retryGeneration,
    canRetryGeneration,
    saveMutation,
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
    summaries,
    isLoading,
    isFetchError,
    refetch,
    pageDisplay,
    serverHasNext,
    serverHasPrev,
    goToNextPage,
    goToPreviousPage,
    deleteMutation,
    clearFilters,
    showCreateForm,
    setShowCreateForm,
    deleteError,
    selectedSummary,
    setSelectedSummary,
    isModalOpen,
    setIsModalOpen,
    deleteCandidate,
    setDeleteCandidate,
    savedPdfInfo,
    isSaving,
    saveError,
    startDate,
    setStartDate,
    endDate,
    setEndDate,
    enabledDataTypes,
    enhancedRefresh,
    isRefreshingData,
    refreshSuccess,
    refreshError,
    refreshMessage,
    robustDownloadMarkdown,
    isDownloadingFile,
    downloadError,
    lastFailedSummary,
    retryLastDownload,
    clearError,
    canRetry,
    selectedCount,
    daysSelected,
    ...creationHandlers,
    handleCancelGeneration: cancelGeneration,
    handleDateFilterApply,
    ...tableHandlers,
    formatDateForInput,
    preservedConfig,
    handleErrorRetry,
    handleResetToIdle,
  };
}

export type SummariesPageViewModel = ReturnType<typeof useSummariesPageModel>;
