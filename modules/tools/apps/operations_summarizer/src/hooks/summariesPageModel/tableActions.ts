import type { CogniteClient } from "@cognite/sdk";
import type { Dispatch } from "react";
import type { NavigationPage } from "@/contexts/NavigationContext";
import type { OperationsAction } from "@/contexts/operations-summarizer/types";
import type { SummariesRobustDownloadValue } from "@/contexts/SummariesPageModelDepsContext";
import type { SummariesListDeleteMutationRef } from "@/hooks/useDefaultSummariesListPipeline";
import type { SummaryInstance } from "@/types/operations";
import { getDeleteError, logError } from "@/utils/errorHandling";

interface TableActionsDeps {
  sdk: CogniteClient | null;
  deleteMutation: SummariesListDeleteMutationRef;
  robustDownloadPdf: SummariesRobustDownloadValue["downloadPdf"];
  robustDownloadMarkdown: SummariesRobustDownloadValue["downloadMarkdown"];
  downloadOperationSummaryPdfWithSignedUrlFirst: (
    sdk: CogniteClient,
    summary: SummaryInstance,
    options: { fallback: SummariesRobustDownloadValue["downloadPdf"] },
  ) => Promise<void>;
  navigate: (page: NavigationPage) => void;
  selectedSummary: SummaryInstance | null;
  deleteCandidate: SummaryInstance | null;
  setDeleteError: (value: string | null) => void;
  setDeleteCandidate: (value: SummaryInstance | null) => void;
  setSelectedSummary: (value: SummaryInstance | null) => void;
  setIsModalOpen: (value: boolean) => void;
}

export function createSummariesTableHandlers(deps: TableActionsDeps) {
  const {
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
  } = deps;

  const handleViewSummary = (summary: SummaryInstance) => {
    setSelectedSummary(summary);
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
    setSelectedSummary(null);
  };

  const handleDeleteConfirm = async (summaryFromChild?: SummaryInstance) => {
    const target = summaryFromChild ?? deleteCandidate;
    if (!target) return;
    setDeleteError(null);
    try {
      await deleteMutation.mutateAsync(target);
      setDeleteCandidate(null);
      if (selectedSummary?.externalId === target.externalId) {
        handleCloseModal();
      }
    } catch (err) {
      logError(err, "Delete summary");
      setDeleteError(getDeleteError(err, "summary"));
    }
  };

  const handleDeleteCancel = () => {
    setDeleteCandidate(null);
  };

  const handleDownloadMd = async (summary: SummaryInstance) => {
    await robustDownloadMarkdown(summary);
  };

  const handleDownloadPdf = async (summary: SummaryInstance) => {
    if (!sdk) {
      await robustDownloadPdf(summary);
      return;
    }
    await downloadOperationSummaryPdfWithSignedUrlFirst(sdk, summary, {
      fallback: robustDownloadPdf,
    });
  };

  const handleCreateTest = () => {
    navigate("tests");
  };

  return {
    handleViewSummary,
    handleCloseModal,
    handleDeleteConfirm,
    handleDeleteCancel,
    handleDownloadMd,
    handleDownloadPdf,
    handleCreateTest,
  };
}

export function createSummariesErrorRecoveryHandlers(
  retryGeneration: () => Promise<unknown>,
  recoverFromGenerationError: () => void,
  dispatch: Dispatch<OperationsAction>,
) {
  const handleErrorRetry = async () => {
    await retryGeneration();
  };

  const handleResetToIdle = () => {
    recoverFromGenerationError();
    dispatch({ type: "SET_SHOW_SUMMARY", payload: false });
  };

  return { handleErrorRetry, handleResetToIdle };
}
