import type { CogniteClient } from "@cognite/sdk";
import type { Dispatch } from "react";
import type { NavigationPage } from "@/contexts/NavigationContext";
import type { OperationsAction } from "@/contexts/operations-summarizer/types";
import type { SummariesRobustDownloadValue } from "@/contexts/SummariesPageModelDepsContext";
import { parseDatesFromExternalId } from "@/hooks/testsPageModel/dateUtils";
import type { SummariesListDeleteMutationRef } from "@/hooks/useDefaultSummariesListPipeline";
import type { DataTypeName, SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { getDeleteError, logError } from "@/utils/errorHandling";
import { extractFileProperties } from "@/utils/nodeProperties";
import { encodeTestBaselineSelectionKey } from "@/utils/summaryInstanceMapping";
import { SUMMARY_CONTENT_TYPE_BITMAP_ORDER } from "@/utils/summaryListQuery";

const RANGE_TAG_RE = /^range:(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})$/;
const OPS_BITMAP_RE = /^OPS:([01]+)\n/;

/**
 * Period covered by a summary: `range:` tag the function writes, else the dates in its external id.
 * Local fallback until `getSummaryPeriod` (Group D1, `utils/summaryPeriod.ts`) is merged; switch to it then.
 */
function getBaselinePeriod(
	summary: SummaryInstance,
	tags: unknown
): { start: string; end: string } | null {
	if (Array.isArray(tags)) {
		for (const tag of tags) {
			const match = typeof tag === "string" ? RANGE_TAG_RE.exec(tag) : null;
			if (match) return { start: match[1], end: match[2] };
		}
	}
	return parseDatesFromExternalId(summary.externalId);
}

/** Data types encoded in the `OPS:` bitmap line of the summary description, if any. */
function getBaselineDataTypes(description: unknown): DataTypeName[] | null {
	if (typeof description !== "string") return null;
	const bits = OPS_BITMAP_RE.exec(description)?.[1];
	if (!bits) return null;
	const types = SUMMARY_CONTENT_TYPE_BITMAP_ORDER.filter(
		(_, index) => bits[index] === "1"
	);
	return types.length > 0 ? types : null;
}

interface TableActionsDeps {
	sdk: CogniteClient | null;
	deleteMutation: SummariesListDeleteMutationRef;
	robustDownloadPdf: SummariesRobustDownloadValue["downloadPdf"];
	robustDownloadMarkdown: SummariesRobustDownloadValue["downloadMarkdown"];
	downloadOperationSummaryPdfWithSignedUrlFirst: (
		sdk: CogniteClient,
		summary: SummaryInstance,
		options: { fallback: SummariesRobustDownloadValue["downloadPdf"] }
	) => Promise<void>;
	navigate: (page: NavigationPage) => void;
	dispatch: Dispatch<OperationsAction>;
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
		dispatch,
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

	const handleCreateTest = (summary: SummaryInstance) => {
		const fileProps = extractFileProperties(
			summary,
			VIEW_IDS.SUMMARY_OUTPUT_FILE
		);
		const period = getBaselinePeriod(summary, fileProps?.tags);
		const dataTypes = getBaselineDataTypes(fileProps?.description);

		dispatch({
			type: "SET_TEST_SELECTED_SUMMARY_ID",
			payload: encodeTestBaselineSelectionKey(summary),
		});
		if (period) {
			dispatch({ type: "SET_TEST_START_DATE", payload: period.start });
			dispatch({ type: "SET_TEST_END_DATE", payload: period.end });
		}
		if (dataTypes) {
			dispatch({ type: "SET_TEST_SELECTED_DATA_TYPES", payload: dataTypes });
		}
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
	dispatch: Dispatch<OperationsAction>
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
