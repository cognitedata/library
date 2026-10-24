/**
 * Shared utilities for CDF operations
 *
 * This module exports utilities that are actually used across multiple modules.
 */

export { cdfApiFetch, parseJsonResponse } from "./api-helpers";
export { buildCanvasNodesAndEdges } from "./canvas-helpers";
export {
	applyAssetReplacement,
	applySelectionReplacement,
	applyWorkOrderReplacement,
	isMainAsset,
	isMainWorkOrder,
	isSelectableResourceSlot,
} from "./canvas-item-helpers";
export { getCanvasValidationUserMessages } from "./canvas-validation-messages";
export {
	extractInstanceProperties,
	getArrayProp,
	getNumberProp,
	getOptionalStringProp,
	getStringProp,
	mapNodeToResourceInstance,
	toCdfNode,
} from "./cdf-helpers";
export { copyToClipboard } from "./clipboard";
export { cn } from "./cn";
export {
	extractAssetContext,
	extractWorkOrderContext,
} from "./context-helpers";
export { formatDate, nowIsoMs } from "./datetime";
export {
	getErrorMessage,
	looksLikeTechnicalErrorMessage,
	toUserFacingErrorMessage,
	USER_FACING_ERROR_FALLBACK,
} from "./errors";
export { formatPropertyValue, getColumnTitle } from "./format";
export { logError, type StructuredErrorLog } from "./logger";
export { generateUuid } from "./uuid";
export { SEARCH_WORK_ORDER_TO_BEGIN } from "./workflow-copy";
export {
	getWorkflowRecoveryHints,
	type WorkflowRecoveryHint,
	type WorkflowRecoveryStep,
} from "./workflow-recovery-hints";
