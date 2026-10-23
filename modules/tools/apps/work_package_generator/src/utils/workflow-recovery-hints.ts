import type { AiSelections, TemplateField, ValidationResult } from "@/types";
import {
	getCanvasValidationUserMessages,
	isCanvasValidationNetworkOrUnknown,
} from "./canvas-validation-messages";

/** Sidebar step index: Template = 1, Resources = 2, Selections = 3 */
export type WorkflowRecoveryStep = 1 | 2 | 3;

export type WorkflowRecoveryHint = {
	step: WorkflowRecoveryStep;
	/** Full line shown as the link label, e.g. "2 resources unmatched — review Selections" */
	label: string;
};

function countUnmatchedSlots(
	templateFields: TemplateField[],
	aiSelections: AiSelections
): number {
	return templateFields.filter((tf) => !aiSelections[tf.label]).length;
}

/**
 * Actionable workflow steps to jump to after save (or on success with gaps),
 * derived from empty AI slots and post-save validation.
 */
export function getWorkflowRecoveryHints(params: {
	templateFields: TemplateField[];
	aiSelections: AiSelections;
	validation: ValidationResult;
}): WorkflowRecoveryHint[] {
	const { templateFields, aiSelections, validation } = params;
	const unmatched = countUnmatchedSlots(templateFields, aiSelections);
	const byStep = new Map<WorkflowRecoveryStep, string>();

	if (unmatched > 0) {
		const resourceWord = unmatched === 1 ? "resource" : "resources";
		byStep.set(3, `${unmatched} ${resourceWord} unmatched — review Selections`);
		const slotWord = unmatched === 1 ? "slot" : "slots";
		byStep.set(
			2,
			`${unmatched} ${slotWord} had no automatic match — review Resources`
		);
	}

	const isIndeterminate = isCanvasValidationNetworkOrUnknown(validation);

	if (!validation.success && !isIndeterminate) {
		if (
			!validation.itemsMatchExpected &&
			validation.expectedItemsCount >= 0 &&
			!byStep.has(3)
		) {
			if (validation.itemsCount < validation.expectedItemsCount) {
				byStep.set(
					3,
					"Canvas has fewer items than expected — review Selections"
				);
			} else {
				byStep.set(
					3,
					"Canvas has more items than expected — review Selections"
				);
			}
		}

		if (
			byStep.size === 0 &&
			getCanvasValidationUserMessages(validation).length > 0
		) {
			byStep.set(3, "Verification issue — review Selections, then save again");
		}
	}

	/** Preserve Template (1) for future hints; Resources before Selections in the list. */
	const orderedSteps: WorkflowRecoveryStep[] = [1, 2, 3];
	return orderedSteps
		.map((step) => {
			const label = byStep.get(step);
			return label ? { step, label } : null;
		})
		.filter((x): x is WorkflowRecoveryHint => x != null);
}
