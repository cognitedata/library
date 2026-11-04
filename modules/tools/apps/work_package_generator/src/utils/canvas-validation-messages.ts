import type { ValidationResult } from "@/types";

/** True when we only have a generic validation failure (e.g. network), not actionable item checks. */
export function isCanvasValidationNetworkOrUnknown(
	validation: ValidationResult
): boolean {
	return (
		validation.errors.length === 1 &&
		validation.errors[0].startsWith("Validation error:")
	);
}

/**
 * Maps post-save validation state to user-facing bullet messages.
 * Does not expose raw API error strings.
 */
export function getCanvasValidationUserMessages(
	validation: ValidationResult
): string[] {
	const messages: string[] = [];

	const isNetworkOrUnknown = isCanvasValidationNetworkOrUnknown(validation);

	if (isNetworkOrUnknown) {
		messages.push(
			"We couldn’t verify the canvas right now. You can still open it in Fusion. If something looks wrong, try saving again in a moment."
		);
		return messages;
	}

	if (!validation.canvasExists) {
		messages.push(
			"We couldn’t find the canvas in CDF yet. It may take a moment to appear—try opening the link below, or save again if it still fails."
		);
	}

	if (!validation.itemsMatchExpected && validation.expectedItemsCount >= 0) {
		const { itemsCount, expectedItemsCount } = validation;
		// Handle different types of mismatches with specific messaging
		if (itemsCount > expectedItemsCount) {
			messages.push(
				"The canvas has more items than we expected after save. Open it in Fusion to review, or go back to Selections to adjust resources and save again."
			);
		} else if (itemsCount < expectedItemsCount) {
			messages.push(
				"The canvas has fewer items than we expected after save. Go back to Selections to review resources, then save again, or open Fusion to check the canvas."
			);
		} else {
			messages.push(
				"The canvas items don't match what we expected after save. Open it in Fusion to review, or go back to Selections to adjust resources and save again."
			);
		}
	}

	if (messages.length === 0 && validation.errors.length > 0) {
		messages.push(
			"Something didn’t match our checks after save. Open the canvas in Fusion to confirm, or go back to Selections and try saving again."
		);
	}

	return messages;
}
