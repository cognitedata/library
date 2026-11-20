import type { ValidationResult } from "@/types";

export interface CanvasResult {
	canvasId: string;
	canvasLink: string;
	validation: ValidationResult;
	isUpdate: boolean;
	/** The canvas was saved, but some previous items could not be removed. */
	cleanupIncomplete?: boolean;
}
