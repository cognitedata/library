import type { ValidationResult } from "@/types";

export interface CanvasResult {
	canvasId: string;
	canvasLink: string;
	validation: ValidationResult;
	isUpdate: boolean;
}
