/**
 * Shared types for summary generation (core, work orders, hook).
 */

type GenerationProgressStatus =
	| "idle"
	| "fetching"
	| "processing"
	| "generating"
	| "saving"
	| "complete"
	| "error";

export interface SummaryGenerationProgressUpdate {
	status: GenerationProgressStatus;
	message: string;
	progress: number;
	currentStep?: string;
}
