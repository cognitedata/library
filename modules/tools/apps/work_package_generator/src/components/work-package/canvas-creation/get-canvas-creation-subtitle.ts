import type { CanvasResult } from "./types";

export function getCanvasCreationSubtitle(
	isReady: boolean,
	canvasResult?: CanvasResult
): string {
	if (!isReady) return "Preparing canvas generation...";
	if (canvasResult?.validation.success) {
		return "Canvas saved — open in Fusion or save again after you change selections.";
	}
	if (canvasResult) {
		return "Canvas saved — see the note below if verification had issues.";
	}
	return "Review preview, then save to Fusion.";
}
