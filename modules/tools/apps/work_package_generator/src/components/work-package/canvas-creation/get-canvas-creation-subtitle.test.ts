import { describe, expect, it } from "vitest";
import { getCanvasCreationSubtitle } from "./get-canvas-creation-subtitle";
import type { CanvasResult } from "./types";

const result = (success: boolean): CanvasResult => ({
	canvasId: "c1",
	canvasLink: "https://example.test/c",
	validation: {
		success,
		canvasExists: true,
		itemsCount: 1,
		expectedItemsCount: 1,
		itemsMatchExpected: true,
		canvasId: "c1",
		errors: [],
	},
	isUpdate: false,
});

describe(getCanvasCreationSubtitle.name, () => {
	it("should describe preparing when the workflow is not ready", () => {
		expect(getCanvasCreationSubtitle(false)).toBe(
			"Preparing canvas generation..."
		);
	});

	it("should describe preview when ready and unsaved", () => {
		expect(getCanvasCreationSubtitle(true)).toBe(
			"Review preview, then save to Fusion."
		);
	});

	it("should describe a successful save", () => {
		expect(getCanvasCreationSubtitle(true, result(true))).toBe(
			"Canvas saved — open in Fusion or save again after you change selections."
		);
	});

	it("should describe a save with verification issues", () => {
		expect(getCanvasCreationSubtitle(true, result(false))).toBe(
			"Canvas saved — see the note below if verification had issues."
		);
	});
});
