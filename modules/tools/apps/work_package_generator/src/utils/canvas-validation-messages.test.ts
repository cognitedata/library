import { describe, expect, it } from "vitest";
import type { ValidationResult } from "@/types";
import { getCanvasValidationUserMessages } from "./canvas-validation-messages";

function base(overrides: Partial<ValidationResult>): ValidationResult {
	return {
		success: false,
		canvasExists: true,
		itemsCount: 0,
		expectedItemsCount: 0,
		itemsMatchExpected: true,
		canvasId: "test-canvas",
		errors: [],
		...overrides,
	};
}

describe("getCanvasValidationUserMessages", () => {
	it("returns a single friendly line for validation API errors", () => {
		const v = base({
			canvasExists: false,
			itemsMatchExpected: false,
			errors: ["Validation error: network timeout"],
		});
		const msgs = getCanvasValidationUserMessages(v);
		expect(msgs).toHaveLength(1);
		expect(msgs[0]).toContain("verify the canvas");
	});

	it("explains missing canvas when not a validation-error catch", () => {
		const v = base({
			canvasExists: false,
			itemsMatchExpected: true,
			errors: ["Canvas node not found"],
		});
		const msgs = getCanvasValidationUserMessages(v);
		expect(msgs.some((m) => m.includes("couldn’t find the canvas"))).toBe(true);
	});

	it("explains more items than expected", () => {
		const v = base({
			success: false,
			itemsCount: 14,
			expectedItemsCount: 6,
			itemsMatchExpected: false,
			errors: ["Item count mismatch: expected 6, found 14"],
		});
		const msgs = getCanvasValidationUserMessages(v);
		expect(msgs.some((m) => m.includes("more items"))).toBe(true);
		expect(msgs.some((m) => m.includes("Selections"))).toBe(true);
	});

	it("explains fewer items than expected", () => {
		const v = base({
			success: false,
			itemsCount: 2,
			expectedItemsCount: 8,
			itemsMatchExpected: false,
			errors: ["Item count mismatch"],
		});
		const msgs = getCanvasValidationUserMessages(v);
		expect(msgs.some((m) => m.includes("fewer items"))).toBe(true);
	});

	it("explains data mismatch when count matches but items don't match expected", () => {
		const v = base({
			success: false,
			itemsCount: 5,
			expectedItemsCount: 5,
			itemsMatchExpected: false,
			errors: ["Item data mismatch"],
		});
		const msgs = getCanvasValidationUserMessages(v);
		expect(msgs.some((m) => m.includes("don't match what we expected"))).toBe(
			true
		);
		expect(msgs.some((m) => m.includes("Open it in Fusion"))).toBe(true);
		expect(msgs.some((m) => !m.includes("fewer items"))).toBe(true);
		expect(msgs.some((m) => !m.includes("more items"))).toBe(true);
	});

	it("falls back when only unknown error strings exist", () => {
		const v = base({
			success: false,
			canvasExists: true,
			itemsMatchExpected: true,
			errors: ["Something weird"],
		});
		const msgs = getCanvasValidationUserMessages(v);
		expect(msgs).toHaveLength(1);
		expect(msgs[0]).toContain("didn’t match our checks");
	});
});
