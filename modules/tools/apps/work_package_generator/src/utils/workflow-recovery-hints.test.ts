import { describe, expect, it } from "vitest";
import type { ValidationResult } from "@/types";
import { getWorkflowRecoveryHints } from "./workflow-recovery-hints";

const emptyValidation: ValidationResult = {
	success: true,
	canvasExists: true,
	itemsCount: 0,
	expectedItemsCount: 0,
	itemsMatchExpected: true,
	canvasId: "c1",
	errors: [],
};

describe("getWorkflowRecoveryHints", () => {
	it("returns Selections and Resources links when slots are unmatched", () => {
		const hints = getWorkflowRecoveryHints({
			templateFields: [
				{ type: "x", label: "A" },
				{ type: "x", label: "B" },
			],
			aiSelections: {},
			validation: emptyValidation,
		});
		expect(hints).toHaveLength(2);
		expect(hints[0].step).toBe(2);
		expect(hints[0].label).toContain("2");
		expect(hints[0].label).toContain("Resources");
		expect(hints[1].step).toBe(3);
		expect(hints[1].label).toContain("Selections");
		expect(hints[1].label).toContain("unmatched");
	});

	it("uses singular copy for one unmatched slot", () => {
		const hints = getWorkflowRecoveryHints({
			templateFields: [{ type: "x", label: "Only" }],
			aiSelections: {},
			validation: emptyValidation,
		});
		expect(hints.find((h) => h.step === 3)?.label).toContain("1 resource");
		expect(hints.find((h) => h.step === 2)?.label).toContain("1 slot");
	});

	it("adds Selections hint for item count mismatch when slots are filled", () => {
		const v: ValidationResult = {
			success: false,
			canvasExists: true,
			itemsCount: 2,
			expectedItemsCount: 5,
			itemsMatchExpected: false,
			canvasId: "c1",
			errors: ["mismatch"],
		};
		const hints = getWorkflowRecoveryHints({
			templateFields: [{ type: "x", label: "A" }],
			aiSelections: {
				A: { space: "s", externalId: "e" },
			},
			validation: v,
		});
		expect(hints).toHaveLength(1);
		expect(hints[0].step).toBe(3);
		expect(hints[0].label).toContain("fewer items");
	});

	it("does not add duplicate Selections when unmatched already covered", () => {
		const v: ValidationResult = {
			success: false,
			canvasExists: true,
			itemsCount: 0,
			expectedItemsCount: 2,
			itemsMatchExpected: false,
			canvasId: "c1",
			errors: ["mismatch"],
		};
		const hints = getWorkflowRecoveryHints({
			templateFields: [
				{ type: "x", label: "A" },
				{ type: "x", label: "B" },
			],
			aiSelections: {},
			validation: v,
		});
		const selections = hints.filter((h) => h.step === 3);
		expect(selections).toHaveLength(1);
	});

	it("returns no hints when save succeeded and all slots filled", () => {
		const hints = getWorkflowRecoveryHints({
			templateFields: [{ type: "x", label: "A" }],
			aiSelections: { A: { space: "s", externalId: "e" } },
			validation: emptyValidation,
		});
		expect(hints).toHaveLength(0);
	});

	it("hints more items than expected when counts diverge upward", () => {
		const v: ValidationResult = {
			success: false,
			canvasExists: true,
			itemsCount: 5,
			expectedItemsCount: 2,
			itemsMatchExpected: false,
			canvasId: "c1",
			errors: ["x"],
		};
		const hints = getWorkflowRecoveryHints({
			templateFields: [{ type: "x", label: "A" }],
			aiSelections: { A: { space: "s", externalId: "e" } },
			validation: v,
		});
		expect(hints.some((h) => h.label.includes("more items"))).toBe(true);
	});

	it("adds verification hint when user messages exist and no other hint applied", () => {
		const v: ValidationResult = {
			success: false,
			canvasExists: false,
			itemsCount: 1,
			expectedItemsCount: 1,
			itemsMatchExpected: true,
			canvasId: "c1",
			errors: [],
		};
		const hints = getWorkflowRecoveryHints({
			templateFields: [{ type: "x", label: "A" }],
			aiSelections: { A: { space: "s", externalId: "e" } },
			validation: v,
		});
		expect(hints.some((h) => h.label.includes("Verification issue"))).toBe(
			true
		);
	});
});
