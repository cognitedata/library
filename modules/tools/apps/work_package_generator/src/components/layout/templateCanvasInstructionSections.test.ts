import { describe, expect, it } from "vitest";
import { instructionSections } from "./templateCanvasInstructionSections";

describe(instructionSections.name, () => {
	const terms = {
		workOrder: "Work Order",
		workPackage: "Work Package",
		functionalLocation: "Functional Location",
	};
	const examples = {
		workOrderId: "WO-1",
		functionalLocationId: "FL-1",
	};

	it("returns four numbered sections", () => {
		// Act
		const sections = instructionSections(examples, terms);

		// Assert
		expect(sections.map((section) => section.title.charAt(0))).toEqual([
			"1",
			"2",
			"3",
			"4",
		]);
	});

	it("interpolates configured terms and example ids", () => {
		// Act
		const text = JSON.stringify(instructionSections(examples, terms));

		// Assert
		expect(text).toContain("Work Packages");
		expect(text).toContain("Work Orders: (e.g., WO-1)");
		expect(text).toContain("Functional Locations: (e.g., FL-1)");
	});
});
