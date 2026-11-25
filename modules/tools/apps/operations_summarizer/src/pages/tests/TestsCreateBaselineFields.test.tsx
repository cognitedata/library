import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TestsCreateBaselineFields } from "./TestsCreateBaselineFields";

describe("TestsCreateBaselineFields date inputs", () => {
	it.each([
		"Filter baseline summaries from date",
		"Filter baseline summaries to date",
	])("shows a visible focus ring on %s (every segment)", (label) => {
		render(
			<TestsCreateBaselineFields
				summaries={[]}
				filteredSummaries={[]}
				filterStartDate=""
				filterEndDate=""
				onFilterStartDate={vi.fn()}
				onFilterEndDate={vi.fn()}
				selectedSummaryId=""
				onSelectSummary={vi.fn()}
			/>
		);

		const input = screen.getByLabelText(label);
		expect(input.className).toContain("focus:ring-2");
		expect(input.className).toContain("focus:ring-ring");
		expect(input.className).not.toContain("focus:ring-1");
	});
});
