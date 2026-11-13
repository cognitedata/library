import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "@/utils/workflow-copy";
import { SelectionsEmptyState } from "./SelectionsEmptyState";

const base = {
	workOrder: { externalId: "wo", space: "s" },
	templateCanvasId: "tpl",
	isBaseTemplate: false,
	isLoadingFields: false,
	templateFieldsErrorMessage: null,
};

describe("SelectionsEmptyState", () => {
	it("prompts for a work order when none is selected", () => {
		render(<SelectionsEmptyState {...base} workOrder={null} />);
		expect(screen.getByText(SEARCH_WORK_ORDER_TO_BEGIN)).toBeInTheDocument();
	});

	it("retries a failed template-field load", async () => {
		const user = userEvent.setup();
		const onRetry = vi.fn();
		render(
			<SelectionsEmptyState
				{...base}
				templateFieldsErrorMessage="Could not load slots"
				onRetryTemplateFields={onRetry}
			/>
		);
		await user.click(screen.getByRole("button", { name: /retry/i }));
		expect(onRetry).toHaveBeenCalled();
	});
});
