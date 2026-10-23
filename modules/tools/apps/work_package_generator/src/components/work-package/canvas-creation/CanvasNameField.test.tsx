import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { CanvasNameField } from "./CanvasNameField";

describe("CanvasNameField", () => {
	it("reports name changes and a lookup retry", async () => {
		const user = userEvent.setup();
		const onCanvasNameChange = vi.fn();
		const onRetry = vi.fn();
		render(
			<CanvasNameField
				canvasName=""
				onCanvasNameChange={onCanvasNameChange}
				isCreating={false}
				isCheckingExists={false}
				existingCanvasLookupErrorMessage="Lookup failed"
				onRetryExistingCanvasLookup={onRetry}
				hasExistingCanvas={false}
			/>
		);
		expect(
			screen.getByText("Canvas name is required to save")
		).toBeInTheDocument();
		await user.type(screen.getByLabelText("Canvas Name"), "A");
		expect(onCanvasNameChange).toHaveBeenCalled();
		await user.click(screen.getByRole("button", { name: /retry check/i }));
		expect(onRetry).toHaveBeenCalled();
	});

	it("shows the existing-canvas notice", () => {
		render(
			<CanvasNameField
				canvasName="Taken"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas
			/>
		);
		expect(screen.getByText(/already exists/i)).toBeInTheDocument();
	});
});
