import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ExistingCanvasSaveChoiceDialog } from "./ExistingCanvasSaveChoiceDialog";

describe("ExistingCanvasSaveChoiceDialog", () => {
	it("updates the existing canvas and closes", async () => {
		const user = userEvent.setup();
		const onUpdateExisting = vi.fn();
		const onOpenChange = vi.fn();
		render(
			<ExistingCanvasSaveChoiceDialog
				open
				onOpenChange={onOpenChange}
				onUpdateExisting={onUpdateExisting}
				onCreateNew={vi.fn()}
				isActionPending={false}
				canvasName="WO canvas"
			/>
		);
		await user.click(screen.getByRole("button", { name: /update existing/i }));
		expect(onUpdateExisting).toHaveBeenCalled();
		expect(onOpenChange).toHaveBeenCalledWith(false);
	});
});
