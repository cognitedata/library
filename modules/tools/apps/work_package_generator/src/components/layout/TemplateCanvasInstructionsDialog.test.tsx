import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TemplateCanvasInstructionsDialog } from "./TemplateCanvasInstructionsDialog";

describe("TemplateCanvasInstructionsDialog", () => {
	it("opens the instructions dialog when the info button is clicked", () => {
		render(<TemplateCanvasInstructionsDialog />);

		fireEvent.click(
			screen.getByRole("button", {
				name: /how to create a work package template/i,
			})
		);

		expect(
			screen.getByRole("dialog", { name: /creating a template canvas/i })
		).toBeInTheDocument();
		expect(
			screen.getByText(/Add the official "Work Packages" tag/i)
		).toBeInTheDocument();
		expect(
			screen.getByText(/template dropdown selection menu/i)
		).toBeInTheDocument();
	});

	it("closes the dialog when the close button is clicked", () => {
		render(<TemplateCanvasInstructionsDialog />);

		fireEvent.click(
			screen.getByRole("button", {
				name: /how to create a work package template/i,
			})
		);
		fireEvent.click(screen.getByRole("button", { name: /close dialog/i }));

		expect(
			screen.getByRole("dialog", { name: /creating a template canvas/i })
		).toHaveAttribute("data-closed");
	});
});
