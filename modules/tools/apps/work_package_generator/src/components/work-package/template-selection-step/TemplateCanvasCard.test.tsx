import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TemplateCanvasCard } from "./TemplateCanvasCard";

describe("TemplateCanvasCard", () => {
	it("selects the template and lists slots", async () => {
		const user = userEvent.setup();
		const onSelect = vi.fn();
		render(
			<TemplateCanvasCard
				template={{
					externalId: "tpl",
					name: "Pump pack",
					visibility: "public",
					space: "s",
				}}
				isSelected
				isAiRecommended
				slotsLoading={false}
				slots={[
					{
						label: "Manual",
						viewExternalId: "CogniteFile",
						category: "file",
						populatedBy: "ai",
					},
				]}
				onSelect={onSelect}
			/>
		);
		expect(screen.getByText("AI Pick")).toBeInTheDocument();
		expect(screen.getByText("Manual")).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /pump pack/i }));
		expect(onSelect).toHaveBeenCalledWith("tpl");
	});
});
