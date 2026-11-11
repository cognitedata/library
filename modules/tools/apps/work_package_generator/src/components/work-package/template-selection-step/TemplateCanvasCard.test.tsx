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
					createdBy: "ada",
					updatedAt: "2024-06-01T15:04:05.000Z",
					ownerName: "Ada Lovelace",
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
		const card = screen.getByRole("button", { name: /pump pack/i });
		expect(card).toHaveAttribute("aria-pressed", "true");
		expect(card).toHaveAccessibleName(/AI pick/i);
		expect(screen.getByText("AI Pick")).toBeInTheDocument();
		expect(screen.getByText("Updated")).toBeInTheDocument();
		expect(screen.getByText("2024-06-01")).toBeInTheDocument();
		expect(screen.getByText("Owner")).toBeInTheDocument();
		expect(screen.getByText("Ada Lovelace")).toBeInTheDocument();
		expect(screen.getByText("Manual")).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /pump pack/i }));
		expect(onSelect).toHaveBeenCalledWith("tpl");
	});

	it("omits Updated and Owner on All Resources", () => {
		render(
			<TemplateCanvasCard
				template={{
					externalId: "__base_all_resources__",
					name: "All Resources",
					visibility: "public",
					space: "local",
				}}
				isSelected={false}
				isAiRecommended={false}
				slotsLoading={false}
				slots={[]}
				onSelect={vi.fn()}
			/>
		);
		expect(screen.queryByText("Updated")).not.toBeInTheDocument();
		expect(screen.queryByText("Owner")).not.toBeInTheDocument();
	});
});
