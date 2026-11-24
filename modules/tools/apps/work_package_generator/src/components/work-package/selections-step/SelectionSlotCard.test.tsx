import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { SelectionSlotCard } from "./SelectionSlotCard";

const field = { type: "file", label: "Procedure" };

describe("SelectionSlotCard", () => {
	it("shows the selected resource and opens manual pick", async () => {
		const user = userEvent.setup();
		const onPick = vi.fn();
		render(
			<SelectionSlotCard
				field={field}
				selection={{ externalId: "f1", space: "s", name: "Doc" }}
				canPickManually
				isRunningAiSelection={false}
				onPickManually={onPick}
			/>
		);
		expect(screen.getByText("Doc")).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /pick manually/i }));
		expect(onPick).toHaveBeenCalledWith("Procedure");
	});

	it("C16: shows the slot error and still allows a manual pick", () => {
		render(
			<SelectionSlotCard
				field={field}
				selection={null}
				canPickManually
				isRunningAiSelection={false}
				slotError="AI unavailable: pick manually or retry"
				onPickManually={vi.fn()}
				onRetrySlot={vi.fn()}
			/>
		);
		expect(
			screen.getByText("AI unavailable: pick manually or retry")
		).toBeInTheDocument();
		expect(
			screen.getByRole("button", { name: /pick manually/i })
		).toBeEnabled();
		expect(
			screen.getByRole("button", { name: /^Retry$/i })
		).toBeInTheDocument();
	});
});
