import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { AutomationReviewSection } from "./AutomationReviewSection";

describe("AutomationReviewSection", () => {
	it("continues to create and reports empty slots", async () => {
		const user = userEvent.setup();
		const onContinue = vi.fn();
		render(
			<AutomationReviewSection
				templateDisplayName="Pump pack"
				filled={1}
				total={2}
				emptySlotCount={1}
				onContinueToCreate={onContinue}
				onGoToResources={vi.fn()}
			/>
		);
		expect(screen.getByText("Pump pack")).toBeInTheDocument();
		expect(screen.getByText(/no suggested resource/i)).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /continue/i }));
		expect(onContinue).toHaveBeenCalled();
	});
});
