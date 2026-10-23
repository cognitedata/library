import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ValidationResult } from "@/types";
import { copyToClipboard } from "@/utils/clipboard";
import { CanvasResultCard } from "./CanvasResultCard";
import type { CanvasResult } from "./types";

// vi.mock: clipboard is a browser API wrapper (not DI); stub avoids real clipboard access.
vi.mock("@/utils/clipboard", () => ({
	copyToClipboard: vi.fn(() => Promise.resolve()),
}));

function validation(
	overrides: Partial<ValidationResult> = {}
): ValidationResult {
	return {
		success: true,
		canvasExists: true,
		itemsCount: 1,
		expectedItemsCount: 1,
		itemsMatchExpected: true,
		canvasId: "c1",
		errors: [],
		...overrides,
	};
}

function result(overrides: Partial<CanvasResult> = {}): CanvasResult {
	return {
		canvasId: "c1",
		canvasLink: "https://example.test/c",
		validation: validation(),
		isUpdate: false,
		...overrides,
	};
}

const recoveryHints = [
	{
		step: 2 as const,
		label: "1 slot had no automatic match — review Resources",
	},
	{ step: 3 as const, label: "1 resource unmatched — review Selections" },
];

describe(CanvasResultCard.name, () => {
	beforeEach(() => {
		vi.clearAllMocks();
		vi.mocked(copyToClipboard).mockResolvedValue(undefined);
	});

	it("should show a created-success message and Fusion link", () => {
		render(
			<CanvasResultCard
				result={result()}
				canvasName="Package A"
				recoveryHints={[]}
			/>
		);

		expect(
			screen.getByText("Canvas created successfully!")
		).toBeInTheDocument();
		const link = screen.getByRole("link", { name: /Open in Fusion/i });
		expect(link).toHaveAttribute("href", "https://example.test/c");
		expect(link).toHaveAttribute("target", "_blank");
		expect(
			screen.getByRole("button", { name: /Copy link/i })
		).toBeInTheDocument();
		expect(
			screen.getByRole("button", { name: /Email link/i })
		).toBeInTheDocument();
	});

	it("should show an updated-success message when the result is an update", () => {
		render(
			<CanvasResultCard
				result={result({ isUpdate: true })}
				canvasName="Package A"
				recoveryHints={[]}
			/>
		);

		expect(
			screen.getByText("Canvas updated successfully!")
		).toBeInTheDocument();
	});

	it("should show verification messages when save validation failed", () => {
		render(
			<CanvasResultCard
				result={result({
					validation: validation({
						success: false,
						canvasExists: false,
						itemsMatchExpected: true,
					}),
				})}
				canvasName="Package A"
				recoveryHints={[]}
			/>
		);

		expect(
			screen.getByText("Canvas saved, but we couldn’t fully verify it")
		).toBeInTheDocument();
		expect(
			screen.getByText(/We couldn’t find the canvas in CDF yet/i)
		).toBeInTheDocument();
		expect(
			screen.queryByLabelText("Jump to workflow steps to fix issues")
		).not.toBeInTheDocument();
	});

	it("should not render recovery links without a navigation handler", () => {
		render(
			<CanvasResultCard
				result={result()}
				canvasName="Package A"
				recoveryHints={recoveryHints}
			/>
		);

		expect(
			screen.queryByLabelText("Jump to workflow steps to fix issues")
		).not.toBeInTheDocument();
	});

	it("should render success recovery hints and navigate on click", () => {
		const onNavigate = vi.fn();
		render(
			<CanvasResultCard
				result={result()}
				canvasName="Package A"
				recoveryHints={recoveryHints}
				onNavigateToWorkflowStep={onNavigate}
			/>
		);

		expect(
			screen.getByText("Some slots still need a resource — open a step to fix:")
		).toBeInTheDocument();

		fireEvent.click(
			screen.getByRole("button", {
				name: "1 resource unmatched — review Selections",
			})
		);
		expect(onNavigate).toHaveBeenCalledWith(3);
	});

	it("should render failure recovery hints and navigate on click", () => {
		const onNavigate = vi.fn();
		render(
			<CanvasResultCard
				result={result({
					validation: validation({
						success: false,
						itemsMatchExpected: false,
						itemsCount: 0,
						expectedItemsCount: 2,
					}),
				})}
				canvasName="Package A"
				recoveryHints={recoveryHints}
				onNavigateToWorkflowStep={onNavigate}
			/>
		);

		expect(
			screen.getByText("Fix issues faster — open a step:")
		).toBeInTheDocument();

		fireEvent.click(
			screen.getByRole("button", {
				name: "1 slot had no automatic match — review Resources",
			})
		);
		expect(onNavigate).toHaveBeenCalledWith(2);
	});

	it("should show copied confirmation after copying the canvas link", async () => {
		render(
			<CanvasResultCard
				result={result()}
				canvasName="Package A"
				recoveryHints={[]}
			/>
		);

		await act(async () => {
			fireEvent.click(screen.getByRole("button", { name: /Copy link/i }));
		});

		expect(copyToClipboard).toHaveBeenCalledWith("https://example.test/c");
		expect(screen.getByText("Copied!")).toBeInTheDocument();
	});
});
