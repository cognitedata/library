import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { NavigationProvider, useNavigation } from "./NavigationContext";

function Consumer() {
	const {
		currentPage,
		navigate,
		goToNext,
		goToPrevious,
		canGoNext,
		canGoPrevious,
	} = useNavigation();
	return (
		<>
			<span data-testid="page">{currentPage}</span>
			<span data-testid="can-go-next">{canGoNext.toString()}</span>
			<span data-testid="can-go-previous">{canGoPrevious.toString()}</span>
			<button type="button" onClick={() => navigate("canvas-selection")}>
				navigate
			</button>
			<button type="button" onClick={goToNext} disabled={!canGoNext}>
				next
			</button>
			<button type="button" onClick={goToPrevious} disabled={!canGoPrevious}>
				previous
			</button>
		</>
	);
}

describe("NavigationContext", () => {
	it("provides default work-order-selection page", () => {
		render(
			<NavigationProvider>
				<Consumer />
			</NavigationProvider>
		);
		expect(screen.getByTestId("page")).toHaveTextContent(
			"work-order-selection"
		);
	});

	it("throws when useNavigation is used outside provider", () => {
		expect(() => render(<Consumer />)).toThrow(
			/useNavigation must be used within a NavigationProvider/
		);
	});

	it("navigates between workflow steps", () => {
		render(
			<NavigationProvider>
				<Consumer />
			</NavigationProvider>
		);
		expect(screen.getByTestId("page")).toHaveTextContent(
			"work-order-selection"
		);
		fireEvent.click(screen.getByRole("button", { name: "navigate" }));
		expect(screen.getByTestId("page")).toHaveTextContent("canvas-selection");
	});

	it("handles sequential navigation with goToNext and goToPrevious", () => {
		render(
			<NavigationProvider>
				<Consumer />
			</NavigationProvider>
		);

		// Start at work-order-selection
		expect(screen.getByTestId("page")).toHaveTextContent(
			"work-order-selection"
		);
		expect(screen.getByTestId("can-go-next")).toHaveTextContent("true");
		expect(screen.getByTestId("can-go-previous")).toHaveTextContent("false");

		// Go to next step
		fireEvent.click(screen.getByRole("button", { name: "next" }));
		expect(screen.getByTestId("page")).toHaveTextContent("canvas-selection");
		expect(screen.getByTestId("can-go-previous")).toHaveTextContent("true");

		// Go back to previous step
		fireEvent.click(screen.getByRole("button", { name: "previous" }));
		expect(screen.getByTestId("page")).toHaveTextContent(
			"work-order-selection"
		);
		expect(screen.getByTestId("can-go-previous")).toHaveTextContent("false");
	});

	it("prevents navigation beyond boundaries", () => {
		render(
			<NavigationProvider>
				<Consumer />
			</NavigationProvider>
		);

		// At first step, previous should be disabled
		expect(screen.getByRole("button", { name: "previous" })).toBeDisabled();

		// Navigate to last step
		fireEvent.click(screen.getByRole("button", { name: "navigate" })); // canvas-selection
		fireEvent.click(screen.getByRole("button", { name: "next" })); // canvas-generation
		fireEvent.click(screen.getByRole("button", { name: "next" })); // ai-selection
		fireEvent.click(screen.getByRole("button", { name: "next" })); // canvas-creation

		expect(screen.getByTestId("page")).toHaveTextContent("canvas-creation");
		expect(screen.getByTestId("can-go-next")).toHaveTextContent("false");
		expect(screen.getByRole("button", { name: "next" })).toBeDisabled();
	});
});
