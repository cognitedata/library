import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { WorkPackageGeneratorLayout } from "./WorkPackageGeneratorLayout";

// vi.mock: WorkflowStepper is covered in its own suite; stub isolates layout chrome wiring.
vi.mock("./WorkflowStepper", () => ({
	WorkflowStepper: ({
		onStepClick,
		workflowStep,
		message,
	}: {
		workflowStep: number;
		activeStep: number;
		onStepClick: (step: number) => void;
		message: string;
		orientation: string;
	}) => (
		<div data-testid="workflow-stepper">
			<button type="button" onClick={() => onStepClick(0)}>
				step-0
			</button>
			<span>{workflowStep}</span>
			<span>{message}</span>
		</div>
	),
}));

// vi.mock: ErrorAlert is covered in its own suite; stub verifies dismiss wiring only.
vi.mock("./ErrorAlert", () => ({
	ErrorAlert: ({
		message,
		onDismiss,
	}: {
		message: string;
		onDismiss: () => void;
	}) => (
		<div role="alert">
			{message}
			<button type="button" onClick={onDismiss}>
				dismiss
			</button>
		</div>
	),
}));

const defaults = {
	isMobileLayout: false,
	error: null,
	onDismissError: vi.fn(),
	isAnyLoading: false,
	loadingLabel: "Loading…",
	stepperWorkflowStep: 0,
	activeStep: 0 as const,
	onStepClick: vi.fn(),
	stepperMessage: "step msg",
};

describe("WorkPackageGeneratorLayout", () => {
	it("renders children", () => {
		render(
			<WorkPackageGeneratorLayout {...defaults}>
				<div>Content</div>
			</WorkPackageGeneratorLayout>
		);

		expect(screen.getByText("Content")).toBeInTheDocument();
	});

	it("shows error alert when error is provided", () => {
		render(
			<WorkPackageGeneratorLayout {...defaults} error="Something went wrong">
				<div />
			</WorkPackageGeneratorLayout>
		);

		expect(screen.getByRole("alert")).toHaveTextContent("Something went wrong");
	});

	it("calls onDismissError when error is dismissed", () => {
		const onDismissError = vi.fn();
		render(
			<WorkPackageGeneratorLayout
				{...defaults}
				error="Error"
				onDismissError={onDismissError}
			>
				<div />
			</WorkPackageGeneratorLayout>
		);

		fireEvent.click(screen.getByText("dismiss"));
		expect(onDismissError).toHaveBeenCalledTimes(1);
	});

	it("hides error alert when error is null", () => {
		render(
			<WorkPackageGeneratorLayout {...defaults} error={null}>
				<div />
			</WorkPackageGeneratorLayout>
		);

		expect(screen.queryByRole("alert")).not.toBeInTheDocument();
	});

	it("shows loading bar and label when isAnyLoading is true", () => {
		render(
			<WorkPackageGeneratorLayout
				{...defaults}
				isAnyLoading
				loadingLabel="Saving canvas..."
			>
				<div />
			</WorkPackageGeneratorLayout>
		);

		expect(screen.getByText("Saving canvas...")).toBeInTheDocument();
		expect(screen.getByRole("status")).toBeInTheDocument();
	});

	it("hides loading bar when isAnyLoading is false", () => {
		render(
			<WorkPackageGeneratorLayout {...defaults} isAnyLoading={false}>
				<div />
			</WorkPackageGeneratorLayout>
		);

		expect(screen.queryByRole("status")).not.toBeInTheDocument();
	});

	it("renders stepper and wires onStepClick", () => {
		const onStepClick = vi.fn();
		render(
			<WorkPackageGeneratorLayout {...defaults} onStepClick={onStepClick}>
				<div />
			</WorkPackageGeneratorLayout>
		);

		fireEvent.click(screen.getByText("step-0"));
		expect(onStepClick).toHaveBeenCalledWith(0);
	});

	it("applies mobile-specific styles when isMobileLayout is true", () => {
		const { container } = render(
			<WorkPackageGeneratorLayout {...defaults} isMobileLayout>
				<div />
			</WorkPackageGeneratorLayout>
		);

		expect(container.firstChild).toHaveClass("flex");
		expect(container.querySelector(".px-4")).toBeTruthy();
	});

	it("applies desktop-specific styles when isMobileLayout is false", () => {
		const { container } = render(
			<WorkPackageGeneratorLayout {...defaults} isMobileLayout={false}>
				<div />
			</WorkPackageGeneratorLayout>
		);

		expect(container.querySelector(".px-6")).toBeTruthy();
	});
});
