import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactElement } from "react";
import { describe, expect, it, vi } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import { AppConfigProvider } from "@/app-config";
import { WorkflowStepper, workflowStepToIndex } from "./WorkflowStepper";

describe("workflowStepToIndex", () => {
	it.each([
		[0, 0],
		[1, 0],
		[2, 1],
		[3, 1],
		[4, 2],
		[5, 3],
		[6, 4],
		[99, 4],
	] as const)("maps workflow step %s to index %s", (step, idx) => {
		expect(workflowStepToIndex(step)).toBe(idx);
	});
});

function renderStepper(ui: ReactElement) {
	return render(<AppConfigProvider>{ui}</AppConfigProvider>);
}

describe("WorkflowStepper", () => {
	it("renders all step labels", () => {
		renderStepper(<WorkflowStepper workflowStep={0} />);
		for (const label of createMockAppConfig().ui.stepperLabels) {
			expect(screen.getByText(label)).toBeInTheDocument();
		}
	});

	it("shows message when provided", () => {
		renderStepper(
			<WorkflowStepper workflowStep={2} message="Next: pick a template" />
		);
		expect(screen.getByText("Next: pick a template")).toBeInTheDocument();
	});

	it("calls onStepClick with index", () => {
		const onStepClick = vi.fn();
		renderStepper(
			<WorkflowStepper workflowStep={4} onStepClick={onStepClick} />
		);
		fireEvent.click(screen.getByRole("button", { name: /Template/i }));
		expect(onStepClick).toHaveBeenCalledWith(1);
	});

	it("uses activeStep override in vertical mode for styling", () => {
		const { container } = renderStepper(
			<WorkflowStepper
				workflowStep={6}
				activeStep={2}
				orientation="vertical"
				message="msg"
			/>
		);
		expect(container.querySelector("ol")).toBeInTheDocument();
		expect(screen.getByText("msg")).toBeInTheDocument();
	});

	it("covers horizontal completed and pending states across workflow steps", () => {
		const { rerender } = renderStepper(<WorkflowStepper workflowStep={1} />);
		expect(screen.getAllByRole("button").length).toBe(5);
		rerender(
			<AppConfigProvider>
				<WorkflowStepper workflowStep={4} />
			</AppConfigProvider>
		);
		rerender(
			<AppConfigProvider>
				<WorkflowStepper workflowStep={6} />
			</AppConfigProvider>
		);
	});
});
