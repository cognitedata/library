import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TemplateSelectionStep } from "./TemplateSelectionStep";
import { WorkOrderDetailStep } from "./WorkOrderDetailStep";

const template = {
	externalId: "t1",
	space: "s",
	name: "T1",
	visibility: "public" as const,
};

describe("TemplateSelectionStep", () => {
	it("renders template list and continue flow", () => {
		const onSelect = vi.fn();
		const onContinue = vi.fn();
		render(
			<TemplateSelectionStep
				templates={[template]}
				templateCanvasId=""
				aiSelectedCanvasId={null}
				slotsByTemplate={{}}
				isLoadingSlots={false}
				isLoadingTemplates={false}
				isRunningAiCanvasRecommendation={false}
				onSelectTemplate={onSelect}
				onContinueFromTemplate={onContinue}
			/>
		);
		fireEvent.click(screen.getByRole("button", { name: /T1/ }));
		expect(onSelect).toHaveBeenCalledWith("t1");
		const continueBtn = screen.getByRole("button", {
			name: /Continue to resources/i,
		});
		expect(continueBtn).toBeDisabled();
	});

	it("opens template instructions from the step header info button", () => {
		render(
			<TemplateSelectionStep
				templates={[template]}
				templateCanvasId=""
				aiSelectedCanvasId={null}
				slotsByTemplate={{}}
				isLoadingSlots={false}
				isLoadingTemplates={false}
				isRunningAiCanvasRecommendation={false}
				onSelectTemplate={vi.fn()}
				onContinueFromTemplate={vi.fn()}
			/>
		);

		fireEvent.click(
			screen.getByRole("button", {
				name: /how to create a work package template/i,
			})
		);

		expect(
			screen.getByRole("dialog", { name: /creating a template canvas/i })
		).toBeInTheDocument();
	});

	it("shows AI recommendation loading state", () => {
		render(
			<TemplateSelectionStep
				templates={[template]}
				templateCanvasId=""
				aiSelectedCanvasId={null}
				slotsByTemplate={{}}
				isLoadingSlots={false}
				isLoadingTemplates={false}
				isRunningAiCanvasRecommendation
				onSelectTemplate={vi.fn()}
				onContinueFromTemplate={vi.fn()}
			/>
		);
		expect(screen.getByText(/AI analyzing templates/i)).toBeInTheDocument();
	});

	it("shows empty template list error with retry", () => {
		const onRetry = vi.fn();
		render(
			<TemplateSelectionStep
				templates={[]}
				templateCanvasId=""
				aiSelectedCanvasId={null}
				slotsByTemplate={{}}
				isLoadingSlots={false}
				isLoadingTemplates={false}
				templatesErrorMessage="Failed to load"
				onRetryTemplates={onRetry}
				isRunningAiCanvasRecommendation={false}
				onSelectTemplate={vi.fn()}
				onContinueFromTemplate={vi.fn()}
			/>
		);
		expect(screen.getByText("Failed to load")).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: /^Retry$/i }));
		expect(onRetry).toHaveBeenCalled();
	});

	it("shows template and slots error retries", () => {
		const onRetryT = vi.fn();
		const onRetryS = vi.fn();
		render(
			<TemplateSelectionStep
				templates={[template]}
				templateCanvasId="t1"
				aiSelectedCanvasId={null}
				slotsByTemplate={{}}
				isLoadingSlots={false}
				slotsError
				onRetrySlots={onRetryS}
				isLoadingTemplates={false}
				templatesErrorMessage="err"
				onRetryTemplates={onRetryT}
				isRunningAiCanvasRecommendation={false}
				onSelectTemplate={vi.fn()}
				onContinueFromTemplate={vi.fn()}
			/>
		);
		const retryButtons = screen.getAllByRole("button", { name: /^Retry$/i });
		expect(retryButtons[0]).toBeDefined();
		fireEvent.click(retryButtons[0]);
		expect(onRetryT).toHaveBeenCalled();
		fireEvent.click(screen.getByRole("button", { name: /Retry slots/i }));
		expect(onRetryS).toHaveBeenCalled();
	});
});

describe("WorkOrderDetailStep", () => {
	it("disables generate until asset resolved", () => {
		const onGen = vi.fn();
		render(
			<WorkOrderDetailStep
				workOrder={{
					externalId: "w",
					space: "s",
					name: "W",
					status: "open",
				}}
				asset={null}
				searchInput=""
				onSearchInputChange={vi.fn()}
				onSearch={vi.fn()}
				isSearching={false}
				isClientAvailable
				onGenerateWorkPackage={onGen}
			/>
		);
		const btn = screen.getByRole("button", {
			name: /Continue with AI suggestions/i,
		});
		expect(btn).toBeDisabled();
	});

	it("calls onGenerateWorkPackage when asset present", () => {
		const onGen = vi.fn();
		render(
			<WorkOrderDetailStep
				workOrder={{
					externalId: "w",
					space: "s",
					name: "W",
					status: "open",
				}}
				asset={{
					assetExternalId: "a",
					space: "s",
					assetName: "A",
				}}
				searchInput=""
				onSearchInputChange={vi.fn()}
				onSearch={vi.fn()}
				isSearching={false}
				isClientAvailable
				onGenerateWorkPackage={onGen}
			/>
		);
		fireEvent.click(
			screen.getByRole("button", { name: /Continue with AI suggestions/i })
		);
		expect(onGen).toHaveBeenCalled();
	});
});
