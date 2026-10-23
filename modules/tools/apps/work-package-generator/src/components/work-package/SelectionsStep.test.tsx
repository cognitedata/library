import {
	fireEvent,
	render as renderWithoutConfig,
	screen,
} from "@testing-library/react";
import type { ReactElement } from "react";
import { describe, expect, it, vi } from "vitest";
import { BASE_TEMPLATE_ID } from "@/__mocks__/app-config";
import { AppConfigProvider } from "@/app-config";
import type { ResourceInstance, TemplateField, WorkOrder } from "@/types";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "@/utils/workflow-copy";
import { SelectionsStep } from "./SelectionsStep";

function render(ui: ReactElement) {
	return renderWithoutConfig(ui, { wrapper: AppConfigProvider });
}

// vi.mock: PickResourceModal uses store context; stub isolates SelectionsStep pick-modal state.
vi.mock("./PickResourceModal", () => ({
	PickResourceModal: ({
		slotLabel,
		onClose,
	}: {
		slotLabel: string;
		onClose: () => void;
	}) => (
		<div data-testid="pick-resource-modal">
			<span>{slotLabel}</span>
			<button type="button" onClick={onClose}>
				Close pick modal
			</button>
		</div>
	),
}));

const workOrder: WorkOrder = {
	externalId: "wo-1",
	space: "s",
	name: "WO-1",
	status: "open",
};

const assetField: TemplateField = {
	type: "asset",
	label: "Main asset",
	viewExternalId: "CogniteAsset",
	viewSpace: "cdf_cdm",
	viewVersion: "v1",
};

const fileField: TemplateField = {
	type: "file",
	label: "Doc slot",
	viewExternalId: "CogniteFile",
};

const fileCandidate: ResourceInstance = {
	externalId: "f1",
	space: "s",
	name: "Doc A",
	properties: {},
};

describe("SelectionsStep", () => {
	it("shows begin guidance when no work order is selected", () => {
		render(
			<SelectionsStep
				workOrder={null}
				templateCanvasId={null}
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(screen.getByText(SEARCH_WORK_ORDER_TO_BEGIN)).toBeInTheDocument();
	});

	it("prompts to choose a template when work order exists but template does not", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId={null}
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(
			screen.getByText(/Choose a template in the Template step/i)
		).toBeInTheDocument();
	});

	it("shows base template empty-slot message", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId={BASE_TEMPLATE_ID}
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(
			screen.getByText(/places every collected resource on the canvas/i)
		).toBeInTheDocument();
	});

	it("shows loading slots state", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields
			/>
		);
		expect(
			screen.getByText(/Loading slots from your template/i)
		).toBeInTheDocument();
	});

	it("shows template fields error with retry", () => {
		const onRetry = vi.fn();
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
				templateFieldsErrorMessage="failed to load slots"
				onRetryTemplateFields={onRetry}
			/>
		);
		expect(screen.getByRole("alert")).toHaveTextContent("failed to load slots");
		fireEvent.click(screen.getByRole("button", { name: /Retry/i }));
		expect(onRetry).toHaveBeenCalledTimes(1);
	});

	it("shows no-slots message for templates without resource slots", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(
			screen.getByText(/This template has no resource slots to fill/i)
		).toBeInTheDocument();
	});

	it("shows All filled badge when every slot has a selection", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[assetField]}
				aiSelections={{
					"Main asset": {
						space: "s",
						externalId: "a1",
						name: "Asset One",
						description: "Primary",
					},
				}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(screen.getByText(/1 of 1 canvas slot filled/i)).toBeInTheDocument();
		expect(screen.getByText(/All filled/i)).toBeInTheDocument();
		expect(screen.getByText("Asset One")).toBeInTheDocument();
		expect(screen.getByText("a1")).toBeInTheDocument();
		expect(screen.getByText("Primary")).toBeInTheDocument();
	});

	it("shows Partial badge and no-match guidance for empty slots", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[assetField, fileField]}
				aiSelections={{
					"Main asset": { space: "s", externalId: "a1", name: "Asset One" },
				}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(screen.getByText(/Partial/i)).toBeInTheDocument();
		expect(screen.getByText(/No automatic match/i)).toBeInTheDocument();
	});

	it("shows selecting state while AI selection is running", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[fileField]}
				aiSelections={{}}
				isRunningAiSelection
				isLoadingFields={false}
			/>
		);
		expect(screen.getByText(/Selecting\.\.\./i)).toBeInTheDocument();
	});

	it("opens and closes PickResourceModal when candidates exist", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[fileField]}
				aiSelections={{}}
				candidatesByType={{ file: [fileCandidate] }}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(screen.queryByTestId("pick-resource-modal")).not.toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: /pick manually/i }));
		expect(screen.getByTestId("pick-resource-modal")).toHaveTextContent(
			"Doc slot"
		);
		fireEvent.click(screen.getByRole("button", { name: /Close pick modal/i }));
		expect(screen.queryByTestId("pick-resource-modal")).not.toBeInTheDocument();
	});

	it("hides pick manually when no candidates are available", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[fileField]}
				aiSelections={{}}
				candidatesByType={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
			/>
		);
		expect(
			screen.queryByRole("button", { name: /pick manually/i })
		).not.toBeInTheDocument();
	});

	it("renders automation review actions and empty-slot warning", () => {
		const onContinue = vi.fn();
		const onGoToTemplate = vi.fn();
		const onGoToResources = vi.fn();
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[assetField, fileField]}
				aiSelections={{
					"Main asset": { space: "s", externalId: "a1" },
				}}
				isRunningAiSelection={false}
				isLoadingFields={false}
				templateDisplayName="Pump Template"
				showAutomationReview
				onContinueToCreate={onContinue}
				onGoToTemplate={onGoToTemplate}
				onGoToResources={onGoToResources}
			/>
		);
		expect(
			screen.getByRole("region", {
				name: /Review AI suggestions before creating the canvas/i,
			})
		).toBeInTheDocument();
		expect(screen.getByText("Pump Template")).toBeInTheDocument();
		expect(
			screen.getByText(/Some slots have no suggested resource/i)
		).toBeInTheDocument();

		fireEvent.click(
			screen.getByRole("button", { name: /Continue to Create/i })
		);
		expect(onContinue).toHaveBeenCalledTimes(1);
		fireEvent.click(screen.getByRole("button", { name: /Edit template/i }));
		expect(onGoToTemplate).toHaveBeenCalledTimes(1);
		fireEvent.click(screen.getByRole("button", { name: /Open Resources/i }));
		expect(onGoToResources).toHaveBeenCalledTimes(1);
	});

	it("shows no-slots review copy when template has zero fields", () => {
		render(
			<SelectionsStep
				workOrder={workOrder}
				templateCanvasId="tpl-1"
				templateFields={[]}
				aiSelections={{}}
				isRunningAiSelection={false}
				isLoadingFields={false}
				showAutomationReview
				onContinueToCreate={vi.fn()}
			/>
		);
		expect(
			screen.getByText(/None on this template — you can create the canvas/i)
		).toBeInTheDocument();
	});
});
