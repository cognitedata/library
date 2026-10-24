import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { getResourceConfigs } from "@/__mocks__/app-config";
import type { WorkPackageGeneratorPageViewModel } from "@/hooks/useWorkPackageGeneratorPage";
import { WorkPackageGeneratorStepContent } from "./WorkPackageGeneratorStepContent";

// vi.mock: step children are covered in dedicated suites; stubs verify step routing only.
vi.mock("./WorkOrderSearchPanel", () => ({
	WorkOrderSearchPanel: () => <div data-testid="step-search" />,
}));
vi.mock("./WorkOrderDetailStep", () => ({
	WorkOrderDetailStep: () => <div data-testid="step-detail" />,
}));
vi.mock("./TemplateSelectionStep", () => ({
	TemplateSelectionStep: () => <div data-testid="step-template" />,
}));
vi.mock("./ResourcesStep", () => ({
	ResourcesStep: () => <div data-testid="step-resources" />,
}));
vi.mock("./SelectionsStep", () => ({
	SelectionsStep: () => <div data-testid="step-selections" />,
}));
vi.mock("./CanvasCreationSection", () => ({
	CanvasCreationSection: () => <div data-testid="step-create" />,
}));

const noop = () => {};
const baseVm: WorkPackageGeneratorPageViewModel = {
	isMobileLayout: false,
	error: null,
	onDismissError: noop,
	isAnyLoading: false,
	loadingLabel: "",
	stepperWorkflowStep: 0,
	activeStep: 0,
	onStepClick: noop,
	stepperMessage: "",
	workOrder: null,
	asset: null,
	client: undefined,
	searchInput: "",
	setSearchInput: noop,
	onSearch: noop,
	isSearchingWorkOrders: false,
	isSearchFetching: false,
	searchResults: [],
	hasSearched: false,
	searchErrorMessage: null,
	onRetrySearch: noop,
	searchHasNextPage: false,
	searchHasPrevPage: false,
	onSearchNextPage: noop,
	onSearchPrevPage: noop,
	searchCurrentPage: 1,
	isSelectingWorkOrder: false,
	selectWorkOrder: noop,
	recentWorkOrders: [],
	isLoadingRecent: false,
	isFetchingRecent: false,
	recentError: null,
	onRefetchRecent:
		vi.fn() as WorkPackageGeneratorPageViewModel["onRefetchRecent"],
	recentHasNextPage: false,
	recentHasPrevPage: false,
	onRecentNextPage: noop,
	onRecentPrevPage: noop,
	recentCurrentPage: 1,
	workOrderFilters: {},
	onWorkOrderFiltersChange: noop,
	isFindingWorkOrder: false,
	onGenerateWorkPackage: noop,
	templates: [],
	templateCanvasId: "",
	aiSelectedCanvasId: null,
	slotsByTemplate: {},
	isLoadingSlots: false,
	slotsError: false,
	onRetrySlots: noop,
	isLoadingTemplates: false,
	templatesErrorMessage: null,
	onRetryTemplates: noop,
	isRunningAiCanvasRecommendation: false,
	onSelectTemplate: noop,
	onContinueFromTemplate: noop,
	resources: {},
	resourceConfigs: getResourceConfigs(),
	isLoadingResources: false,
	resourcesErrorMessage: null,
	onRetryResources: noop,
	onContinueFromResources: noop,
	onRemoveResource: noop,
	templateFields: [],
	aiSelections: {},
	isRunningAiSelection: false,
	isLoadingFields: false,
	templateFieldsErrorMessage: null,
	onRetryTemplateFields: noop,
	selectedTemplateName: null,
	isReady: false,
	automationReviewAcknowledged: false,
	onContinueToCreate: noop,
	onGoToTemplate: noop,
	onGoToResources: noop,
	canvasName: "",
	setCanvasName: noop,
	isCreatingCanvas: false,
	isCheckingExists: false,
	existingCanvasLookupErrorMessage: null,
	onRetryExistingCanvasLookup: noop,
	hasExistingCanvas: false,
	onNavigateToWorkflowStep: noop,
	canvasResult: undefined,
	onCreateCanvas: noop,
	onUpdateCanvas: noop,
	onCreateAnotherWorkPackage: noop,
	onChangeWorkOrder: noop,
};

describe("WorkPackageGeneratorStepContent", () => {
	it("renders search when step 0 and no work order", () => {
		render(<WorkPackageGeneratorStepContent vm={baseVm} />);
		expect(screen.getByTestId("step-search")).toBeInTheDocument();
		expect(screen.queryByTestId("step-detail")).not.toBeInTheDocument();
	});

	it("renders detail when step 0 and work order exists", () => {
		render(
			<WorkPackageGeneratorStepContent
				vm={{
					...baseVm,
					workOrder: { externalId: "w", space: "s", name: "W" },
				}}
			/>
		);
		expect(screen.getByTestId("step-detail")).toBeInTheDocument();
		expect(screen.queryByTestId("step-search")).not.toBeInTheDocument();
	});

	it.each([1, 2, 3, 4] as const)(
		"renders step panel for activeStep %s",
		(n) => {
			const ids = [
				"",
				"step-template",
				"step-resources",
				"step-selections",
				"step-create",
			] as const;
			render(
				<WorkPackageGeneratorStepContent vm={{ ...baseVm, activeStep: n }} />
			);
			expect(screen.getByTestId(ids[n])).toBeInTheDocument();
		}
	);
});
