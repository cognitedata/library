import { act, renderHook } from "@testing-library/react";
import type { FormEvent, ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import {
	cdfRuntimeQueryWrapper,
	createStubCdfClient,
} from "@/__mocks__/cdf-runtime-test-wrapper";
import { useWorkPackageStore } from "@/store";
import type { Asset, WorkOrder } from "@/types";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "@/utils/workflow-copy";
import type { WorkPackageGeneratorViewModel } from "./useWorkPackage";
import {
	defaultWorkPackageGeneratorPageDeps,
	UseWorkPackageGeneratorPageContext,
	useWorkPackageGeneratorPage,
} from "./useWorkPackageGeneratorPage";
import { UseCanvasNameFormContext } from "./work-package-generator-page/useCanvasNameForm";

const mockUseWorkPackageGenerator = vi.fn();
const mockUseFindCanvasesByName = vi.fn();
const mockUseAllTemplateSlots = vi.fn();

const CdfWrapper = cdfRuntimeQueryWrapper(
	{
		client: createStubCdfClient({
			project: "p",
			getBaseUrl: () => "https://x.cognitedata.com",
		}),
		project: "p",
		cluster: "x",
	},
	{ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } }
);

const pageDeps = {
	...defaultWorkPackageGeneratorPageDeps,
	useWorkPackageGenerator: mockUseWorkPackageGenerator,
	useAllTemplateSlots: mockUseAllTemplateSlots,
	useAiAutoTriggers: vi.fn(),
	useMobileLayout: () => false,
};

function wrapper({ children }: { children: ReactNode }) {
	return (
		<CdfWrapper>
			<UseWorkPackageGeneratorPageContext.Provider value={pageDeps}>
				<UseCanvasNameFormContext.Provider
					value={{ useFindCanvasesByName: mockUseFindCanvasesByName }}
				>
					{children}
				</UseCanvasNameFormContext.Provider>
			</UseWorkPackageGeneratorPageContext.Provider>
		</CdfWrapper>
	);
}

const searchWorkOrders = vi.fn();
const createCanvas = vi.fn();
const refetchTemplates = vi.fn();
const refetchResources = vi.fn();
const refetchTemplateFields = vi.fn();
const refetchSlots = vi.fn();
const resetSearch = vi.fn();

function buildVm(
	override: Partial<WorkPackageGeneratorViewModel> = {}
): WorkPackageGeneratorViewModel {
	const s = useWorkPackageStore.getState();
	return {
		...s,
		client: null,
		project: "p",
		cluster: "x",
		isFindingWorkOrder: false,
		searchWorkOrders,
		searchResults: [],
		isSearchingWorkOrders: false,
		searchError: null,
		resetSearch,
		selectWorkOrder: vi.fn(),
		isSelectingWorkOrder: false,
		recentWorkOrders: [],
		isLoadingRecent: false,
		isFetchingRecent: false,
		recentError: null,
		refetchRecent: vi.fn(),
		recentHasNextPage: false,
		recentHasPrevPage: false,
		goToNextRecentPage: vi.fn(),
		goToPrevRecentPage: vi.fn(),
		recentCurrentPage: 1,
		templates: [],
		isLoadingTemplates: false,
		templatesError: null,
		refetchTemplates,
		resources: [],
		isLoadingResources: false,
		resourcesError: null,
		refetchResources,
		templateFields: [],
		isLoadingFields: false,
		templateFieldsError: null,
		refetchTemplateFields,
		runAiCanvasRecommendation: vi.fn(),
		isRunningAiCanvasRecommendation: false,
		runAiSelection: vi.fn(),
		isRunningAiSelection: false,
		createCanvas,
		isCreatingCanvas: false,
		canvasResult: undefined,
		canvasError: undefined,
		resetCanvasCreation: vi.fn(),
		findWorkOrder: vi.fn(),
		...override,
	} as WorkPackageGeneratorViewModel;
}

const sampleWo: WorkOrder = {
	externalId: "wo-1",
	space: "s",
	name: "WO One",
	status: "open",
};

const sampleAsset: Asset = {
	assetExternalId: "a-1",
	space: "s",
	assetName: "Asset 1",
};

describe("useWorkPackageGeneratorPage", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		vi.useRealTimers();
		useWorkPackageStore.setState({
			...useWorkPackageStore.getState(),
			workOrder: null,
			asset: null,
			templateCanvasId: "",
			candidateTemplates: [],
			templateFields: [],
			templateFieldsLoadedFrom: null,
			aiSelections: {},
			aiSelectedCanvasId: null,
			error: null,
			config: DEFAULT_CANVAS_CONFIG,
		});
		mockUseWorkPackageGenerator.mockImplementation(() => buildVm());
		mockUseFindCanvasesByName.mockReturnValue({
			data: undefined,
			isLoading: false,
			error: null,
			refetch: vi.fn(),
		});
		mockUseAllTemplateSlots.mockReturnValue({
			slotsByTemplate: {},
			isLoading: false,
			isFetchingSlots: false,
			slotsError: false,
			firstSlotsError: null,
			refetchSlots,
		});
		refetchTemplates.mockResolvedValue({ data: [] });
		refetchResources.mockResolvedValue({ data: [] });
		refetchTemplateFields.mockResolvedValue({ data: [] });
		refetchSlots.mockResolvedValue([]);
	});

	it("shows search-to-begin message when no work order", () => {
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		expect(result.current.stepperMessage).toBe(SEARCH_WORK_ORDER_TO_BEGIN);
		expect(result.current.stepperWorkflowStep).toBe(0);
	});

	it("onGenerateWorkPackage commits start and moves to step 1", () => {
		useWorkPackageStore.setState({ workOrder: sampleWo, asset: sampleAsset });
		mockUseWorkPackageGenerator.mockImplementation(() => buildVm());
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		expect(result.current.activeStep).toBe(1);
	});

	it("onStepClick ignores non-zero steps before commit", () => {
		useWorkPackageStore.setState({ workOrder: sampleWo, asset: sampleAsset });
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onStepClick(2));
		expect(result.current.activeStep).toBe(0);
	});

	it("blocks step >= 2 until template is confirmed", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onStepClick(2));
		expect(result.current.activeStep).toBe(1);
	});

	it("blocks Create step when automation not acknowledged and ready", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
			templateFields: [
				{
					type: "rel",
					label: "x",
					viewExternalId: "v",
					viewSpace: "s",
					viewVersion: "v1",
				},
			],
			aiSelections: { x: { space: "s", externalId: "e" } },
		});
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({
				templateFields: [
					{
						type: "rel",
						label: "x",
						viewExternalId: "v",
						viewSpace: "s",
						viewVersion: "v1",
					},
				],
				aiSelections: { x: { space: "s", externalId: "e" } },
			})
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => {
			result.current.onContinueFromTemplate();
		});
		act(() => {
			result.current.onContinueFromResources();
		});
		act(() => result.current.onStepClick(4));
		expect(result.current.activeStep).not.toBe(4);
	});

	it("onContinueToCreate acknowledges automation and sets Create step", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
		});
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ templateCanvasId: "t1" })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onContinueFromResources());
		act(() => result.current.onContinueToCreate());
		expect(result.current.automationReviewAcknowledged).toBe(true);
		expect(result.current.activeStep).toBe(4);
	});

	it("onContinueFromResources advances to selections", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		expect(result.current.activeStep).toBe(2);
		act(() => result.current.onContinueFromResources());
		expect(result.current.activeStep).toBe(3);
	});

	it("onContinueFromResources runs AI selection when selections are empty", () => {
		const runAiSelection = vi.fn();
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
			aiSelections: {},
		});
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ runAiSelection })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onContinueFromResources());
		expect(runAiSelection).toHaveBeenCalledWith(undefined);
		expect(result.current.activeStep).toBe(3);
	});

	it("onContinueFromResources preserves existing selections and skips AI re-run", () => {
		const runAiSelection = vi.fn();
		const existing = {
			Slot: { space: "s", externalId: "manual-1", name: "M" },
		};
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
			aiSelections: existing,
		});
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ runAiSelection, aiSelections: existing })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onContinueFromResources());
		expect(runAiSelection).not.toHaveBeenCalled();
		expect(useWorkPackageStore.getState().aiSelections).toEqual(existing);
		expect(result.current.activeStep).toBe(3);
	});

	it("onStepClick to selections preserves existing selections and skips AI re-run", () => {
		const runAiSelection = vi.fn();
		const existing = {
			Slot: { space: "s", externalId: "manual-2", name: "M" },
		};
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
			aiSelections: existing,
		});
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ runAiSelection, aiSelections: existing })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onContinueFromResources());
		act(() => result.current.onStepClick(2));
		runAiSelection.mockClear();
		act(() => result.current.onStepClick(3));
		expect(runAiSelection).not.toHaveBeenCalled();
		expect(useWorkPackageStore.getState().aiSelections).toEqual(existing);
		expect(result.current.activeStep).toBe(3);
	});

	it("blocks selections until resources step is confirmed", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onStepClick(3));
		expect(result.current.activeStep).toBe(2);
	});

	it("onGoToResources sets active step 2", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onGoToResources());
		expect(result.current.activeStep).toBe(2);
	});

	it("onNavigateToWorkflowStep delegates to step click", () => {
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t1",
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onNavigateToWorkflowStep(2));
		expect(result.current.activeStep).toBe(2);
	});

	it("onSearch does nothing when query is whitespace", () => {
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.setSearchInput("   "));
		act(() => {
			result.current.onSearch(searchFormEvent());
		});
		expect(searchWorkOrders).not.toHaveBeenCalled();
	});

	it("onSearch resets work order when searching again with a selected order", () => {
		const resetSpy = vi.spyOn(
			useWorkPackageStore.getState(),
			"resetForNewWorkOrder"
		);
		useWorkPackageStore.setState({ workOrder: sampleWo, asset: sampleAsset });
		mockUseWorkPackageGenerator.mockImplementation(() => buildVm());
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.setSearchInput("next-query"));
		act(() => {
			result.current.onSearch(searchFormEvent());
		});
		expect(resetSpy).toHaveBeenCalled();
		expect(searchWorkOrders).toHaveBeenCalledWith("next-query");
		resetSpy.mockRestore();
	});

	it("onSelectTemplate clears fields when id changes and resets automation ack", () => {
		useWorkPackageStore.setState({
			templateCanvasId: "old",
			templateFields: [
				{
					type: "rel",
					label: "f",
					viewExternalId: "v",
					viewSpace: "s",
					viewVersion: "v1",
				},
			],
			aiSelections: { f: { space: "s", externalId: "x" } },
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onContinueFromResources());
		act(() => result.current.onContinueToCreate());
		act(() => result.current.onSelectTemplate("new-id"));
		const st = useWorkPackageStore.getState();
		expect(st.templateCanvasId).toBe("new-id");
		expect(st.templateFields).toEqual([]);
		expect(st.aiSelections).toEqual({});
	});

	it("debounces canvas name before find-by-name query updates", async () => {
		vi.useFakeTimers({ shouldAdvanceTime: true });
		useWorkPackageStore.setState({ workOrder: sampleWo });
		mockUseWorkPackageGenerator.mockImplementation(() => buildVm());
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.setCanvasName("Custom canvas name"));
		expect(mockUseFindCanvasesByName).toHaveBeenCalledWith("Canvas for WO One");
		await act(async () => {
			vi.advanceTimersByTime(500);
		});
		expect(mockUseFindCanvasesByName).toHaveBeenCalledWith(
			"Custom canvas name"
		);
		vi.useRealTimers();
	});

	it("onCreateCanvas passes canvas name to createCanvas", () => {
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.setCanvasName("My canvas"));
		act(() => result.current.onCreateCanvas());
		expect(createCanvas).toHaveBeenCalledWith({ canvasName: "My canvas" });
	});

	it("onUpdateCanvas passes existing canvas id when present", () => {
		mockUseFindCanvasesByName.mockReturnValue({
			data: [{ externalId: "existing", space: "s" }],
			isLoading: false,
			error: null,
			refetch: vi.fn(),
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.setCanvasName("N"));
		act(() => result.current.onUpdateCanvas());
		expect(createCanvas).toHaveBeenCalledWith({
			canvasName: "N",
			existingCanvasId: "existing",
		});
	});

	it("onCreateAnotherWorkPackage resets workflow to the beginning", () => {
		const resetCanvasCreation = vi.fn();
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({
				workOrder: sampleWo,
				asset: sampleAsset,
				resetCanvasCreation,
			})
		);
		useWorkPackageStore.setState({
			...useWorkPackageStore.getState(),
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "tpl-1",
			aiSelections: { Slot: { externalId: "x", space: "s", name: "X" } },
		});
		const resetSpy = vi.spyOn(
			useWorkPackageStore.getState(),
			"resetForNewWorkOrder"
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => {
			result.current.setSearchInput("pump");
			result.current.setCanvasName("Saved canvas");
			result.current.onContinueToCreate();
		});
		expect(result.current.activeStep).toBe(4);

		act(() => result.current.onCreateAnotherWorkPackage());

		expect(resetCanvasCreation).toHaveBeenCalled();
		expect(resetSearch).toHaveBeenCalled();
		expect(resetSpy).toHaveBeenCalled();
		expect(result.current.activeStep).toBe(0);
		expect(result.current.searchInput).toBe("");
		expect(result.current.canvasName).toBe("");
		expect(result.current.automationReviewAcknowledged).toBe(false);
		resetSpy.mockRestore();
	});

	it("maps search error to user-facing message", () => {
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ searchError: new Error("boom") })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		expect(result.current.searchErrorMessage).toBeTruthy();
	});

	it("uses loadingLabel from loadingMessage when set", () => {
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ loadingMessage: "Custom load…", isLoading: true })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		expect(result.current.loadingLabel).toBe("Custom load…");
	});

	it("falls back to contextual loading labels", () => {
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({
				loadingMessage: "",
				isFindingWorkOrder: true,
			})
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		expect(result.current.loadingLabel).toBe("Finding work order...");
	});

	it("selectedTemplateName prefers template name over id", () => {
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({
				templates: [
					{
						externalId: "tid",
						space: "s",
						name: "Nice name",
						visibility: "public",
					},
				],
			})
		);
		useWorkPackageStore.setState({ templateCanvasId: "tid" });
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		expect(result.current.selectedTemplateName).toBe("Nice name");
	});

	it("workflow message when canvas saved with failed validation", () => {
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({
				workOrder: sampleWo,
				asset: sampleAsset,
				templateCanvasId: "t",
				templateFields: [],
				canvasResult: {
					canvasId: "c",
					canvasLink: "https://x",
					validation: {
						success: false,
						errors: [],
						canvasExists: true,
						itemsCount: 0,
						expectedItemsCount: 1,
						itemsMatchExpected: false,
						canvasId: "c",
					},
					isUpdate: false,
				},
			})
		);
		useWorkPackageStore.setState({
			workOrder: sampleWo,
			asset: sampleAsset,
			templateCanvasId: "t",
		});
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onGenerateWorkPackage());
		act(() => result.current.onContinueFromTemplate());
		act(() => result.current.onContinueFromResources());
		expect(result.current.stepperMessage).toContain("see a note");
	});

	it("calls retry helpers", () => {
		mockUseWorkPackageGenerator.mockImplementation(() => buildVm());
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onRetrySearch());
		act(() => result.current.onRetryTemplates());
		act(() => result.current.onRetryResources());
		act(() => result.current.onRetryTemplateFields());
		act(() => result.current.onRetrySlots());
		act(() => result.current.onRetryExistingCanvasLookup());
		expect(resetSearch).toHaveBeenCalled();
		expect(refetchTemplates).toHaveBeenCalled();
		expect(refetchResources).toHaveBeenCalled();
		expect(refetchTemplateFields).toHaveBeenCalled();
		expect(refetchSlots).toHaveBeenCalled();
	});

	it("onDismissError clears store error", () => {
		useWorkPackageStore.setState({ error: "e" });
		mockUseWorkPackageGenerator.mockImplementation(() =>
			buildVm({ error: "e" })
		);
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onDismissError());
		expect(useWorkPackageStore.getState().error).toBeNull();
	});

	it("onRemoveResource drops the candidate from the store", () => {
		useWorkPackageStore
			.getState()
			.setCandidatesGeneric("file", [
				{ externalId: "f1", space: "s", properties: {} },
			]);
		mockUseWorkPackageGenerator.mockImplementation(() => buildVm());
		const { result } = renderHook(() => useWorkPackageGeneratorPage(), {
			wrapper,
		});
		act(() => result.current.onRemoveResource("file", "f1"));
		expect(useWorkPackageStore.getState().candidatesByType.file).toEqual([]);
	});
});

function searchFormEvent(): FormEvent {
	const event: Partial<FormEvent> = { preventDefault: vi.fn() };
	return event as FormEvent;
}
