import { act, renderHook as renderHookBase } from "@testing-library/react";
import { createElement, type ReactNode, useRef, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	BASE_TEMPLATE_ID,
	DEFAULT_CANVAS_CONFIG,
} from "@/__mocks__/app-config";
import { AppConfigProvider } from "@/app-config";
import { useWorkPackageStore } from "@/store";
import type {
	AiSelections,
	Asset,
	TemplateCanvas,
	TemplateField,
	WorkflowStep,
	WorkOrder,
} from "@/types";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "@/utils/workflow-copy";
import {
	useWorkflowStepState,
	type WorkflowStepStateInput,
} from "./useWorkflowStepState";

function renderHook<Result, Props>(
	hook: (props: Props) => Result,
	options?: Parameters<typeof renderHookBase<Result, Props>>[1]
) {
	return renderHookBase(hook, {
		...options,
		wrapper: ({ children }: { children: ReactNode }) =>
			createElement(AppConfigProvider, null, children),
	});
}

const workOrder: WorkOrder = { externalId: "wo-1", space: "s", name: "WO" };
const asset: Asset = { assetExternalId: "a1", space: "s", assetName: "Asset" };
const template: TemplateCanvas = {
	externalId: "tmpl-1",
	space: "s",
	name: "template_pump",
	visibility: "public",
};
const templateFields: TemplateField[] = [{ type: "file", label: "Doc" }];

function resetStore() {
	useWorkPackageStore.setState({
		aiSelections: {},
		config: DEFAULT_CANVAS_CONFIG,
		currentStep: "work-order-selection" as WorkflowStep,
	});
}

function useHarness(
	overrides: Partial<WorkflowStepStateInput> = {}
): ReturnType<typeof useWorkflowStepState> & {
	setters: Pick<
		WorkflowStepStateInput,
		| "clearAiSelections"
		| "runAiSelection"
		| "setTemplateCanvasId"
		| "setTemplateFields"
	>;
} {
	const [templateStepConfirmed, setTemplateStepConfirmed] = useState(
		overrides.templateStepConfirmed ?? false
	);
	const [resourcesStepConfirmed, setResourcesStepConfirmed] = useState(
		overrides.resourcesStepConfirmed ?? false
	);
	const stableFns = useRef({
		clearAiSelections: overrides.clearAiSelections ?? vi.fn(),
		runAiSelection: overrides.runAiSelection ?? vi.fn(),
		setCurrentStep: overrides.setCurrentStep ?? vi.fn(),
		setTemplateCanvasId: overrides.setTemplateCanvasId ?? vi.fn(),
		setTemplateFields: overrides.setTemplateFields ?? vi.fn(),
		setTemplateFieldsLoadedFrom:
			overrides.setTemplateFieldsLoadedFrom ?? vi.fn(),
	});
	const {
		clearAiSelections,
		runAiSelection,
		setCurrentStep,
		setTemplateCanvasId,
		setTemplateFields,
		setTemplateFieldsLoadedFrom,
	} = stableFns.current;

	const state = useWorkflowStepState({
		workOrder,
		asset,
		templateCanvasId: "tmpl-1",
		aiSelections: {},
		templates: [template],
		templateFields,
		canvasResult: null,
		isFindingWorkOrder: false,
		isSelectingWorkOrder: false,
		isLoadingTemplates: false,
		isLoadingResources: false,
		isRunningAiCanvasRecommendation: false,
		isLoadingFields: false,
		isRunningAiSelection: false,
		clearAiSelections,
		runAiSelection,
		setCurrentStep,
		setTemplateCanvasId,
		setTemplateFields,
		setTemplateFieldsLoadedFrom,
		...overrides,
		templateStepConfirmed:
			overrides.templateStepConfirmed ?? templateStepConfirmed,
		setTemplateStepConfirmed,
		resourcesStepConfirmed:
			overrides.resourcesStepConfirmed ?? resourcesStepConfirmed,
		setResourcesStepConfirmed,
	});

	return {
		...state,
		setters: {
			clearAiSelections,
			runAiSelection,
			setTemplateCanvasId,
			setTemplateFields,
		},
	};
}

describe("useWorkflowStepState", () => {
	beforeEach(() => {
		resetStore();
	});

	it("reports begin guidance when no work order is selected", () => {
		const { result } = renderHook(() =>
			useHarness({ workOrder: null, asset: null })
		);
		expect(result.current.stepperMessage).toBe(SEARCH_WORK_ORDER_TO_BEGIN);
		expect(result.current.isReady).toBe(false);
	});

	it("shows finding/selecting/resolving messages before templates load", () => {
		const finding = renderHook(() =>
			useHarness({ isFindingWorkOrder: true, templateCanvasId: null })
		);
		expect(finding.result.current.stepperMessage).toBe("Finding work order...");

		const selecting = renderHook(() =>
			useHarness({ isSelectingWorkOrder: true, templateCanvasId: null })
		);
		expect(selecting.result.current.stepperMessage).toBe(
			"Loading work order..."
		);

		const resolving = renderHook(() =>
			useHarness({ asset: null, templateCanvasId: null })
		);
		expect(resolving.result.current.stepperMessage).toBe("Resolving asset...");
	});

	it("caps the stepper at work-order until the user commits to generating", () => {
		const { result } = renderHook(() => useHarness());
		expect(result.current.hasCommittedWorkPackageStart).toBe(false);
		expect(result.current.stepperWorkflowStep).toBe(1);
		expect(result.current.stepperMessage).toMatch(
			/Continue with AI suggestions/
		);

		act(() => {
			result.current.onGenerateWorkPackage();
		});
		expect(result.current.hasCommittedWorkPackageStart).toBe(true);
		expect(result.current.activeStep).toBe(1);
	});

	it("advances through template and resources confirmation gates", () => {
		const clearAiSelections = vi.fn();
		const runAiSelection = vi.fn();
		const { result } = renderHook(() =>
			useHarness({ clearAiSelections, runAiSelection })
		);

		act(() => {
			result.current.onGenerateWorkPackage();
			result.current.onContinueFromTemplate();
		});
		expect(result.current.activeStep).toBe(2);

		act(() => {
			result.current.onContinueFromResources();
		});
		expect(clearAiSelections).toHaveBeenCalled();
		expect(runAiSelection).toHaveBeenCalled();
		expect(result.current.activeStep).toBe(3);
	});

	it("does not retrigger AI selection for the base template", () => {
		const runAiSelection = vi.fn();
		const { result } = renderHook(() =>
			useHarness({
				templateCanvasId: BASE_TEMPLATE_ID,
				runAiSelection,
			})
		);
		act(() => {
			result.current.onContinueFromResources();
		});
		expect(runAiSelection).not.toHaveBeenCalled();
		expect(result.current.isReady).toBe(true);
	});

	it("blocks stepper jumps until confirmation gates are satisfied", () => {
		const { result } = renderHook(() => useHarness());
		act(() => {
			result.current.onStepClick(2);
		});
		expect(result.current.activeStep).toBe(0);

		act(() => {
			result.current.onGenerateWorkPackage();
			result.current.onStepClick(2);
		});
		expect(result.current.activeStep).toBe(1);

		act(() => {
			result.current.onStepClick(4);
		});
		expect(result.current.activeStep).toBe(1);
	});

	it("selects a new template and clears prior AI selections", () => {
		const clearAiSelections = vi.fn();
		const setTemplateCanvasId = vi.fn();
		const setTemplateFields = vi.fn();
		const aiSelections: AiSelections = {
			Doc: { externalId: "f1", space: "s", name: "F" },
		};
		const { result } = renderHook(() =>
			useHarness({
				aiSelections,
				clearAiSelections,
				setTemplateCanvasId,
				setTemplateFields,
			})
		);
		expect(result.current.selectedTemplateName).toBe("template_pump");

		act(() => {
			result.current.onSelectTemplate("tmpl-1");
		});
		expect(setTemplateCanvasId).not.toHaveBeenCalled();

		act(() => {
			result.current.onSelectTemplate("tmpl-2");
		});
		expect(setTemplateCanvasId).toHaveBeenCalledWith("tmpl-2");
		expect(clearAiSelections).toHaveBeenCalled();
		expect(setTemplateFields).toHaveBeenCalledWith([]);
	});

	it("acknowledges automation review and resets for a new package", () => {
		const { result } = renderHook(() =>
			useHarness({
				aiSelections: { Doc: { externalId: "f1", space: "s", name: "F" } },
			})
		);
		act(() => {
			result.current.onGenerateWorkPackage();
			result.current.onContinueFromTemplate();
			result.current.onContinueFromResources();
			result.current.onContinueToCreate();
		});
		expect(result.current.automationReviewAcknowledged).toBe(true);
		expect(result.current.activeStep).toBe(4);

		act(() => {
			result.current.onGoToResources();
		});
		expect(result.current.activeStep).toBe(2);

		act(() => {
			result.current.resetWorkflowForNewPackage();
		});
		expect(result.current.hasCommittedWorkPackageStart).toBe(false);
		expect(result.current.activeStep).toBe(0);
	});

	it("reports canvas success and warning messages after save", () => {
		const success = renderHook(() =>
			useHarness({
				templateStepConfirmed: true,
				resourcesStepConfirmed: true,
				aiSelections: { Doc: { externalId: "f1", space: "s", name: "F" } },
				canvasResult: {
					validation: {
						success: true,
						canvasExists: true,
						itemsCount: 1,
						expectedItemsCount: 1,
						itemsMatchExpected: true,
						canvasId: "c1",
						errors: [],
					},
				},
			})
		);
		act(() => {
			success.result.current.onGenerateWorkPackage();
			success.result.current.onContinueFromTemplate();
			success.result.current.onContinueFromResources();
		});
		expect(success.result.current.stepperMessage).toMatch(/open in Fusion/i);

		const warned = renderHook(() =>
			useHarness({
				templateStepConfirmed: true,
				resourcesStepConfirmed: true,
				aiSelections: { Doc: { externalId: "f1", space: "s", name: "F" } },
				canvasResult: {
					validation: {
						success: false,
						canvasExists: true,
						itemsCount: 1,
						expectedItemsCount: 2,
						itemsMatchExpected: false,
						canvasId: "c1",
						errors: ["note"],
					},
				},
			})
		);
		act(() => {
			warned.result.current.onGenerateWorkPackage();
			warned.result.current.onContinueFromTemplate();
			warned.result.current.onContinueFromResources();
		});
		expect(warned.result.current.stepperMessage).toMatch(/see a note/i);
	});
});
