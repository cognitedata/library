/**
 * Work Package Generator Store
 * Zustand store for centralized state management
 */

import { create } from "zustand";
import { devtools } from "zustand/middleware";
import type {
	AiSelections,
	Asset,
	CanvasConfig,
	ResourceInstance,
	Selection,
	TemplateCanvas,
	TemplateField,
	WorkflowStep,
	WorkOrder,
} from "./types";

// ============================================================================
// Store State Interface
// ============================================================================

interface WorkPackageState {
	workOrder: WorkOrder | null;
	asset: Asset | null;

	candidatesByType: Record<string, ResourceInstance[]>;
	removedCandidateIdsByType: Record<string, string[]>;
	cachedAssetId: string | null;

	templateCanvasId: string;
	candidateTemplates: TemplateCanvas[];
	templateFields: TemplateField[];
	templateFieldsLoadedFrom: string | null;

	aiSelections: AiSelections;
	aiSelectedCanvasId: string | null;

	config: CanvasConfig;

	currentStep: WorkflowStep;
	isLoading: boolean;
	loadingMessage: string;
	error: string | null;
}

// ============================================================================
// Store Actions Interface
// ============================================================================

interface WorkPackageActions {
	setWorkOrder: (workOrder: WorkOrder | null) => void;
	setAsset: (asset: Asset | null) => void;

	setCandidatesGeneric: (typeKey: string, items: ResourceInstance[]) => void;
	addCandidateToType: (typeKey: string, item: ResourceInstance) => void;
	removeCandidateFromType: (typeKey: string, externalId: string) => void;
	setCachedAssetId: (id: string | null) => void;

	setTemplateCanvasId: (id: string) => void;
	setCandidateTemplates: (templates: TemplateCanvas[]) => void;

	setTemplateFields: (fields: TemplateField[]) => void;
	setTemplateFieldsLoadedFrom: (templateId: string | null) => void;

	setAiSelections: (selections: AiSelections) => void;
	setSlotSelection: (slotLabel: string, selection: Selection | null) => void;
	setAiSelectedCanvasId: (id: string | null) => void;
	clearAiSelections: () => void;

	setCurrentStep: (step: WorkflowStep) => void;
	setLoading: (isLoading: boolean, message?: string) => void;
	setError: (error: string | null) => void;
	setCanvasConfig: (config: CanvasConfig) => void;

	resetForNewWorkOrder: () => void;
}

/** Replaced when {@link AppConfigGate} finishes loading the WPG config node. */
export const PLACEHOLDER_CANVAS_CONFIG: CanvasConfig = {
	canvasSpaceCanvas: "",
	canvasSpaceInstance: "",
	canvasContainerCanvas: "",
	canvasInstanceContainerId: "",
	canvasAnnotationContainerId: "",
};

// ============================================================================
// Initial State
// ============================================================================

const initialState: WorkPackageState = {
	workOrder: null,
	asset: null,
	candidatesByType: {},
	removedCandidateIdsByType: {},
	cachedAssetId: null,
	templateCanvasId: "",
	candidateTemplates: [],
	templateFields: [],
	templateFieldsLoadedFrom: null,
	aiSelections: {},
	aiSelectedCanvasId: null,
	config: PLACEHOLDER_CANVAS_CONFIG,
	currentStep: "work-order-selection",
	isLoading: false,
	loadingMessage: "",
	error: null,
};

// ============================================================================
// Store Creation
// ============================================================================

export const useWorkPackageStore = create<
	WorkPackageState & WorkPackageActions
>()(
	devtools(
		(set, get) => ({
			...initialState,

			setWorkOrder: (workOrder) => set({ workOrder }, false, "setWorkOrder"),
			setAsset: (asset) => set({ asset }, false, "setAsset"),

			setCandidatesGeneric: (typeKey, items) =>
				set(
					(state) => ({
						candidatesByType: {
							...state.candidatesByType,
							[typeKey]: items,
						},
					}),
					false,
					"setCandidatesGeneric"
				),

			addCandidateToType: (typeKey, item) =>
				set(
					(state) => {
						const existing = state.candidatesByType[typeKey] ?? [];
						const removedIds = (
							state.removedCandidateIdsByType?.[typeKey] ?? []
						).filter((id) => id !== item.externalId);
						const nextRemoved = {
							...state.removedCandidateIdsByType,
							[typeKey]: removedIds,
						};
						if (existing.some((r) => r.externalId === item.externalId)) {
							return { removedCandidateIdsByType: nextRemoved };
						}
						return {
							candidatesByType: {
								...state.candidatesByType,
								[typeKey]: [...existing, item],
							},
							removedCandidateIdsByType: nextRemoved,
						};
					},
					false,
					"addCandidateToType"
				),

			removeCandidateFromType: (typeKey, externalId) =>
				set(
					(state) => {
						const existing = state.candidatesByType[typeKey] ?? [];
						const removedIds = state.removedCandidateIdsByType?.[typeKey] ?? [];
						const nextRemovedIds = removedIds.includes(externalId)
							? removedIds
							: [...removedIds, externalId];
						const nextSelections = { ...state.aiSelections };
						let selectionsChanged = false;
						for (const [slot, selection] of Object.entries(nextSelections)) {
							if (selection?.externalId === externalId) {
								nextSelections[slot] = null;
								selectionsChanged = true;
							}
						}
						return {
							candidatesByType: {
								...state.candidatesByType,
								[typeKey]: existing.filter((r) => r.externalId !== externalId),
							},
							removedCandidateIdsByType: {
								...state.removedCandidateIdsByType,
								[typeKey]: nextRemovedIds,
							},
							...(selectionsChanged ? { aiSelections: nextSelections } : {}),
						};
					},
					false,
					"removeCandidateFromType"
				),

			setCachedAssetId: (id) =>
				set({ cachedAssetId: id }, false, "setCachedAssetId"),

			setTemplateCanvasId: (id) =>
				set({ templateCanvasId: id }, false, "setTemplateCanvasId"),

			setCandidateTemplates: (templates) =>
				set({ candidateTemplates: templates }, false, "setCandidateTemplates"),

			setTemplateFields: (fields) =>
				set({ templateFields: fields }, false, "setTemplateFields"),

			setTemplateFieldsLoadedFrom: (templateId) =>
				set(
					{ templateFieldsLoadedFrom: templateId },
					false,
					"setTemplateFieldsLoadedFrom"
				),

			setAiSelections: (selections) =>
				set({ aiSelections: selections }, false, "setAiSelections"),

			setSlotSelection: (slotLabel, selection) =>
				set(
					(state) => ({
						aiSelections: {
							...state.aiSelections,
							[slotLabel]: selection,
						},
					}),
					false,
					"setSlotSelection"
				),

			setAiSelectedCanvasId: (id) =>
				set({ aiSelectedCanvasId: id }, false, "setAiSelectedCanvasId"),

			clearAiSelections: () =>
				set(
					{ aiSelections: {}, aiSelectedCanvasId: null },
					false,
					"clearAiSelections"
				),

			setCurrentStep: (step) =>
				set({ currentStep: step }, false, "setCurrentStep"),

			setLoading: (isLoading, message = "") =>
				set({ isLoading, loadingMessage: message }, false, "setLoading"),

			setError: (error) => set({ error }, false, "setError"),

			setCanvasConfig: (config) => set({ config }, false, "setCanvasConfig"),

			resetForNewWorkOrder: () =>
				set(
					{
						...initialState,
						candidateTemplates: get().candidateTemplates,
						config: get().config,
					},
					false,
					"resetForNewWorkOrder"
				),
		}),
		{ name: "work-package-store" }
	)
);
