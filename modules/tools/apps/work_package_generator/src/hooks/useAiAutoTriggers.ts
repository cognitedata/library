import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { isBaseTemplateId, useAppConfig } from "@/app-config";
import type {
	AiSelections,
	Asset,
	ResourceInstance,
	TemplateField,
	WorkOrder,
} from "@/types";

interface UseAiAutoTriggersOptions {
	/** When false, AI runs are deferred (e.g. until the user confirms the work order step). */
	workflowAutomationEnabled: boolean;
	/** When false, AI slot selection is deferred until the user confirms the template step. */
	templateStepConfirmed: boolean;
	/** When false, AI slot selection is deferred until the user confirms the resources step. */
	resourcesStepConfirmed: boolean;
	workOrder: WorkOrder | null;
	asset: Asset | null;
	templates: { externalId: string }[];
	resources: Record<string, ResourceInstance[]>;
	templateCanvasId: string | null;
	templateFields: TemplateField[];
	aiSelections: AiSelections;
	aiSelectedCanvasId: string | null;
	isLoadingResources: boolean;
	isLoadingTemplates: boolean;
	isLoadingFields: boolean;
	isRunningAiCanvasRecommendation: boolean;
	isRunningAiSelection: boolean;
	runAiCanvasRecommendation: () => void;
	runAiSelection: (arg: undefined) => void;
}

const defaultAiAutoTriggerDeps = {
	useAppConfig,
	isBaseTemplateId,
};

export type UseAiAutoTriggersContextType = typeof defaultAiAutoTriggerDeps;
export const UseAiAutoTriggersContext =
	createContext<UseAiAutoTriggersContextType>(defaultAiAutoTriggerDeps);

/**
 * Automatically fires the AI canvas recommendation and AI resource selection
 * effects once the required data is ready, guarding against duplicate runs.
 */
export function useAiAutoTriggers({
	workflowAutomationEnabled,
	templateStepConfirmed,
	resourcesStepConfirmed,
	workOrder,
	asset,
	templates,
	resources,
	templateCanvasId,
	templateFields,
	aiSelections,
	aiSelectedCanvasId,
	isLoadingResources,
	isLoadingTemplates,
	isLoadingFields,
	isRunningAiCanvasRecommendation,
	isRunningAiSelection,
	runAiCanvasRecommendation,
	runAiSelection,
}: UseAiAutoTriggersOptions): void {
	const { useAppConfig: useConfig, isBaseTemplateId: isBaseTemplate } =
		useContext(UseAiAutoTriggersContext);
	const appConfig = useConfig();
	// -------------------------------------------------------------------------
	// AI canvas recommendation
	// -------------------------------------------------------------------------

	const canRequestAiRecommendation = useMemo(
		() =>
			!!(
				workflowAutomationEnabled &&
				workOrder &&
				asset &&
				templates.some((t) => !isBaseTemplate(t.externalId, appConfig)) &&
				Object.keys(resources).length > 0 &&
				!isLoadingResources &&
				!isLoadingTemplates
			),
		[
			workflowAutomationEnabled,
			workOrder,
			asset,
			templates,
			resources,
			isLoadingResources,
			isLoadingTemplates,
			appConfig,
			isBaseTemplate,
		]
	);

	const [aiRecommendationRunForWorkOrder, setAiRecommendationRunForWorkOrder] =
		useState<string | null>(null);

	useEffect(() => {
		if (
			canRequestAiRecommendation &&
			!isRunningAiCanvasRecommendation &&
			workOrder?.externalId &&
			aiRecommendationRunForWorkOrder !== workOrder.externalId &&
			!aiSelectedCanvasId
		) {
			setAiRecommendationRunForWorkOrder(workOrder.externalId);
			runAiCanvasRecommendation();
		}
	}, [
		canRequestAiRecommendation,
		isRunningAiCanvasRecommendation,
		workOrder?.externalId,
		aiRecommendationRunForWorkOrder,
		aiSelectedCanvasId,
		runAiCanvasRecommendation,
	]);

	useEffect(() => {
		if (!workOrder) setAiRecommendationRunForWorkOrder(null);
	}, [workOrder]);

	useEffect(() => {
		if (!workflowAutomationEnabled) {
			setAiRecommendationRunForWorkOrder(null);
			setAiSelectionRunForTemplate(null);
		}
	}, [workflowAutomationEnabled]);

	// -------------------------------------------------------------------------
	// AI resource selection
	// -------------------------------------------------------------------------

	const canRunAiSelection = useMemo(
		() =>
			!!(
				workflowAutomationEnabled &&
				templateStepConfirmed &&
				resourcesStepConfirmed &&
				workOrder &&
				asset &&
				templateCanvasId &&
				!isBaseTemplate(templateCanvasId, appConfig) &&
				templateFields.length > 0 &&
				Object.keys(resources).length > 0 &&
				!isLoadingResources &&
				!isLoadingFields
			),
		[
			workflowAutomationEnabled,
			templateStepConfirmed,
			resourcesStepConfirmed,
			workOrder,
			asset,
			templateCanvasId,
			templateFields.length,
			resources,
			isLoadingResources,
			isLoadingFields,
			appConfig,
			isBaseTemplate,
		]
	);

	const [aiSelectionRunForTemplate, setAiSelectionRunForTemplate] = useState<
		string | null
	>(null);

	useEffect(() => {
		if (
			canRunAiSelection &&
			!isRunningAiSelection &&
			templateCanvasId &&
			aiSelectionRunForTemplate !== templateCanvasId &&
			Object.keys(aiSelections).length === 0
		) {
			setAiSelectionRunForTemplate(templateCanvasId);
			runAiSelection(undefined);
		}
	}, [
		canRunAiSelection,
		isRunningAiSelection,
		templateCanvasId,
		aiSelectionRunForTemplate,
		aiSelections,
		runAiSelection,
	]);

	useEffect(() => {
		if (!templateCanvasId) setAiSelectionRunForTemplate(null);
	}, [templateCanvasId]);

	useEffect(() => {
		if (!templateStepConfirmed) setAiSelectionRunForTemplate(null);
	}, [templateStepConfirmed]);

	useEffect(() => {
		if (!resourcesStepConfirmed) setAiSelectionRunForTemplate(null);
	}, [resourcesStepConfirmed]);
}
