/**
 * Workflow step status, confirmation gates, and stepper navigation handlers.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isBaseTemplateId, useAppConfig } from "@/app-config";
import {
	type WorkflowStepIndex,
	workflowStepToIndex,
} from "@/components/work-package/WorkflowStepper";
import { useWorkPackageStore } from "@/store";
import type {
	AiSelections,
	Asset,
	TemplateCanvas,
	TemplateField,
	ValidationResult,
	WorkflowStep,
	WorkOrder,
} from "@/types";
import type { WorkflowRecoveryStep } from "@/utils";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "@/utils/workflow-copy";

type CanvasResult =
	| {
			validation: ValidationResult;
	  }
	| null
	| undefined;

export type WorkflowStepStateInput = {
	templateStepConfirmed: boolean;
	setTemplateStepConfirmed: (value: boolean) => void;
	resourcesStepConfirmed: boolean;
	setResourcesStepConfirmed: (value: boolean) => void;
	workOrder: WorkOrder | null;
	asset: Asset | null;
	templateCanvasId: string | null;
	aiSelections: AiSelections;
	templates: TemplateCanvas[];
	templateFields: TemplateField[];
	canvasResult: CanvasResult;
	isFindingWorkOrder: boolean;
	isSelectingWorkOrder: boolean;
	isLoadingTemplates: boolean;
	isLoadingResources: boolean;
	isRunningAiCanvasRecommendation: boolean;
	isLoadingFields: boolean;
	isRunningAiSelection: boolean;
	clearAiSelections: () => void;
	runAiSelection: (override?: undefined) => void;
	setCurrentStep: (step: WorkflowStep) => void;
	setTemplateCanvasId: (id: string) => void;
	setTemplateFields: (fields: TemplateField[]) => void;
	setTemplateFieldsLoadedFrom: (id: string | null) => void;
};

export function useWorkflowStepState({
	templateStepConfirmed,
	setTemplateStepConfirmed,
	resourcesStepConfirmed,
	setResourcesStepConfirmed,
	workOrder,
	asset,
	templateCanvasId,
	aiSelections,
	templates,
	templateFields,
	canvasResult,
	isFindingWorkOrder,
	isSelectingWorkOrder,
	isLoadingTemplates,
	isLoadingResources,
	isRunningAiCanvasRecommendation,
	isLoadingFields,
	isRunningAiSelection,
	clearAiSelections,
	runAiSelection,
	setCurrentStep,
	setTemplateCanvasId,
	setTemplateFields,
	setTemplateFieldsLoadedFrom,
}: WorkflowStepStateInput) {
	const appConfig = useAppConfig();
	const workflowStatus = useMemo(() => {
		if (!workOrder) return { step: 0, message: SEARCH_WORK_ORDER_TO_BEGIN };
		if (isFindingWorkOrder || isSelectingWorkOrder)
			return {
				step: 1,
				message: isSelectingWorkOrder
					? "Loading work order..."
					: "Finding work order...",
			};
		if (!asset) return { step: 1, message: "Resolving asset..." };
		if (isLoadingTemplates || isLoadingResources)
			return { step: 2, message: "Loading templates and resources..." };
		if (isRunningAiCanvasRecommendation)
			return { step: 3, message: "AI selecting best template..." };
		if (!templateCanvasId)
			return { step: 3, message: "Waiting for template selection..." };
		if (!templateStepConfirmed)
			return {
				step: 3,
				message: "Review your template, then click Continue to resources.",
			};
		if (!resourcesStepConfirmed)
			return {
				step: 4,
				message: "Review resources, then click Continue to selections.",
			};
		if (isLoadingFields)
			return { step: 5, message: "Loading template fields..." };
		if (isRunningAiSelection)
			return { step: 5, message: "AI selecting resources..." };
		if (Object.keys(aiSelections).length === 0 && templateFields.length > 0)
			return { step: 5, message: "Waiting for AI selection..." };
		if (canvasResult) {
			if (canvasResult.validation.success) {
				return {
					step: 6,
					message:
						"Canvas saved — open in Fusion or save again after you change selections.",
				};
			}
			return {
				step: 6,
				message: "Canvas saved — see a note on the Create step for details.",
			};
		}
		return { step: 6, message: "Review preview, then save to Fusion." };
	}, [
		workOrder,
		isFindingWorkOrder,
		isSelectingWorkOrder,
		asset,
		isLoadingTemplates,
		isLoadingResources,
		isRunningAiCanvasRecommendation,
		templateCanvasId,
		templateStepConfirmed,
		resourcesStepConfirmed,
		isLoadingFields,
		isRunningAiSelection,
		aiSelections,
		templateFields,
		canvasResult,
	]);

	const [activeStep, setActiveStep] = useState<WorkflowStepIndex>(0);
	const [hasCommittedWorkPackageStart, setHasCommittedWorkPackageStart] =
		useState(false);
	const [automationReviewAcknowledged, setAutomationReviewAcknowledged] =
		useState(false);

	const workOrderKey = workOrder
		? `${workOrder.space}/${workOrder.externalId}`
		: null;

	// biome-ignore lint/correctness/useExhaustiveDependencies: reset when selected work order identity (workOrderKey) changes
	useEffect(() => {
		setHasCommittedWorkPackageStart(false);
		setTemplateStepConfirmed(false);
		setResourcesStepConfirmed(false);
		setAutomationReviewAcknowledged(false);
		setActiveStep(0);
		setCurrentStep("work-order-selection");
	}, [
		workOrderKey,
		setCurrentStep,
		setTemplateStepConfirmed,
		setResourcesStepConfirmed,
	]);

	const currentStepIndex = useMemo(
		() => workflowStepToIndex(workflowStatus.step),
		[workflowStatus.step]
	);
	const lastAutoStepRef = useRef(currentStepIndex);

	useEffect(() => {
		if (!hasCommittedWorkPackageStart) {
			lastAutoStepRef.current = currentStepIndex;
			return;
		}

		const previousStepIndex = lastAutoStepRef.current;
		const isMovingForward = currentStepIndex > previousStepIndex;

		let allowedStep: WorkflowStepIndex = currentStepIndex;

		if (!templateStepConfirmed && currentStepIndex >= 2) {
			allowedStep = 1;
		} else if (!resourcesStepConfirmed && currentStepIndex >= 3) {
			allowedStep = 2;
		} else if (!automationReviewAcknowledged && currentStepIndex >= 4) {
			allowedStep = 3;
		}

		if (isMovingForward || activeStep > allowedStep) {
			setActiveStep(allowedStep);
		}

		lastAutoStepRef.current = currentStepIndex;
	}, [
		currentStepIndex,
		hasCommittedWorkPackageStart,
		templateStepConfirmed,
		resourcesStepConfirmed,
		automationReviewAcknowledged,
		activeStep,
	]);

	const isReady = useMemo(
		() =>
			!!(
				workOrder &&
				asset &&
				templateCanvasId &&
				(isBaseTemplateId(templateCanvasId, appConfig) ||
					templateFields.length === 0 ||
					Object.keys(aiSelections).length > 0)
			),
		[
			workOrder,
			asset,
			templateCanvasId,
			templateFields.length,
			aiSelections,
			appConfig,
		]
	);

	const selectedTemplateName = useMemo(() => {
		if (!templateCanvasId) return null;
		return (
			templates.find((t) => t.externalId === templateCanvasId)?.name ??
			templateCanvasId
		);
	}, [templates, templateCanvasId]);

	const handleStepClick = useCallback(
		(step: WorkflowStepIndex) => {
			if (!hasCommittedWorkPackageStart && step !== 0) return;
			if (step === 4 && (!automationReviewAcknowledged || !isReady)) return;
			if (step >= 2 && !templateStepConfirmed) return;
			if (step >= 3 && !resourcesStepConfirmed) return;

			if (
				step === 3 &&
				activeStep === 2 &&
				templateStepConfirmed &&
				!isBaseTemplateId(templateCanvasId, appConfig) &&
				Object.keys(useWorkPackageStore.getState().aiSelections).length === 0
			) {
				clearAiSelections();
				runAiSelection(undefined);
			}

			setActiveStep(step);
		},
		[
			hasCommittedWorkPackageStart,
			automationReviewAcknowledged,
			isReady,
			templateStepConfirmed,
			resourcesStepConfirmed,
			activeStep,
			templateCanvasId,
			clearAiSelections,
			runAiSelection,
			appConfig,
		]
	);

	const handleGenerateWorkPackage = useCallback(() => {
		setHasCommittedWorkPackageStart(true);
		setTemplateStepConfirmed(false);
		setResourcesStepConfirmed(false);
		setAutomationReviewAcknowledged(false);
		setActiveStep(1);
	}, [setTemplateStepConfirmed, setResourcesStepConfirmed]);

	const handleContinueToCreate = useCallback(() => {
		setAutomationReviewAcknowledged(true);
		setActiveStep(4);
	}, []);

	const handleGoToResources = useCallback(() => {
		setActiveStep(2);
	}, []);

	const handleGoToTemplate = useCallback(() => {
		handleStepClick(1);
	}, [handleStepClick]);

	const handleContinueFromTemplate = useCallback(() => {
		setTemplateStepConfirmed(true);
		setResourcesStepConfirmed(false);
		setActiveStep(2);
	}, [setTemplateStepConfirmed, setResourcesStepConfirmed]);

	const handleContinueFromResources = useCallback(() => {
		setResourcesStepConfirmed(true);
		if (
			templateCanvasId &&
			!isBaseTemplateId(templateCanvasId, appConfig) &&
			Object.keys(useWorkPackageStore.getState().aiSelections).length === 0
		) {
			clearAiSelections();
			runAiSelection(undefined);
		}
		setActiveStep(3);
	}, [
		templateCanvasId,
		clearAiSelections,
		runAiSelection,
		setResourcesStepConfirmed,
		appConfig,
	]);

	const handleNavigateToWorkflowStep = useCallback(
		(step: WorkflowRecoveryStep) => {
			handleStepClick(step as WorkflowStepIndex);
		},
		[handleStepClick]
	);

	const handleSelectTemplate = useCallback(
		(id: string) => {
			if (id === templateCanvasId) return;

			setTemplateStepConfirmed(false);
			setResourcesStepConfirmed(false);
			setTemplateFields([]);
			setTemplateFieldsLoadedFrom(null);
			setTemplateCanvasId(id);
			if (Object.keys(aiSelections).length > 0) clearAiSelections();
			setAutomationReviewAcknowledged(false);
		},
		[
			templateCanvasId,
			setTemplateCanvasId,
			setTemplateFields,
			setTemplateFieldsLoadedFrom,
			aiSelections,
			clearAiSelections,
			setTemplateStepConfirmed,
			setResourcesStepConfirmed,
		]
	);

	const resetWorkflowForNewPackage = useCallback(() => {
		setTemplateStepConfirmed(false);
		setResourcesStepConfirmed(false);
		setAutomationReviewAcknowledged(false);
		setHasCommittedWorkPackageStart(false);
		setActiveStep(0);
	}, [setTemplateStepConfirmed, setResourcesStepConfirmed]);

	const stepperWorkflowStep = useMemo(() => {
		if (workOrder && !hasCommittedWorkPackageStart) {
			return Math.min(workflowStatus.step, 1);
		}
		return workflowStatus.step;
	}, [workOrder, hasCommittedWorkPackageStart, workflowStatus.step]);

	const stepperMessage = useMemo(() => {
		if (workOrder && !hasCommittedWorkPackageStart) {
			if (isFindingWorkOrder || isSelectingWorkOrder || !asset) {
				return workflowStatus.message;
			}
			return "Review the work order and asset, then click Continue with AI suggestions to begin.";
		}
		return workflowStatus.message;
	}, [
		workOrder,
		hasCommittedWorkPackageStart,
		isFindingWorkOrder,
		isSelectingWorkOrder,
		asset,
		workflowStatus.message,
	]);

	return {
		activeStep,
		hasCommittedWorkPackageStart,
		automationReviewAcknowledged,
		isReady,
		selectedTemplateName,
		stepperWorkflowStep,
		stepperMessage,
		onStepClick: handleStepClick,
		onGenerateWorkPackage: handleGenerateWorkPackage,
		onContinueToCreate: handleContinueToCreate,
		onGoToResources: handleGoToResources,
		onGoToTemplate: handleGoToTemplate,
		onContinueFromTemplate: handleContinueFromTemplate,
		onContinueFromResources: handleContinueFromResources,
		onNavigateToWorkflowStep: handleNavigateToWorkflowStep,
		onSelectTemplate: handleSelectTemplate,
		resetWorkflowForNewPackage,
	};
}
