/**
 * Page-level workflow state, derived status, and handlers for WorkPackageGenerator.
 * Composes focused hooks for step navigation and canvas naming.
 */

import {
	createContext,
	type FormEvent,
	useCallback,
	useContext,
	useMemo,
	useState,
} from "react";
import { getResourceConfigs, useAppConfig } from "@/app-config";
import { useWorkPackageStore } from "@/store";
import type { WorkOrderFilters } from "@/types";
import { getErrorMessage, toUserFacingErrorMessage } from "@/utils/errors";
import { useAiAutoTriggers } from "./useAiAutoTriggers";
import { useMobileLayout } from "./useMobileLayout";
import { useAllTemplateSlots } from "./useTemplates";
import { useWorkPackageGenerator } from "./useWorkPackage";
import { useCanvasNameForm } from "./work-package-generator-page/useCanvasNameForm";
import { useWorkflowStepState } from "./work-package-generator-page/useWorkflowStepState";

export const defaultWorkPackageGeneratorPageDeps = {
	useAppConfig,
	useMobileLayout,
	useWorkPackageGenerator,
	useWorkPackageStore,
	useAllTemplateSlots,
	useAiAutoTriggers,
	useCanvasNameForm,
	useWorkflowStepState,
	getResourceConfigs,
};

export type UseWorkPackageGeneratorPageContextType =
	typeof defaultWorkPackageGeneratorPageDeps;
export const UseWorkPackageGeneratorPageContext =
	createContext<UseWorkPackageGeneratorPageContextType>(
		defaultWorkPackageGeneratorPageDeps
	);

export function useWorkPackageGeneratorPage() {
	const {
		useAppConfig: useConfig,
		useMobileLayout: useIsMobile,
		useWorkPackageGenerator: useWorkPackage,
		useWorkPackageStore: useStore,
		useAllTemplateSlots: useSlots,
		useAiAutoTriggers: useAutoTriggers,
		useCanvasNameForm: useCanvasForm,
		useWorkflowStepState: useWorkflow,
		getResourceConfigs: getConfigs,
	} = useContext(UseWorkPackageGeneratorPageContext);
	const appConfig = useConfig();
	const isMobileLayout = useIsMobile();
	const [templateStepConfirmed, setTemplateStepConfirmed] = useState(false);
	const [resourcesStepConfirmed, setResourcesStepConfirmed] = useState(false);
	const [searchInput, setSearchInput] = useState("");
	const [hasSearched, setHasSearched] = useState(false);
	const [workOrderFilters, setWorkOrderFilters] = useState<WorkOrderFilters>(
		{}
	);

	const {
		workOrder,
		asset,
		templateCanvasId,
		aiSelections,
		error,
		aiSelectedCanvasId,
		client,
		isLoading,
		loadingMessage,
		isFindingWorkOrder,
		searchWorkOrders,
		searchResults,
		isSearchingWorkOrders,
		searchError,
		resetSearch,
		searchHasNextPage,
		searchHasPrevPage,
		goToNextSearchPage,
		goToPrevSearchPage,
		searchCurrentPage,
		isSearchFetching,
		selectWorkOrder,
		isSelectingWorkOrder,
		templates,
		isLoadingTemplates,
		refetchTemplates,
		templatesError,
		runAiCanvasRecommendation,
		isRunningAiCanvasRecommendation,
		resources,
		isLoadingResources,
		refetchResources,
		resourcesError,
		templateFields,
		isLoadingFields,
		refetchTemplateFields,
		templateFieldsError,
		runAiSelection,
		isRunningAiSelection,
		createCanvas,
		isCreatingCanvas,
		canvasResult,
		resetCanvasCreation,
		recentWorkOrders,
		isLoadingRecent,
		isFetchingRecent,
		recentError,
		refetchRecent,
		recentHasNextPage,
		recentHasPrevPage,
		goToNextRecentPage,
		goToPrevRecentPage,
		recentCurrentPage,
	} = useWorkPackage({
		templateFieldsQueryEnabled: templateStepConfirmed,
		recentWorkOrderFilters: hasSearched ? undefined : workOrderFilters,
	});

	const {
		setTemplateCanvasId,
		clearAiSelections,
		setTemplateFields,
		setTemplateFieldsLoadedFrom,
		setError,
		resetForNewWorkOrder,
		setCurrentStep,
		removeCandidateFromType,
	} = useStore();

	const {
		slotsByTemplate,
		isLoading: isLoadingSlots,
		slotsError,
		refetchSlots,
	} = useSlots(templates);

	const workflow = useWorkflow({
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
	});

	const canvasForm = useCanvasForm({
		workOrder,
		createCanvas,
	});

	useAutoTriggers({
		workflowAutomationEnabled: workflow.hasCommittedWorkPackageStart,
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
	});

	const handleSearch = useCallback(
		(e: FormEvent) => {
			e.preventDefault();
			const trimmed = searchInput.trim();
			if (!trimmed) return;
			if (workOrder) resetForNewWorkOrder();
			setHasSearched(true);
			searchWorkOrders(trimmed);
		},
		[searchInput, workOrder, resetForNewWorkOrder, searchWorkOrders]
	);

	const { resetCanvasNameForm } = canvasForm;
	const { resetWorkflowForNewPackage } = workflow;

	const handleCreateAnotherWorkPackage = useCallback(() => {
		resetCanvasCreation();
		resetSearch();
		setSearchInput("");
		setHasSearched(false);
		setWorkOrderFilters({});
		resetCanvasNameForm();
		resetWorkflowForNewPackage();
		resetForNewWorkOrder();
	}, [
		resetCanvasCreation,
		resetSearch,
		resetForNewWorkOrder,
		resetCanvasNameForm,
		resetWorkflowForNewPackage,
	]);

	const handleDismissError = useCallback(() => setError(null), [setError]);

	const handleRetrySearch = useCallback(() => {
		resetSearch();
		const trimmed = searchInput.trim();
		if (trimmed) searchWorkOrders(trimmed);
	}, [resetSearch, searchInput, searchWorkOrders]);

	const handleRetryTemplates = useCallback(() => {
		void refetchTemplates();
	}, [refetchTemplates]);

	const handleRetryResources = useCallback(() => {
		void refetchResources();
	}, [refetchResources]);

	const handleRetryTemplateFields = useCallback(() => {
		void refetchTemplateFields();
	}, [refetchTemplateFields]);

	const handleRetrySlots = useCallback(() => {
		void refetchSlots();
	}, [refetchSlots]);

	const resourceConfigs = useMemo(
		() => getConfigs(appConfig),
		[appConfig, getConfigs]
	);

	const isAnyLoading =
		isLoading ||
		isFindingWorkOrder ||
		isSearchingWorkOrders ||
		isSearchFetching ||
		isSelectingWorkOrder ||
		isLoadingTemplates ||
		isLoadingResources ||
		isLoadingSlots ||
		isRunningAiCanvasRecommendation ||
		isLoadingFields ||
		isRunningAiSelection ||
		isCreatingCanvas ||
		canvasForm.isCheckingExists;

	const loadingLabel =
		loadingMessage ||
		(isFindingWorkOrder && "Finding work order...") ||
		(isSearchingWorkOrders && "Searching work orders...") ||
		(isSelectingWorkOrder && "Loading work order...") ||
		(isLoadingTemplates && "Loading templates...") ||
		(isLoadingResources && "Loading resources...") ||
		(isLoadingSlots && "Loading template slot details...") ||
		(isRunningAiCanvasRecommendation && "AI selecting template...") ||
		(isLoadingFields && "Loading template fields...") ||
		(isRunningAiSelection && "AI selecting resources...") ||
		(isCreatingCanvas && "Creating canvas...") ||
		(canvasForm.isCheckingExists && "Checking for existing canvas...") ||
		"Loading...";

	const searchErrorMessage = searchError
		? toUserFacingErrorMessage(getErrorMessage(searchError))
		: null;
	const templatesErrorMessage = templatesError
		? toUserFacingErrorMessage(getErrorMessage(templatesError))
		: null;
	const resourcesErrorMessage = resourcesError
		? toUserFacingErrorMessage(getErrorMessage(resourcesError))
		: null;
	const templateFieldsErrorMessage = templateFieldsError
		? toUserFacingErrorMessage(getErrorMessage(templateFieldsError))
		: null;

	return {
		isMobileLayout,
		error,
		onDismissError: handleDismissError,
		isAnyLoading,
		loadingLabel,
		stepperWorkflowStep: workflow.stepperWorkflowStep,
		activeStep: workflow.activeStep,
		onStepClick: workflow.onStepClick,
		stepperMessage: workflow.stepperMessage,
		workOrder,
		asset,
		client,
		searchInput,
		setSearchInput,
		onSearch: handleSearch,
		isSearchingWorkOrders,
		isSearchFetching,
		searchResults,
		hasSearched,
		searchErrorMessage,
		onRetrySearch: handleRetrySearch,
		searchHasNextPage,
		searchHasPrevPage,
		onSearchNextPage: goToNextSearchPage,
		onSearchPrevPage: goToPrevSearchPage,
		searchCurrentPage,
		isSelectingWorkOrder,
		selectWorkOrder,
		recentWorkOrders,
		isLoadingRecent,
		isFetchingRecent,
		recentError,
		onRefetchRecent: refetchRecent,
		recentHasNextPage,
		recentHasPrevPage,
		onRecentNextPage: goToNextRecentPage,
		onRecentPrevPage: goToPrevRecentPage,
		recentCurrentPage,
		workOrderFilters,
		onWorkOrderFiltersChange: setWorkOrderFilters,
		isFindingWorkOrder,
		onGenerateWorkPackage: workflow.onGenerateWorkPackage,
		templates,
		templateCanvasId,
		aiSelectedCanvasId,
		slotsByTemplate,
		isLoadingSlots,
		slotsError,
		onRetrySlots: handleRetrySlots,
		isLoadingTemplates,
		templatesErrorMessage,
		onRetryTemplates: handleRetryTemplates,
		isRunningAiCanvasRecommendation,
		onSelectTemplate: workflow.onSelectTemplate,
		onContinueFromTemplate: workflow.onContinueFromTemplate,
		resources,
		resourceConfigs,
		isLoadingResources,
		resourcesErrorMessage,
		onRetryResources: handleRetryResources,
		onContinueFromResources: workflow.onContinueFromResources,
		onRemoveResource: removeCandidateFromType,
		templateFields,
		aiSelections,
		isRunningAiSelection,
		isLoadingFields,
		templateFieldsErrorMessage,
		onRetryTemplateFields: handleRetryTemplateFields,
		selectedTemplateName: workflow.selectedTemplateName,
		isReady: workflow.isReady,
		automationReviewAcknowledged: workflow.automationReviewAcknowledged,
		onContinueToCreate: workflow.onContinueToCreate,
		onGoToTemplate: workflow.onGoToTemplate,
		onGoToResources: workflow.onGoToResources,
		canvasName: canvasForm.canvasName,
		setCanvasName: canvasForm.setCanvasName,
		isCreatingCanvas,
		isCheckingExists: canvasForm.isCheckingExists,
		existingCanvasLookupErrorMessage:
			canvasForm.existingCanvasLookupErrorMessage,
		onRetryExistingCanvasLookup: canvasForm.onRetryExistingCanvasLookup,
		hasExistingCanvas: canvasForm.hasExistingCanvas,
		onNavigateToWorkflowStep: workflow.onNavigateToWorkflowStep,
		canvasResult,
		onCreateCanvas: canvasForm.onCreateCanvas,
		onUpdateCanvas: canvasForm.onUpdateCanvas,
		onCreateAnotherWorkPackage: handleCreateAnotherWorkPackage,
		onChangeWorkOrder: resetForNewWorkOrder,
	};
}

export type WorkPackageGeneratorPageViewModel = ReturnType<
	typeof useWorkPackageGeneratorPage
>;
