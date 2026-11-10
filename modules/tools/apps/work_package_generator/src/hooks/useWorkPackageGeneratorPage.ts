/**
 * Page-level workflow state, derived status, and handlers for WorkPackageGenerator.
 * Composes focused hooks for step navigation and canvas naming.
 */

import {
	createContext,
	type FormEvent,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { getResourceConfigs, useAppConfig } from "@/app-config";
import { useWorkPackageStore } from "@/store";
import type { WorkOrder, WorkOrderFilters } from "@/types";
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
		recentWorkOrderFilters: workOrderFilters,
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

	const handleSearchInputChange = useCallback(
		(value: string) => {
			setSearchInput(value);
			if (value.trim()) return;
			setHasSearched(false);
			resetSearch();
		},
		[resetSearch]
	);

	const handleSearch = useCallback(
		(e: FormEvent) => {
			e.preventDefault();
			const trimmed = searchInput.trim();
			if (!trimmed) return;
			resetCanvasCreation();
			if (workOrder) resetForNewWorkOrder();
			setHasSearched(true);
			searchWorkOrders(trimmed);
		},
		[
			searchInput,
			workOrder,
			resetCanvasCreation,
			resetForNewWorkOrder,
			searchWorkOrders,
		]
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

	const handleStartNewWorkOrder = useCallback(() => {
		resetCanvasCreation();
		resetForNewWorkOrder();
	}, [resetCanvasCreation, resetForNewWorkOrder]);

	const handleDismissError = useCallback(() => setError(null), [setError]);

	const filteredSearchResults = useMemo(
		() => filterWorkOrders(searchResults, workOrderFilters),
		[searchResults, workOrderFilters]
	);

	const hasUnsavedProgress = workOrder != null && canvasResult == null;
	const activeStep = workflow.activeStep;
	const onStepClick = workflow.onStepClick;
	const lastHistoryStepRef = useRef(activeStep);
	const skipNextHistoryPushRef = useRef(false);

	useEffect(() => {
		if (lastHistoryStepRef.current === activeStep) {
			skipNextHistoryPushRef.current = false;
			return;
		}
		if (skipNextHistoryPushRef.current) {
			skipNextHistoryPushRef.current = false;
			lastHistoryStepRef.current = activeStep;
			return;
		}
		window.history.pushState({ wpgStep: activeStep }, "");
		lastHistoryStepRef.current = activeStep;
	}, [activeStep]);

	useEffect(() => {
		const onPopState = (event: PopStateEvent) => {
			const step = readHistoryStep(event.state);
			if (step == null) return;
			skipNextHistoryPushRef.current = true;
			onStepClick(step);
		};
		window.addEventListener("popstate", onPopState);
		return () => window.removeEventListener("popstate", onPopState);
	}, [onStepClick]);

	useEffect(() => {
		if (!hasUnsavedProgress) return;
		const onBeforeUnload = (event: BeforeUnloadEvent) => {
			event.preventDefault();
			event.returnValue = "";
		};
		window.addEventListener("beforeunload", onBeforeUnload);
		return () => window.removeEventListener("beforeunload", onBeforeUnload);
	}, [hasUnsavedProgress]);

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
		setSearchInput: handleSearchInputChange,
		onSearch: handleSearch,
		isSearchingWorkOrders,
		isSearchFetching,
		searchResults: filteredSearchResults,
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
		recheckCanvasName: canvasForm.recheckCanvasName,
		onCreateAnotherWorkPackage: handleCreateAnotherWorkPackage,
		onChangeWorkOrder: handleStartNewWorkOrder,
	};
}

export type WorkPackageGeneratorPageViewModel = ReturnType<
	typeof useWorkPackageGeneratorPage
>;

function filterWorkOrders(
	orders: WorkOrder[],
	filters: WorkOrderFilters
): WorkOrder[] {
	return orders.filter((order) => {
		if (filters.status && order.status !== filters.status) return false;
		if (filters.priority && order.priority !== filters.priority) return false;
		const scheduledDay = order.scheduledStartTime?.slice(0, 10);
		if (filters.dateFrom || filters.dateTo) {
			if (!scheduledDay) return false;
			if (filters.dateFrom && scheduledDay < filters.dateFrom) return false;
			if (filters.dateTo && scheduledDay > filters.dateTo) return false;
		}
		return true;
	});
}

function readHistoryStep(state: unknown): 0 | 1 | 2 | 3 | 4 | null {
	if (typeof state !== "object" || state == null || !("wpgStep" in state)) {
		return null;
	}
	const step = state.wpgStep;
	if (step === 0 || step === 1 || step === 2 || step === 3 || step === 4) {
		return step;
	}
	return null;
}
