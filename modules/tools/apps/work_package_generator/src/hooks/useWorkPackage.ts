/**
 * Orchestrator hook that composes all domain-specific hooks into a single
 * convenience API for the Work Package Generator page.
 */

import { createContext, useContext } from "react";
import { useWorkPackageStore } from "../store";
import type { WorkOrderFilters } from "../types";
import { useAiCanvasRecommendation, useAiSelection } from "./useAiSelections";
import { useCanvasCreation } from "./useCanvasCreation";
import { useCdfClientInfo } from "./useClientInfo";
import { useAssetResources } from "./useResources";
import { useTemplateFields } from "./useTemplateFields";
import { useTemplateCanvases } from "./useTemplates";
import {
	useFindWorkOrder,
	useRecentWorkOrders,
	useSearchWorkOrders,
	useSelectWorkOrder,
} from "./useWorkOrders";

const defaultWorkPackageDeps = {
	useWorkPackageStore,
	useCdfClientInfo,
	useFindWorkOrder,
	useSearchWorkOrders,
	useSelectWorkOrder,
	useRecentWorkOrders,
	useTemplateCanvases,
	useAssetResources,
	useTemplateFields,
	useAiCanvasRecommendation,
	useAiSelection,
	useCanvasCreation,
};

export type UseWorkPackageContextType = typeof defaultWorkPackageDeps;
export const UseWorkPackageContext = createContext<UseWorkPackageContextType>(
	defaultWorkPackageDeps
);

export function useWorkPackageGenerator(options?: {
	/** When false, template field instances are not fetched (e.g. until the user confirms template choice). */
	templateFieldsQueryEnabled?: boolean;
	recentWorkOrderFilters?: WorkOrderFilters;
}) {
	const {
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
		useFindWorkOrder: useFind,
		useSearchWorkOrders: useSearch,
		useSelectWorkOrder: useSelect,
		useRecentWorkOrders: useRecent,
		useTemplateCanvases: useTemplates,
		useAssetResources: useResources,
		useTemplateFields: useFields,
		useAiCanvasRecommendation: useCanvasRecommendation,
		useAiSelection: useSelection,
		useCanvasCreation: useCreateCanvas,
	} = useContext(UseWorkPackageContext);
	const templateFieldsQueryEnabled =
		options?.templateFieldsQueryEnabled ?? true;
	const store = useStore();
	const { client, project, cluster } = useCdfClient();
	const findWorkOrderMutation = useFind();
	const {
		searchWorkOrders: searchWorkOrdersFn,
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
	} = useSearch();
	const selectWorkOrderMutation = useSelect();
	const {
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
	} = useRecent(options?.recentWorkOrderFilters);
	const {
		templates,
		isLoading: templatesLoading,
		error: templatesQueryError,
		refetch: refetchTemplates,
	} = useTemplates();
	const {
		resources,
		isLoading: resourcesLoading,
		error: resourcesQueryError,
		refetch: refetchResources,
	} = useResources();
	const {
		fields,
		isLoading: fieldsLoading,
		error: templateFieldsQueryError,
		refetch: refetchTemplateFields,
	} = useFields({ queryEnabled: templateFieldsQueryEnabled });
	const aiCanvasRecommendationMutation = useCanvasRecommendation();
	const aiSelectionMutation = useSelection();
	const canvasCreationMutation = useCreateCanvas();

	return {
		...store,

		client,
		project,
		cluster,

		findWorkOrder: findWorkOrderMutation.mutate,
		isFindingWorkOrder: findWorkOrderMutation.isPending,

		searchWorkOrders: searchWorkOrdersFn,
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

		selectWorkOrder: selectWorkOrderMutation.mutate,
		isSelectingWorkOrder: selectWorkOrderMutation.isPending,

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

		templates,
		isLoadingTemplates: templatesLoading,
		templatesError: templatesQueryError,
		refetchTemplates,

		resources,
		isLoadingResources: resourcesLoading,
		resourcesError: resourcesQueryError,
		refetchResources,

		templateFields: fields,
		isLoadingFields: fieldsLoading,
		templateFieldsError: templateFieldsQueryError,
		refetchTemplateFields,

		runAiCanvasRecommendation: aiCanvasRecommendationMutation.mutate,
		isRunningAiCanvasRecommendation: aiCanvasRecommendationMutation.isPending,

		runAiSelection: aiSelectionMutation.mutate,
		isRunningAiSelection: aiSelectionMutation.isPending,

		createCanvas: canvasCreationMutation.mutate,
		isCreatingCanvas: canvasCreationMutation.isPending,
		canvasResult: canvasCreationMutation.data,
		canvasError: canvasCreationMutation.error,
		resetCanvasCreation: canvasCreationMutation.reset,
	};
}

export type WorkPackageGeneratorViewModel = ReturnType<
	typeof useWorkPackageGenerator
>;
