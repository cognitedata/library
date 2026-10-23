/**
 * Shared Hooks
 *
 * Reusable hooks for work package operations.
 */

export {
	type CdfRuntimeContextValue,
	useCdfRuntime,
} from "@/contexts/CdfRuntimeContext";
export { useAddResourceModalViewModel } from "./useAddResourceModalViewModel";
export { useAiAutoTriggers } from "./useAiAutoTriggers";
export { useAiCanvasRecommendation, useAiSelection } from "./useAiSelections";
export { useCanvasCreation } from "./useCanvasCreation";
export { useCanvasCreationSectionViewModel } from "./useCanvasCreationSectionViewModel";
export { useCanvasDraftPreview } from "./useCanvasDraftPreview";
export { useCdfClientInfo } from "./useClientInfo";
export { useCopyCanvasLink } from "./useCopyCanvasLink";
export { useMobileLayout } from "./useMobileLayout";
export { useAssetResources } from "./useResources";
export { useTemplateFields } from "./useTemplateFields";
export type { TemplateSlot } from "./useTemplates";
export {
	useAllTemplateSlots,
	useFindCanvasesByName,
	useTemplateCanvases,
} from "./useTemplates";
export {
	useFindWorkOrder,
	useRecentWorkOrders,
	useSearchWorkOrders,
	useSelectWorkOrder,
} from "./useWorkOrders";
export {
	useWorkPackageGenerator,
	type WorkPackageGeneratorViewModel,
} from "./useWorkPackage";
export {
	useWorkPackageGeneratorPage,
	type WorkPackageGeneratorPageViewModel,
} from "./useWorkPackageGeneratorPage";
