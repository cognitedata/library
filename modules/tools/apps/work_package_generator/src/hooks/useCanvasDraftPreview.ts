import { aggregateCanvasNodes } from "@dune-canvas/canvas-preview-lib";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import { buildCanvasItemsFromTemplate } from "@/api/canvas-builder";
import { buildBaseTemplateItems } from "@/api/canvas-builder/base-template";
import {
	getTemplateCanvasAnnotations,
	getTemplateCanvasInstances,
} from "@/api/retrieve-data";
import { isBaseTemplateId, useAppConfig } from "@/app-config";
import { useWorkPackageStore } from "@/store";
import type { AiSelections } from "@/types";
import { canvasItemsToPreviewAggregateParams } from "@/utils/canvas-preview-payload";
import { useCdfClientInfo } from "./useClientInfo";

function stableSelectionsKey(selections: AiSelections): string {
	return JSON.stringify(
		Object.entries(selections)
			.sort(([a], [b]) => a.localeCompare(b))
			.map(([label, s]) =>
				s == null ? [label, null] : [label, s.space, s.externalId]
			)
	);
}

const defaultCanvasDraftPreviewDeps = {
	aggregateCanvasNodes,
	buildCanvasItemsFromTemplate,
	buildBaseTemplateItems,
	getTemplateCanvasInstances,
	getTemplateCanvasAnnotations,
	useWorkPackageStore,
	useCdfClientInfo,
};

export type UseCanvasDraftPreviewContextType =
	typeof defaultCanvasDraftPreviewDeps;
export const UseCanvasDraftPreviewContext =
	createContext<UseCanvasDraftPreviewContextType>(
		defaultCanvasDraftPreviewDeps
	);

/**
 * Loads the selected template, applies current selections, and aggregates nodes for {@link PreviewCanvas} (read-only preview before save).
 */
export function useCanvasDraftPreview(isReady: boolean) {
	const {
		aggregateCanvasNodes: aggregateNodes,
		buildCanvasItemsFromTemplate: buildFromTemplate,
		buildBaseTemplateItems: buildBaseItems,
		getTemplateCanvasInstances: getTemplateInstances,
		getTemplateCanvasAnnotations: getTemplateAnnotations,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseCanvasDraftPreviewContext);
	const { client } = useCdfClient();
	const templateCanvasId = useStore((s) => s.templateCanvasId);
	const aiSelections = useStore((s) => s.aiSelections);
	const candidatesByType = useStore((s) => s.candidatesByType);
	const asset = useStore((s) => s.asset);
	const appConfig = useAppConfig();
	const workOrder = useStore((s) => s.workOrder);
	const config = useStore((s) => s.config);

	const isBaseTemplate = isBaseTemplateId(templateCanvasId, appConfig);

	return useQuery({
		queryKey: [
			"canvas-draft-preview",
			templateCanvasId,
			stableSelectionsKey(aiSelections),
			asset?.assetExternalId,
			asset?.space,
			workOrder?.externalId,
			workOrder?.space,
			isBaseTemplate ? JSON.stringify(candidatesByType) : null,
		] as const,
		enabled:
			isReady &&
			!!client &&
			!!templateCanvasId &&
			!!asset &&
			!!workOrder &&
			(isBaseTemplate || Object.keys(aiSelections).length > 0),
		queryFn: async () => {
			if (!client) throw new Error("CDF client not available");
			if (!asset) throw new Error("Asset not available");
			if (!workOrder) throw new Error("Work order not available");

			const items = isBaseTemplate
				? buildBaseItems(config, workOrder, asset, candidatesByType)
				: await (async () => {
						const [templateInstances, templateAnnotations] = await Promise.all([
							getTemplateInstances(client, templateCanvasId),
							getTemplateAnnotations(client, templateCanvasId),
						]);
						if (!templateInstances.length) {
							throw new Error("No template instances found for preview.");
						}
						return buildFromTemplate(
							templateInstances,
							templateAnnotations,
							aiSelections,
							asset,
							workOrder,
							config
						);
					})();
			const { fdmContainers, annotations } =
				canvasItemsToPreviewAggregateParams(items);
			return aggregateNodes({
				fdmContainers,
				annotations,
				client,
			});
		},
		staleTime: 60_000,
		placeholderData: keepPreviousData,
	});
}
