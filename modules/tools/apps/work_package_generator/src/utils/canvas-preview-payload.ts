/**
 * Maps app {@link CanvasItem} models to {@link aggregateCanvasNodes} inputs (local FDM preview).
 */

import type {
	CanvasAnnotation,
	FdmInstanceContainerReference,
} from "@dune-canvas/canvas-preview-lib";
import { type AppConfig, resolveAppConfig } from "@/app-config";
import type { CanvasItem } from "@/types";
import { generateUuid } from "@/utils/uuid";

/**
 * Convert canvas items to preview aggregate parameters.
 * Uses the file view version as fallback when item.viewVersion is not specified.
 * Annotations are assigned to the canvas instance space for consistency with canvas-helpers.ts.
 */
export function canvasItemsToPreviewAggregateParams(
	items: readonly (CanvasItem | null | undefined)[],
	config?: AppConfig
): {
	fdmContainers: FdmInstanceContainerReference[];
	annotations: CanvasAnnotation[];
} {
	const appConfig = resolveAppConfig(config);
	const fileViewVersion = appConfig.dataModel.views.file.version;
	const dimensions = appConfig.canvas.dimensions;
	const fdmContainers: FdmInstanceContainerReference[] = [];
	const annotations: CanvasAnnotation[] = [];

	for (const item of items) {
		if (!item) continue;

		if (item.type === "fdmInstance") {
			if (
				item.instanceExternalId == null ||
				item.instanceSpace == null ||
				item.viewExternalId == null ||
				item.viewSpace == null
			) {
				continue;
			}
			fdmContainers.push({
				externalId: item.instanceExternalId,
				space: item.instanceSpace,
				properties: {
					containerReferenceType: "fdmInstance",
					instanceExternalId: item.instanceExternalId,
					instanceSpace: item.instanceSpace,
					viewExternalId: item.viewExternalId,
					viewSpace: item.viewSpace,
					viewVersion: item.viewVersion ?? fileViewVersion,
					label: item.label ?? "",
					properties: item.properties ?? {},
					x: item.x ?? dimensions.x,
					y: item.y ?? dimensions.y,
					width: item.width ?? dimensions.width,
					height: item.height ?? dimensions.height,
				},
			});
			continue;
		}

		if (item.type === "CanvasAnnotation") {
			const nodeId = generateUuid();
			annotations.push({
				externalId: nodeId,
				space: appConfig.canvas.instanceSpace,
				properties: {
					id: nodeId,
					annotationType: item.annotationType ?? "",
					properties: item.properties ?? {},
				},
			});
		}
	}

	return { fdmContainers, annotations };
}
