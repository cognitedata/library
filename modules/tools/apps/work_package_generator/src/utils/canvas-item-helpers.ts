/**
 * Canvas Item Helper Utilities
 * Utilities for checking instance types and applying replacements to canvas items
 */

import {
	type AppConfig,
	dataModelView,
	getResourceConfigs,
} from "../app-config";
import type {
	Asset,
	CanvasItem,
	ResourceConfig,
	TemplateInstance,
	WorkOrder,
} from "../types";

function isMainInstanceOf(
	instance: TemplateInstance,
	viewExternalId: string
): boolean {
	return (
		(instance.viewExternalId || "") === viewExternalId &&
		/\bmain\b/i.test(instance.label || "")
	);
}

export function isMainAsset(
	instance: TemplateInstance,
	config?: AppConfig
): boolean {
	return (
		isMainInstanceOf(instance, dataModelView("asset", config).externalId) ||
		isMainInstanceOf(instance, dataModelView("equipment", config).externalId)
	);
}

export function isMainWorkOrder(
	instance: TemplateInstance,
	config?: AppConfig
): boolean {
	return isMainInstanceOf(
		instance,
		dataModelView("activity", config).externalId
	);
}

/**
 * True when the template instance is an AI-filled resource slot (file, time series, etc.).
 * Main asset / work order items and other auto-placed template content are excluded.
 */
export function isSelectableResourceSlot(
	instance: TemplateInstance,
	config?: AppConfig,
	resourceConfigs: Record<string, ResourceConfig> = getResourceConfigs(config)
): boolean {
	if (isMainAsset(instance, config) || isMainWorkOrder(instance, config))
		return false;
	const viewExt = instance.viewExternalId || "";
	if (!viewExt) return false;
	return Object.values(resourceConfigs).some(
		(conf) => conf.viewId.externalId === viewExt
	);
}

interface InstanceRef {
	externalId: string;
	space: string;
	label?: string;
}

/**
 * Replace the instance reference on a canvas item.
 * Only applies to fdmInstance items; returns the item unchanged otherwise.
 */
function applyInstanceReplacement(
	item: CanvasItem,
	ref: InstanceRef
): CanvasItem {
	if (item.type !== "fdmInstance") return item;
	return {
		...item,
		instanceExternalId: ref.externalId,
		instanceSpace: ref.space,
		...(ref.label !== undefined && { label: ref.label }),
	};
}

export function applyAssetReplacement(
	item: CanvasItem,
	asset: Asset
): CanvasItem {
	if (!asset.assetExternalId) return item;
	return applyInstanceReplacement(item, {
		externalId: asset.assetExternalId,
		space: asset.space,
	});
}

export function applyWorkOrderReplacement(
	item: CanvasItem,
	workOrder: WorkOrder
): CanvasItem {
	if (!workOrder.externalId) return item;
	return applyInstanceReplacement(item, {
		externalId: workOrder.externalId,
		space: workOrder.space,
	});
}

export function applySelectionReplacement(
	item: CanvasItem,
	selection: { externalId: string; space: string; name?: string }
): CanvasItem {
	return applyInstanceReplacement(item, {
		externalId: selection.externalId,
		space: selection.space,
		label: selection.name || selection.externalId,
	});
}
