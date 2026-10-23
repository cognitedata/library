/**
 * Shared resource-config lookup and node mapping for resource fetches.
 */

import type { ViewReference } from "@cognite/sdk";
import { type AppConfig, getResourceConfigs } from "../app-config";
import type { ResourceConfig, ResourceInstance } from "../types";
import { mapNodeToResourceInstance, toCdfNode } from "../utils/cdf-helpers";

/** Resolve one resource type from app config. */
export function resolveResourceConfig(
	resourceType: string,
	config?: AppConfig
): ResourceConfig | null {
	const resourceConfig = getResourceConfigs(config)[resourceType];
	if (!resourceConfig) {
		return null;
	}
	return resourceConfig;
}

/** Map and filter a raw SDK item list into ResourceInstance[]. */
export function itemsToResourceInstances(
	items: unknown[],
	viewId: ViewReference
): ResourceInstance[] {
	return items
		.map((item) => {
			const node = toCdfNode(item);
			return node ? mapNodeToResourceInstance(node, viewId) : null;
		})
		.filter((instance): instance is ResourceInstance => instance !== null);
}
