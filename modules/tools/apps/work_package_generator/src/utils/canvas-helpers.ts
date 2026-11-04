/**
 * Canvas Helper Utilities
 * Utilities for building canvas node and edge structures
 */

import type { EdgeWrite, NodeWrite, PropertyValueGroupV3 } from "@cognite/sdk";
import { type AppConfig, resolveAppConfig } from "../app-config";
import type { CanvasConfig, CanvasItem } from "../types";
import { generateUuid } from "./uuid";

/**
 * Generate properties for a canvas item
 */
export function generateCanvasItemProperties(
	item: CanvasItem | null | undefined,
	nodeId: string,
	config?: AppConfig
): Record<string, unknown> {
	if (!item) return {};
	const dimensions = resolveAppConfig(config).canvas.dimensions;

	if (item.type === "fdmInstance") {
		return {
			id: nodeId,
			containerReferenceType: item.type,
			instanceExternalId: item.instanceExternalId,
			instanceSpace: item.instanceSpace,
			viewExternalId: item.viewExternalId,
			viewSpace: item.viewSpace,
			viewVersion: item.viewVersion,
			label: item.label,
			properties: item.properties || {},
			x: item.x ?? dimensions.x,
			y: item.y ?? dimensions.y,
			width: item.width ?? dimensions.width,
			height: item.height ?? dimensions.height,
			maxWidth: item.maxWidth ?? dimensions.maxWidth,
			maxHeight: item.maxHeight ?? dimensions.maxHeight,
		};
	}

	if (item.type === "CanvasAnnotation") {
		return {
			id: nodeId,
			annotationType: item.annotationType,
			properties: item.properties || {},
		};
	}

	return {};
}

/**
 * Create a canvas node structure using SDK types
 * @returns NodeWrite from @cognite/sdk or null if item is invalid
 */
export function createCanvasNode(
	canvasId: string,
	nodeInstanceId: string,
	item: CanvasItem | null | undefined,
	config: CanvasConfig
): NodeWrite | null {
	if (item == null) return null;
	const props = generateCanvasItemProperties(item, nodeInstanceId);
	if (Object.keys(props).length === 0) return null;

	const nodeExternalId = `${canvasId}_${nodeInstanceId}`;

	return {
		instanceType: "node",
		space: config.canvasSpaceInstance,
		externalId: nodeExternalId,
		sources: [
			{
				source: {
					type: "container",
					space: config.canvasSpaceCanvas,
					externalId: item.containerId,
				},
				properties: props as PropertyValueGroupV3,
			},
		],
	};
}

/**
 * Create a canvas edge structure using SDK types
 * @returns EdgeWrite from @cognite/sdk
 */
export function createCanvasEdge(
	canvasId: string,
	nodeInstanceId: string,
	nodeExternalId: string,
	item: CanvasItem,
	config: CanvasConfig
): EdgeWrite {
	return {
		instanceType: "edge",
		space: config.canvasSpaceInstance,
		externalId: `${canvasId}_${canvasId}_${nodeInstanceId}`,
		type: {
			space: config.canvasSpaceCanvas,
			externalId: `references${item.containerId}`,
		},
		startNode: {
			space: config.canvasSpaceInstance,
			externalId: canvasId,
		},
		endNode: {
			space: config.canvasSpaceInstance,
			externalId: nodeExternalId,
		},
	};
}

/**
 * Build nodes and edges from canvas items using SDK types
 * @returns Object with nodes and edges arrays using NodeWrite and EdgeWrite from @cognite/sdk
 */
export function buildCanvasNodesAndEdges(
	items: readonly (CanvasItem | null | undefined)[],
	canvasId: string,
	config: CanvasConfig
): {
	nodes: NodeWrite[];
	edges: EdgeWrite[];
} {
	const nodes: NodeWrite[] = [];
	const edges: EdgeWrite[] = [];

	for (const item of items) {
		if (!item) continue;

		const nodeInstanceId = generateUuid();
		const node = createCanvasNode(canvasId, nodeInstanceId, item, config);

		if (!node) continue;

		const nodeExternalId = node.externalId;
		const edge = createCanvasEdge(
			canvasId,
			nodeInstanceId,
			nodeExternalId,
			item,
			config
		);

		nodes.push(node);
		edges.push(edge);
	}

	return { nodes, edges };
}
