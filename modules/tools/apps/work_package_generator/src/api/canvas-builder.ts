/**
 * Canvas Builder Utilities
 * Transpiled from Python canvas_builder.py
 */

import type {
	CogniteClient,
	EdgeWrite,
	NodeOrEdgeDeleteRequest,
	NodeWrite,
} from "@cognite/sdk";
import {
	type AppConfig,
	canvasSchemaView,
	cdfClusterQueryHost,
	getResourceConfigs,
	isBaseTemplateId,
	resolveAppConfig,
	viewIdToSource,
} from "../app-config";
import type {
	AiSelections,
	Asset,
	CanvasConfig,
	CanvasItem,
	ResourceInstance,
	TemplateAnnotation,
	TemplateInstance,
	ValidationResult,
	WorkOrder,
} from "../types";
import { generateUuid, getErrorMessage, nowIsoMs } from "../utils";
import { buildCanvasNodesAndEdges } from "../utils/canvas-helpers";
import {
	applyAssetReplacement,
	applySelectionReplacement,
	applyWorkOrderReplacement,
	isMainAsset,
	isMainWorkOrder,
	isSelectableResourceSlot,
} from "../utils/canvas-item-helpers";
import {
	ensureWorkPackageSolutionTag,
	withWorkPackageSolutionTag,
} from "../utils/canvas-solution-tags";
import { resolveCanvasUserId } from "../utils/canvas-user";
import { toCdfNode } from "../utils/cdf-helpers";
import { chunk } from "../utils/chunk";
import { buildBaseTemplateItems } from "./canvas-builder/base-template";
import {
	getTemplateCanvasAnnotations as defaultGetTemplateCanvasAnnotations,
	getTemplateCanvasInstances as defaultGetTemplateCanvasInstances,
} from "./retrieve-data";
import {
	queryInstanceNodesPage,
	retrieveInstanceNodes,
} from "./retrieve-data/list-nodes";
import { resolveApiRuntime } from "./runtime-deps";

export type TemplateCanvasReadDeps = {
	getTemplateCanvasInstances: typeof defaultGetTemplateCanvasInstances;
	getTemplateCanvasAnnotations: typeof defaultGetTemplateCanvasAnnotations;
};

const defaultTemplateReads: TemplateCanvasReadDeps = {
	getTemplateCanvasInstances: defaultGetTemplateCanvasInstances,
	getTemplateCanvasAnnotations: defaultGetTemplateCanvasAnnotations,
};

function resolveTemplateReads(
	overrides?: Partial<TemplateCanvasReadDeps>
): TemplateCanvasReadDeps {
	return { ...defaultTemplateReads, ...overrides };
}

// Use SDK types for delete request items instead of recreating the structure
type DeleteItem = NodeOrEdgeDeleteRequest["items"][0];
type NodeDeleteItem = DeleteItem & { instanceType: "node" };
type EdgeDeleteItem = DeleteItem & { instanceType: "edge" };

// ============================================================================
// Helper Functions
// ============================================================================

// ============================================================================
// Canvas Building Functions
// ============================================================================

/**
 * Build canvas items from template with replacements
 */
export function buildCanvasItemsFromTemplate(
	templateInstances: TemplateInstance[],
	templateAnnotations: TemplateAnnotation[],
	selections: AiSelections,
	asset: Asset,
	workOrder: WorkOrder,
	config: CanvasConfig,
	appConfig?: AppConfig
): CanvasItem[] {
	const items: CanvasItem[] = [];
	const resourceConfigs = getResourceConfigs(appConfig);

	// Add annotations first
	for (const ann of templateAnnotations) {
		items.push({
			type: "CanvasAnnotation",
			containerId: config.canvasAnnotationContainerId,
			annotationType: ann.type,
			properties: ann.properties,
		});
	}

	// Process instances
	for (const inst of templateInstances) {
		const label = inst.label || "";

		const item: CanvasItem = {
			type: "fdmInstance",
			containerId: config.canvasInstanceContainerId,
			instanceExternalId: inst.instanceExternalId,
			instanceSpace: inst.instanceSpace,
			viewExternalId: inst.viewExternalId,
			viewSpace: inst.viewSpace,
			viewVersion: inst.viewVersion || "v1",
			label,
			properties: inst.properties || {},
			x: inst.x,
			y: inst.y,
			width: inst.width,
			height: inst.height,
			maxWidth: inst.maxWidth,
			maxHeight: inst.maxHeight,
		};

		// Apply replacements based on instance type
		if (isMainAsset(inst, appConfig)) {
			items.push(applyAssetReplacement(item, asset));
		} else if (isMainWorkOrder(inst, appConfig)) {
			items.push(applyWorkOrderReplacement(item, workOrder));
		} else {
			const replacement = selections[label];
			if (replacement) {
				items.push(applySelectionReplacement(item, replacement));
			} else if (!isSelectableResourceSlot(inst, appConfig, resourceConfigs)) {
				// Auto template items (not user/AI-filled slots) stay on the canvas
				items.push(item);
			}
			// Selectable slots with no resource chosen are left empty
		}
	}

	return items;
}

// ============================================================================
// Canvas CRUD Operations
// ============================================================================

/**
 * Create canvas from items
 */
export async function createCanvasFromItems(
	client: CogniteClient,
	config: CanvasConfig,
	name: string,
	items: CanvasItem[],
	appConfig?: AppConfig
): Promise<string> {
	const canvasId = generateUuid();
	await ensureWorkPackageSolutionTag(client, appConfig);
	const userId = await resolveCanvasUserId(client, appConfig);
	const timestamp = nowIsoMs();

	// Build canvas node
	const canvasNode = {
		instanceType: "node" as const,
		space: config.canvasSpaceInstance,
		externalId: canvasId,
		sources: [
			{
				source: {
					type: "container" as const,
					space: config.canvasSpaceCanvas,
					externalId: config.canvasContainerCanvas,
				},
				properties: {
					name,
					visibility: resolveAppConfig(appConfig).canvas.visibility,
					updatedAt: timestamp,
					createdBy: userId,
					updatedBy: userId,
					solutionTags: withWorkPackageSolutionTag(undefined, appConfig),
				},
			},
		],
	};

	// Build item nodes and edges
	const { nodes: itemNodes, edges } = buildCanvasNodesAndEdges(
		items,
		canvasId,
		config
	);

	const nodes = [canvasNode, ...itemNodes];

	// Use proper SDK types instead of custom type derivation
	for (const batch of chunk(
		nodes,
		resolveAppConfig(appConfig).limits.upsertBatchSize
	)) {
		await resolveApiRuntime().schedule(() =>
			client.instances.upsert({
				items: batch as NodeWrite[],
			})
		);
	}
	for (const batch of chunk(
		edges,
		resolveAppConfig(appConfig).limits.upsertBatchSize
	)) {
		await resolveApiRuntime().schedule(() =>
			client.instances.upsert({
				items: batch as EdgeWrite[],
			})
		);
	}

	return canvasId;
}

/**
 * Clean canvas child items (nodes and edges), preserving the canvas root node.
 * Uses `${canvasId}_` prefix so the root node (externalId === canvasId) is excluded.
 */
async function cleanCanvasContent(
	client: CogniteClient,
	canvasId: string,
	appConfig?: AppConfig
): Promise<void> {
	const childPrefix = `${canvasId}_`;
	const cleanupLimit = resolveAppConfig(appConfig).limits.maxCanvasCleanupItems;

	const cleanupLimitError = (phase: string) =>
		new Error(
			`[canvas-builder] Canvas "${canvasId}" has more than ${cleanupLimit} child ${phase} (cleanup limit). Aborted; canvas may be in a partial state; reduce canvas size or raise config.limits.maxCanvasCleanupItems.`
		);

	try {
		let nodeCursor: string | undefined;
		let nodesProcessed = 0;

		do {
			const nodeResponse = await queryInstanceNodesPage(client, {
				filter: {
					prefix: {
						property: ["node", "externalId"],
						value: childPrefix,
					},
				},
				limit: resolveAppConfig(appConfig).limits.instancePageSize,
				cursor: nodeCursor,
			});

			const nodeBatch = nodeResponse.items || [];
			nodesProcessed += nodeBatch.length;
			if (
				nodesProcessed >
				resolveAppConfig(appConfig).limits.maxCanvasCleanupItems
			) {
				throw cleanupLimitError("nodes");
			}

			const nodeIds = nodeBatch
				.map((item) => {
					const node = toCdfNode(item);
					if (!node) return null;
					return {
						instanceType: "node" as const,
						space: node.space,
						externalId: node.externalId,
					};
				})
				// Use SDK type instead of manually recreating delete item structure
				.filter((item): item is NodeDeleteItem => item !== null);

			for (const batch of chunk(
				nodeIds,
				resolveAppConfig(appConfig).limits.deleteBatchSize
			)) {
				await resolveApiRuntime().scheduleDelete(() =>
					client.instances.delete(batch)
				);
			}

			nodeCursor = nodeResponse.nextCursor;
			if (
				nodeCursor &&
				nodesProcessed >=
					resolveAppConfig(appConfig).limits.maxCanvasCleanupItems
			) {
				throw cleanupLimitError("nodes");
			}
		} while (nodeCursor);

		let edgeCursor: string | undefined;
		let edgesProcessed = 0;

		do {
			const edgeResponse = await resolveApiRuntime().schedule(() =>
				client.instances.query({
					with: {
						edges: {
							limit: resolveAppConfig(appConfig).limits.instancePageSize,
							edges: {
								filter: {
									prefix: {
										property: ["edge", "externalId"],
										value: childPrefix,
									},
								},
							},
						},
					},
					select: { edges: {} },
					cursors: edgeCursor ? { edges: edgeCursor } : {},
				})
			);

			const edgeBatch: unknown[] = edgeResponse.items?.edges ?? [];
			edgesProcessed += edgeBatch.length;
			if (
				edgesProcessed >
				resolveAppConfig(appConfig).limits.maxCanvasCleanupItems
			) {
				throw cleanupLimitError("edges");
			}

			const edgeIds = edgeBatch
				.map((item: unknown) => {
					const edge = toCdfNode(item);
					if (!edge) return null;
					return {
						instanceType: "edge" as const,
						space: edge.space,
						externalId: edge.externalId,
					};
				})
				.filter((item): item is EdgeDeleteItem => item !== null);

			for (const batch of chunk(
				edgeIds,
				resolveAppConfig(appConfig).limits.deleteBatchSize
			)) {
				await resolveApiRuntime().scheduleDelete(() =>
					client.instances.delete(batch)
				);
			}

			edgeCursor = edgeResponse.nextCursor?.edges;
			if (
				edgeCursor &&
				edgesProcessed >=
					resolveAppConfig(appConfig).limits.maxCanvasCleanupItems
			) {
				throw cleanupLimitError("edges");
			}
		} while (edgeCursor);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "canvas-builder",
			message: "Failed to clean up canvas content for canvas",
			context: { canvasId },
			cause: error,
		});
		throw error;
	}
}

/**
 * Create canvas from template and selections
 */
export async function createCanvasFromTemplateAndSelections(
	client: CogniteClient,
	config: CanvasConfig,
	canvasName: string,
	templateId: string,
	selections: AiSelections,
	asset: Asset,
	workOrder: WorkOrder,
	candidatesByType: Record<string, ResourceInstance[]> = {},
	appConfig?: AppConfig,
	templateReads?: Partial<TemplateCanvasReadDeps>
): Promise<string> {
	const { getTemplateCanvasInstances, getTemplateCanvasAnnotations } =
		resolveTemplateReads(templateReads);
	let items: CanvasItem[];

	if (isBaseTemplateId(templateId, appConfig)) {
		items = buildBaseTemplateItems(
			config,
			workOrder,
			asset,
			candidatesByType,
			appConfig
		);
	} else {
		const templateInstances = await getTemplateCanvasInstances(
			client,
			templateId,
			appConfig
		);
		const templateAnnotations = await getTemplateCanvasAnnotations(
			client,
			templateId,
			appConfig
		);

		if (!templateInstances.length) {
			throw new Error(
				`No template instances found for template ID: ${templateId}`
			);
		}

		items = buildCanvasItemsFromTemplate(
			templateInstances,
			templateAnnotations,
			selections,
			asset,
			workOrder,
			config,
			appConfig
		);
	}

	if (!items.length) {
		throw new Error(
			isBaseTemplateId(templateId, appConfig)
				? "No resources found to place on the canvas. Please add or search for resources first."
				: "No items generated from template. Please check template configuration."
		);
	}

	return createCanvasFromItems(client, config, canvasName, items, appConfig);
}

/**
 * Update canvas from template and selections.
 * Reuses the existing canvasId so the canvas link and root node remain stable.
 *
 * Safety: Validates all new content can be built before performing any destructive operations
 * to prevent leaving the canvas in a broken state if template data retrieval fails.
 */
export async function updateCanvasFromTemplateAndSelections(
	client: CogniteClient,
	config: CanvasConfig,
	canvasId: string,
	canvasName: string,
	templateId: string,
	selections: AiSelections,
	asset: Asset,
	workOrder: WorkOrder,
	candidatesByType: Record<string, ResourceInstance[]> = {},
	appConfig?: AppConfig,
	templateReads?: Partial<TemplateCanvasReadDeps>
): Promise<string> {
	const { getTemplateCanvasInstances, getTemplateCanvasAnnotations } =
		resolveTemplateReads(templateReads);
	let items: CanvasItem[];

	if (isBaseTemplateId(templateId, appConfig)) {
		items = buildBaseTemplateItems(
			config,
			workOrder,
			asset,
			candidatesByType,
			appConfig
		);
	} else {
		const templateInstances = await getTemplateCanvasInstances(
			client,
			templateId,
			appConfig
		);
		const templateAnnotations = await getTemplateCanvasAnnotations(
			client,
			templateId,
			appConfig
		);

		if (!templateInstances.length) {
			throw new Error(
				`No template instances found for template ID: ${templateId}`
			);
		}

		items = buildCanvasItemsFromTemplate(
			templateInstances,
			templateAnnotations,
			selections,
			asset,
			workOrder,
			config,
			appConfig
		);
	}

	if (!items.length) {
		throw new Error(
			isBaseTemplateId(templateId, appConfig)
				? "No resources found to place on the canvas. Please add or search for resources first."
				: "No items generated from template. Please check template configuration."
		);
	}

	await cleanCanvasContent(client, canvasId, appConfig);

	await ensureWorkPackageSolutionTag(client, appConfig);
	const userId = await resolveCanvasUserId(client, appConfig);
	const timestamp = nowIsoMs();

	// Update the canvas root node in-place (name + updatedAt)
	const canvasNode = {
		instanceType: "node" as const,
		space: config.canvasSpaceInstance,
		externalId: canvasId,
		sources: [
			{
				source: {
					type: "container" as const,
					space: config.canvasSpaceCanvas,
					externalId: config.canvasContainerCanvas,
				},
				properties: {
					name: canvasName,
					visibility: resolveAppConfig(appConfig).canvas.visibility,
					updatedAt: timestamp,
					updatedBy: userId,
					solutionTags: withWorkPackageSolutionTag(undefined, appConfig),
				},
			},
		],
	};

	// Build new child nodes and edges anchored to the existing canvasId
	const { nodes: itemNodes, edges } = buildCanvasNodesAndEdges(
		items,
		canvasId,
		config
	);

	const allNodes = [canvasNode, ...itemNodes];
	// Use proper SDK types instead of custom type derivation
	for (const batch of chunk(
		allNodes,
		resolveAppConfig(appConfig).limits.upsertBatchSize
	)) {
		await resolveApiRuntime().schedule(() =>
			client.instances.upsert({
				items: batch as NodeWrite[],
			})
		);
	}
	for (const batch of chunk(
		edges,
		resolveAppConfig(appConfig).limits.upsertBatchSize
	)) {
		await resolveApiRuntime().schedule(() =>
			client.instances.upsert({
				items: batch as EdgeWrite[],
			})
		);
	}

	return canvasId;
}

/**
 * Validate canvas creation by checking that the canvas node exists and
 * the correct number of child items (instances and annotations) were created.
 */
export async function validateCanvasCreation(
	client: CogniteClient,
	_config: CanvasConfig,
	canvasId: string,
	expectedItemsCount: number,
	appConfig?: AppConfig
): Promise<ValidationResult> {
	try {
		// Check canvas exists
		const canvasItems = await retrieveInstanceNodes(
			client,
			[{ space: _config.canvasSpaceInstance, externalId: canvasId }],
			[viewIdToSource(canvasSchemaView(appConfig))]
		);

		const canvasExists = canvasItems.length > 0;

		// Count all child nodes (both instances and annotations) by prefix
		// Don't filter by sources since canvas contains both CANVAS_INSTANCE_VIEW_ID and CANVAS_ANNOTATION_VIEW_ID nodes
		let itemsCount = 0;
		let itemsCursor: string | undefined;
		do {
			const itemsResponse = await queryInstanceNodesPage(client, {
				filter: {
					prefix: {
						property: ["node", "externalId"],
						value: `${canvasId}_`,
					},
				},
				limit: resolveAppConfig(appConfig).limits.instancePageSize,
				cursor: itemsCursor,
			});
			itemsCount += itemsResponse.items?.length || 0;
			itemsCursor = itemsResponse.nextCursor;
			// Mismatch is already determined; no need to scan the rest of the canvas.
			if (expectedItemsCount >= 0 && itemsCount > expectedItemsCount) {
				break;
			}
		} while (itemsCursor);

		const result: ValidationResult = {
			success: canvasExists && itemsCount === expectedItemsCount,
			canvasExists,
			itemsCount,
			expectedItemsCount,
			itemsMatchExpected: itemsCount === expectedItemsCount,
			canvasId,
			errors: [],
		};

		if (!canvasExists) {
			result.errors.push("Canvas node not found");
		}

		if (!result.itemsMatchExpected && expectedItemsCount >= 0) {
			result.errors.push(
				`Item count mismatch: expected ${expectedItemsCount}, found ${itemsCount}`
			);
		}

		return result;
	} catch (error) {
		return {
			success: false,
			canvasExists: false,
			itemsCount: 0,
			expectedItemsCount,
			itemsMatchExpected: false,
			canvasId,
			errors: [`Validation error: ${getErrorMessage(error)}`],
		};
	}
}

// ============================================================================
// Canvas Link Generation
// ============================================================================

/**
 * Generate a link to the canvas in Fusion
 */
export function generateCanvasLink(
	canvasId: string,
	project: string,
	cluster: string,
	organization: string,
	appConfig?: AppConfig
): string {
	const config = resolveAppConfig(appConfig);
	const host = `${organization}${config.fusion.hostSuffix}`;
	const clusterHost = cdfClusterQueryHost(cluster, config);
	const workspace = config.fusion.canvasWorkspace;
	return `https://${host}/${project}/industrial-canvas/canvas?canvasId=${canvasId}&cluster=${clusterHost}&workspace=${workspace}`;
}

export { extractClusterFromUrl } from "../app-config";
