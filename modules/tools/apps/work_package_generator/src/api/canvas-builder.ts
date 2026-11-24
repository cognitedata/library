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
	selectionSlotKey,
} from "../utils/canvas-item-helpers";
import {
	ensureWorkPackageSolutionTag,
	readSolutionTagsFromRootProperties,
	withWorkPackageSolutionTag,
} from "../utils/canvas-solution-tags";
import { resolveCanvasUserId } from "../utils/canvas-user";
import { extractInstanceProperties, toCdfNode } from "../utils/cdf-helpers";
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

/** Source canvas item externalId for a copied item, used to remap annotation anchors. */
const sourceItemIds = new WeakMap<CanvasItem, readonly string[]>();
/** Annotation containerId from the template, before it is rewritten to a new item id. */
const sourceAnnotationContainerIds = new WeakMap<CanvasItem, string>();

type TemplateAnnotationSource = TemplateAnnotation & {
	id?: string;
	itemId?: string;
	containerId?: string;
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

function rememberSourceItem(
	item: CanvasItem,
	...sourceIds: Array<string | undefined>
): CanvasItem {
	const ids = sourceIds
		.map((id) => id?.trim())
		.filter((id): id is string => Boolean(id));
	if (ids.length > 0) sourceItemIds.set(item, ids);
	return item;
}

function nodeProperties(
	node: NodeWrite | undefined
): Record<string, unknown> | undefined {
	const properties = node?.sources?.[0]?.properties;
	if (!properties || typeof properties !== "object") return undefined;
	return properties as Record<string, unknown>;
}

/**
 * Point copied annotations at the new item ids. Annotations whose anchor
 * (`containerId`) or connector endpoints (`fromId` / `toId`) target a template
 * item that was dropped are skipped so they neither point at the template nor
 * collapse unanchored at the canvas origin.
 */
function remapAnnotationAnchors(
	items: readonly CanvasItem[],
	nodes: NodeWrite[],
	edges: EdgeWrite[]
): void {
	const newIdBySource = new Map<string, string>();
	for (let index = 0; index < items.length; index++) {
		const sourceIds = sourceItemIds.get(items[index]);
		const props = nodeProperties(nodes[index]);
		const newId = props?.id;
		if (!sourceIds || typeof newId !== "string" || !newId) continue;
		for (const sourceId of sourceIds) newIdBySource.set(sourceId, newId);
	}

	const dropIndices: number[] = [];
	for (let index = 0; index < items.length; index++) {
		const item = items[index];
		if (item?.type !== "CanvasAnnotation") continue;
		const props = nodeProperties(nodes[index]);
		if (!props) continue;

		const nested =
			props.properties && typeof props.properties === "object"
				? (props.properties as Record<string, unknown>)
				: undefined;
		const nestedContainerId =
			typeof nested?.containerId === "string"
				? nested.containerId.trim()
				: undefined;
		const anchor = sourceAnnotationContainerIds.get(item) || nestedContainerId;

		if (anchor) {
			const mapped = newIdBySource.get(anchor);
			if (!mapped) {
				dropIndices.push(index);
				continue;
			}
			props.containerId = mapped;
			if (nested && "containerId" in nested) {
				nested.containerId = mapped;
			}
		} else if ("containerId" in props) {
			delete props.containerId;
		}

		if (!nested) continue;
		let hasDanglingConnectorEndpoint = false;
		for (const key of ["fromId", "toId"] as const) {
			if (!(key in nested)) continue;
			const current = nested[key];
			if (typeof current === "string" && current.trim()) {
				const mapped = newIdBySource.get(current.trim());
				if (mapped) {
					nested[key] = mapped;
				} else {
					hasDanglingConnectorEndpoint = true;
				}
			} else {
				delete nested[key];
			}
		}
		if (hasDanglingConnectorEndpoint) {
			dropIndices.push(index);
		}
	}

	for (let i = dropIndices.length - 1; i >= 0; i--) {
		const dropIndex = dropIndices[i];
		if (dropIndex === undefined) continue;
		nodes.splice(dropIndex, 1);
		edges.splice(dropIndex, 1);
	}
}

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
	for (const source of templateAnnotations) {
		const ann = source as TemplateAnnotationSource;
		const item: CanvasItem = {
			type: "CanvasAnnotation",
			containerId: config.canvasAnnotationContainerId,
			annotationType: ann.type,
			properties: { ...(ann.properties ?? {}) },
		};
		rememberSourceItem(item, ann.itemId, ann.id);
		const anchor = ann.containerId?.trim();
		if (anchor) sourceAnnotationContainerIds.set(item, anchor);
		items.push(item);
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
			items.push(
				rememberSourceItem(
					applyAssetReplacement(item, asset),
					inst.itemId,
					inst.id
				)
			);
		} else if (isMainWorkOrder(inst, appConfig)) {
			items.push(
				rememberSourceItem(
					applyWorkOrderReplacement(item, workOrder),
					inst.itemId,
					inst.id
				)
			);
		} else {
			const replacement = selections[selectionSlotKey(inst)];
			if (replacement) {
				items.push(
					rememberSourceItem(
						applySelectionReplacement(item, replacement),
						inst.itemId,
						inst.id
					)
				);
			} else if (!isSelectableResourceSlot(inst, appConfig, resourceConfigs)) {
				// Auto template items (not user/AI-filled slots) stay on the canvas
				items.push(rememberSourceItem(item, inst.itemId, inst.id));
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
	remapAnnotationAnchors(items, itemNodes, edges);

	const written: DeleteItem[] = [];
	try {
		await upsertTracked(client, [canvasNode], appConfig, written);
		await upsertTracked(client, itemNodes, appConfig, written);
		await upsertTracked(client, edges, appConfig, written);
	} catch (error) {
		return rollbackWrittenInstances(client, written, error, appConfig);
	}

	return canvasId;
}

type WritableInstance = NodeWrite | EdgeWrite;

async function upsertTracked(
	client: CogniteClient,
	items: WritableInstance[],
	appConfig: AppConfig | undefined,
	written: DeleteItem[]
): Promise<void> {
	for (const batch of chunk(
		items,
		resolveAppConfig(appConfig).limits.upsertBatchSize
	)) {
		await resolveApiRuntime().schedule(() =>
			client.instances.upsert({
				items: batch as NodeWrite[],
			})
		);
		for (const item of batch) {
			written.push({
				instanceType: item.instanceType,
				space: item.space,
				externalId: item.externalId,
			});
		}
	}
}

async function deleteCanvasInstances(
	client: CogniteClient,
	items: DeleteItem[],
	appConfig?: AppConfig,
	deleted?: Set<DeleteItem>
): Promise<void> {
	if (items.length === 0) return;
	const edges = items.filter((item) => item.instanceType === "edge");
	const nodes = items.filter((item) => item.instanceType === "node");
	for (const group of [edges, nodes]) {
		for (const batch of chunk(
			group,
			resolveAppConfig(appConfig).limits.deleteBatchSize
		)) {
			if (batch.length === 0) continue;
			await resolveApiRuntime().scheduleDelete(() =>
				client.instances.delete(batch)
			);
			for (const item of batch) deleted?.add(item);
		}
	}
}

/** Canvas ids whose last update left previous children behind. */
const cleanupIncompleteCanvasIds = new Set<string>();

/**
 * Returns whether the last update of `canvasId` could not remove all previous
 * children, and clears that flag.
 */
export function consumeCanvasCleanupIncomplete(canvasId: string): boolean {
	return cleanupIncompleteCanvasIds.delete(canvasId);
}

/**
 * Delete the previous canvas children after the new ones are written. Retries
 * the not-yet-deleted items once; never throws so the new content is kept.
 *
 * @returns `true` when every previous child was deleted.
 */
async function deletePreviousChildrenBestEffort(
	client: CogniteClient,
	canvasId: string,
	previousChildren: DeleteItem[],
	appConfig?: AppConfig
): Promise<boolean> {
	const deleted = new Set<DeleteItem>();
	try {
		await deleteCanvasInstances(client, previousChildren, appConfig, deleted);
		return true;
	} catch {
		// Retry the remaining items once below.
	}
	const remaining = previousChildren.filter((item) => !deleted.has(item));
	try {
		await deleteCanvasInstances(client, remaining, appConfig);
		return true;
	} catch (retryError) {
		resolveApiRuntime().logError({
			scope: "canvas-builder",
			message:
				"Saved updated canvas items, but failed to delete all previous canvas children",
			context: { canvasId, remainingCount: remaining.length },
			cause: retryError,
		});
		cleanupIncompleteCanvasIds.add(canvasId);
		return false;
	}
}

async function rollbackWrittenInstances(
	client: CogniteClient,
	items: DeleteItem[],
	originalError: unknown,
	appConfig?: AppConfig
): Promise<never> {
	try {
		await deleteCanvasInstances(client, items, appConfig);
	} catch (rollbackError) {
		resolveApiRuntime().logError({
			scope: "canvas-builder",
			message: "Failed to delete canvas instances written before an error",
			context: { count: items.length },
			cause: rollbackError,
		});
		throw new Error(
			`${getErrorMessage(originalError)} (rollback also failed: ${getErrorMessage(rollbackError)})`,
			{ cause: originalError }
		);
	}
	throw originalError;
}

/**
 * List canvas child items (nodes and edges), preserving the canvas root node.
 * Uses `${canvasId}_` prefix so the root node (externalId === canvasId) is excluded.
 * Does not delete anything, so a later write failure can leave the canvas unchanged.
 */
async function listCanvasChildren(
	client: CogniteClient,
	canvasId: string,
	canvasSpace: string,
	appConfig?: AppConfig
): Promise<DeleteItem[]> {
	const childPrefix = `${canvasId}_`;
	const cleanupLimit = resolveAppConfig(appConfig).limits.maxCanvasCleanupItems;
	const children: DeleteItem[] = [];

	const cleanupLimitError = (phase: string) =>
		new Error(
			`[canvas-builder] Canvas "${canvasId}" has more than ${cleanupLimit} child ${phase} (cleanup limit). Aborted before changing the canvas; reduce canvas size or raise config.limits.maxCanvasCleanupItems.`
		);

	try {
		let nodeCursor: string | undefined;
		let nodesProcessed = 0;

		do {
			const nodeResponse = await queryInstanceNodesPage(client, {
				filter: {
					and: [
						{ equals: { property: ["node", "space"], value: canvasSpace } },
						{
							prefix: {
								property: ["node", "externalId"],
								value: childPrefix,
							},
						},
					],
				},
				limit: resolveAppConfig(appConfig).limits.instancePageSize,
				cursor: nodeCursor,
			});

			const nodeBatch = nodeResponse.items || [];
			nodesProcessed += nodeBatch.length;
			if (nodesProcessed > cleanupLimit) {
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
				.filter((item): item is NodeDeleteItem => item !== null);
			children.push(...nodeIds);

			nodeCursor = nodeResponse.nextCursor;
			if (nodeCursor && nodesProcessed >= cleanupLimit) {
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
									and: [
										{
											equals: {
												property: ["edge", "space"],
												value: canvasSpace,
											},
										},
										{
											prefix: {
												property: ["edge", "externalId"],
												value: childPrefix,
											},
										},
									],
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
			if (edgesProcessed > cleanupLimit) {
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
			children.push(...edgeIds);

			edgeCursor = edgeResponse.nextCursor?.edges;
			if (edgeCursor && edgesProcessed >= cleanupLimit) {
				throw cleanupLimitError("edges");
			}
		} while (edgeCursor);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "canvas-builder",
			message: "Failed to list canvas content for canvas",
			context: { canvasId },
			cause: error,
		});
		throw error;
	}

	return children;
}

async function readExistingCanvasPresentation(
	client: CogniteClient,
	config: CanvasConfig,
	canvasId: string,
	appConfig?: AppConfig
): Promise<{
	visibility: "public" | "private";
	solutionTags: ReturnType<typeof readSolutionTagsFromRootProperties>;
}> {
	const fallback = resolveAppConfig(appConfig).canvas.visibility;
	const nodes = await retrieveInstanceNodes(
		client,
		[{ space: config.canvasSpaceInstance, externalId: canvasId }],
		[viewIdToSource(canvasSchemaView(appConfig))]
	);
	const node = toCdfNode(nodes[0]);
	if (!node) return { visibility: fallback, solutionTags: undefined };
	const props = extractInstanceProperties(node, canvasSchemaView(appConfig));
	const visibility =
		props.visibility === "private" || props.visibility === "public"
			? props.visibility
			: fallback;
	return {
		visibility,
		solutionTags: readSolutionTagsFromRootProperties(props),
	};
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

	cleanupIncompleteCanvasIds.delete(canvasId);
	const previousChildren = await listCanvasChildren(
		client,
		canvasId,
		config.canvasSpaceInstance,
		appConfig
	);
	await ensureWorkPackageSolutionTag(client, appConfig);
	const existing = await readExistingCanvasPresentation(
		client,
		config,
		canvasId,
		appConfig
	);
	const userId = await resolveCanvasUserId(client, appConfig);
	const timestamp = nowIsoMs();

	const { nodes: itemNodes, edges } = buildCanvasNodesAndEdges(
		items,
		canvasId,
		config
	);
	remapAnnotationAnchors(items, itemNodes, edges);

	const written: DeleteItem[] = [];
	try {
		await upsertTracked(client, itemNodes, appConfig, written);
		await upsertTracked(client, edges, appConfig, written);
	} catch (error) {
		return rollbackWrittenInstances(client, written, error, appConfig);
	}
	await deletePreviousChildrenBestEffort(
		client,
		canvasId,
		previousChildren,
		appConfig
	);

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
					visibility: existing.visibility,
					updatedAt: timestamp,
					updatedBy: userId,
					solutionTags: withWorkPackageSolutionTag(
						existing.solutionTags,
						appConfig
					),
				},
			},
		],
	};
	await upsertTracked(client, [canvasNode], appConfig, []);

	return canvasId;
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
	return `https://${host}/${encodeURIComponent(project)}/industrial-canvas/canvas?canvasId=${encodeURIComponent(canvasId)}&cluster=${encodeURIComponent(clusterHost)}&workspace=${encodeURIComponent(workspace)}`;
}

export { extractClusterFromUrl } from "../app-config";
