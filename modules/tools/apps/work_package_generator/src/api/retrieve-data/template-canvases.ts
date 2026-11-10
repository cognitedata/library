/**
 * Template canvas listing and child instance/annotation fetches.
 */

import type { CogniteClient, ViewReference } from "@cognite/sdk";
import {
	type AppConfig,
	canvasAnnotationView,
	canvasInstanceView,
	canvasSchemaView,
	resolveAppConfig,
	viewIdToSource,
} from "../../app-config";
import type {
	TemplateAnnotation,
	TemplateCanvas,
	TemplateInstance,
} from "../../types";
import { resolveCanvasUserId } from "../../utils/canvas-user";
import {
	extractInstanceProperties,
	getNumberProp,
	getOptionalStringProp,
	getStringProp,
	toCdfNode,
} from "../../utils/cdf-helpers";
import { listInstanceNodesWithCap } from "./list-nodes";

/** Annotation row plus the node id and container the builder writes back. */
export interface TemplateAnnotationItem extends TemplateAnnotation {
	id: string;
	containerId: string;
}

export interface TemplateReadNotices {
	catalogTruncated: boolean;
	truncatedChildIds: readonly string[];
}

let catalogTruncated = false;
const truncatedChildIds = new Set<string>();
const noticeListeners = new Set<() => void>();
let templateReadNotices: TemplateReadNotices = {
	catalogTruncated: false,
	truncatedChildIds: [],
};

function emitTemplateReadNotices(): void {
	templateReadNotices = {
		catalogTruncated,
		truncatedChildIds: [...truncatedChildIds],
	};
	for (const listener of noticeListeners) listener();
}

/** Subscribe to truncation notices for template reads. */
export function subscribeTemplateReadNotices(listener: () => void): () => void {
	noticeListeners.add(listener);
	return () => {
		noticeListeners.delete(listener);
	};
}

/** Latest truncation state for template catalog and child reads. */
export function getTemplateReadNotices(): TemplateReadNotices {
	return templateReadNotices;
}

/** Test-only reset for truncation notices. */
export function resetTemplateReadNotices(): void {
	catalogTruncated = false;
	truncatedChildIds.clear();
	emitTemplateReadNotices();
}

function markCatalogTruncated(truncated: boolean): void {
	catalogTruncated = truncated;
	emitTemplateReadNotices();
}

function markChildTruncated(templateId: string, truncated: boolean): void {
	if (!truncated) return;
	truncatedChildIds.add(templateId);
	emitTemplateReadNotices();
}

function propertyPath(view: ViewReference, property: string): string[] {
	return [view.space, `${view.externalId}/${view.version}`, property];
}

/** Child nodes are stored as `${canvasId}_…`, so the prefix needs the separator. */
function childExternalIdPrefix(templateId: string): string {
	return `${templateId}_`;
}

/** Map a canvas node + its extracted properties to a TemplateCanvas. */
function mapNodeToTemplateCanvas(
	node: NonNullable<ReturnType<typeof toCdfNode>>,
	props: ReturnType<typeof extractInstanceProperties>,
	name: string
): TemplateCanvas {
	return {
		externalId: node.externalId,
		space: node.space,
		name,
		visibility:
			(getStringProp(props, "visibility") as "public" | "private") || "private",
		createdBy: getOptionalStringProp(props, "createdBy"),
		updatedBy: getOptionalStringProp(props, "updatedBy"),
		createdAt: getOptionalStringProp(props, "createdAt"),
		updatedAt: getOptionalStringProp(props, "updatedAt"),
	};
}

function isVisibleTemplate(
	canvas: TemplateCanvas,
	prefix: string,
	userId: string,
	isArchived: boolean
): boolean {
	if (isArchived) return false;
	if (!canvas.name.toLowerCase().startsWith(prefix.toLowerCase())) return false;
	if (canvas.visibility === "public") return true;
	return canvas.createdBy === userId;
}

/**
 * List every visible template canvas. Name, archive, and visibility filters
 * are applied on the server; the same rules are checked on each row.
 * Newest canvases are sorted first. Same names are all kept.
 */
export async function listTemplateCanvases(
	client: CogniteClient,
	config?: AppConfig
): Promise<TemplateCanvas[]> {
	const resolvedConfig = resolveAppConfig(config);
	const schemaView = canvasSchemaView(resolvedConfig);
	const prefix = resolvedConfig.templates.namePrefix;
	const userId = await resolveCanvasUserId(client, resolvedConfig);
	const namePath = propertyPath(schemaView, "name");
	const visibilityPath = propertyPath(schemaView, "visibility");
	const createdByPath = propertyPath(schemaView, "createdBy");
	const archivedPath = propertyPath(schemaView, "isArchived");

	const { items: pageItems, truncated } = await listInstanceNodesWithCap(
		client,
		{
			sources: [viewIdToSource(schemaView)],
			filter: {
				and: [
					{ prefix: { property: namePath, value: prefix } },
					{ not: { equals: { property: archivedPath, value: true } } },
					{
						or: [
							{ equals: { property: visibilityPath, value: "public" } },
							{ equals: { property: createdByPath, value: userId } },
						],
					},
				],
			},
			maxTotalItems: resolvedConfig.limits.maxCanvasListScanItems,
			warnContext: "listTemplateCanvases",
			config: resolvedConfig,
		}
	);
	markCatalogTruncated(truncated);

	const all: TemplateCanvas[] = [];
	for (const item of pageItems) {
		const node = toCdfNode(item);
		if (!node) continue;

		const props = extractInstanceProperties(node, schemaView);
		const name = getStringProp(props, "name");
		const canvas = mapNodeToTemplateCanvas(node, props, name);
		const archived = props.isArchived === true;
		if (isVisibleTemplate(canvas, prefix, userId, archived)) {
			all.push(canvas);
		}
	}

	return all.sort((a, b) =>
		(b.updatedAt ?? "").localeCompare(a.updatedAt ?? "")
	);
}

/**
 * Find all canvases whose name exactly matches the given name.
 */
export async function findCanvasesByName(
	client: CogniteClient,
	canvasName: string,
	config?: AppConfig
): Promise<TemplateCanvas[]> {
	const trimmedTarget = canvasName.trim();
	if (!trimmedTarget) return [];

	const canvasNamePropPath = [
		canvasSchemaView(config).space,
		`${canvasSchemaView(config).externalId}/${canvasSchemaView(config).version}`,
		"name",
	];

	const { items: pageItems } = await listInstanceNodesWithCap(client, {
		sources: [viewIdToSource(canvasSchemaView(config))],
		filter: {
			equals: {
				property: canvasNamePropPath,
				value: trimmedTarget,
			},
		},
		maxTotalItems: resolveAppConfig(config).limits.findCanvasByNameMaxItems,
		warnContext: `findCanvasesByName:${trimmedTarget}`,
		config,
	});

	const matches: TemplateCanvas[] = [];
	for (const item of pageItems) {
		const node = toCdfNode(item);
		if (!node) continue;

		const props = extractInstanceProperties(node, canvasSchemaView(config));
		const name = getStringProp(props, "name").trim();

		if (name === trimmedTarget) {
			matches.push(mapNodeToTemplateCanvas(node, props, name));
		}
	}

	return matches;
}

/**
 * Get template canvas instances
 */
export async function getTemplateCanvasInstances(
	client: CogniteClient,
	templateId: string,
	config?: AppConfig
): Promise<TemplateInstance[]> {
	const { items, truncated } = await listInstanceNodesWithCap(client, {
		sources: [viewIdToSource(canvasInstanceView(config))],
		filter: {
			prefix: {
				property: ["node", "externalId"],
				value: childExternalIdPrefix(templateId),
			},
		},
		maxTotalItems: resolveAppConfig(config).limits.maxTemplateCanvasChildItems,
		warnContext: `getTemplateCanvasInstances:${templateId}`,
		config,
	});
	markChildTruncated(templateId, truncated);

	const mapped = items
		.map((item) => {
			const node = toCdfNode(item);
			if (!node) return null;

			const props = extractInstanceProperties(node, canvasInstanceView(config));

			return {
				id: node.externalId,
				label: getStringProp(props, "label", "Unlabeled"),
				viewExternalId: getStringProp(props, "viewExternalId"),
				viewSpace: getStringProp(props, "viewSpace"),
				viewVersion: getStringProp(props, "viewVersion", "v1"),
				instanceExternalId: getOptionalStringProp(props, "instanceExternalId"),
				instanceSpace: getOptionalStringProp(props, "instanceSpace"),
				description: getOptionalStringProp(props, "description"),
				x: getNumberProp(
					props,
					"x",
					resolveAppConfig(config).canvas.dimensions.x
				),
				y: getNumberProp(
					props,
					"y",
					resolveAppConfig(config).canvas.dimensions.y
				),
				width: getNumberProp(
					props,
					"width",
					resolveAppConfig(config).canvas.dimensions.width
				),
				height: getNumberProp(
					props,
					"height",
					resolveAppConfig(config).canvas.dimensions.height
				),
				maxWidth: getNumberProp(
					props,
					"maxWidth",
					resolveAppConfig(config).canvas.dimensions.maxWidth
				),
				maxHeight: getNumberProp(
					props,
					"maxHeight",
					resolveAppConfig(config).canvas.dimensions.maxHeight
				),
				properties: (props.properties as Record<string, unknown>) || {},
			} as TemplateInstance;
		})
		.filter((item): item is TemplateInstance => item !== null);
	return mapped;
}

/**
 * Get template canvas annotations
 */
export async function getTemplateCanvasAnnotations(
	client: CogniteClient,
	templateId: string,
	config?: AppConfig
): Promise<TemplateAnnotationItem[]> {
	const { items, truncated } = await listInstanceNodesWithCap(client, {
		sources: [viewIdToSource(canvasAnnotationView(config))],
		filter: {
			prefix: {
				property: ["node", "externalId"],
				value: childExternalIdPrefix(templateId),
			},
		},
		maxTotalItems: resolveAppConfig(config).limits.maxTemplateCanvasChildItems,
		warnContext: `getTemplateCanvasAnnotations:${templateId}`,
		config,
	});
	markChildTruncated(templateId, truncated);

	return items
		.map((item) => {
			const node = toCdfNode(item);
			if (!node) return null;

			const props = extractInstanceProperties(
				node,
				canvasAnnotationView(config)
			);

			return {
				id: node.externalId,
				containerId: getStringProp(props, "containerId", ""),
				type: getStringProp(props, "annotationType", "Unknown"),
				properties: (props.properties as Record<string, unknown>) || {},
			};
		})
		.filter((item): item is TemplateAnnotationItem => item !== null);
}
