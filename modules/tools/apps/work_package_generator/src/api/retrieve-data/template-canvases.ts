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

/**
 * Views to scan for template roots. Canvas/v1 has no `visibility` property,
 * so a filter on it fails the whole query. Canvas/v7 is where Industrial
 * Canvas stores current roots. Scan both when they differ.
 */
function templateCanvasViews(config: AppConfig): ViewReference[] {
	const view = canvasSchemaView(config);
	const views = [view];
	if (
		view.space === "cdf_industrial_canvas" &&
		view.externalId === "Canvas" &&
		view.version !== "v7"
	) {
		views.push({ ...view, version: "v7" });
	}
	return views;
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
		visibility: readVisibility(props),
		createdBy: getOptionalStringProp(props, "createdBy"),
		updatedBy: getOptionalStringProp(props, "updatedBy"),
		createdAt: getOptionalStringProp(props, "createdAt"),
		updatedAt: getOptionalStringProp(props, "updatedAt"),
	};
}

function readVisibility(props: Record<string, unknown>): "public" | "private" {
	return props.visibility === "private" ? "private" : "public";
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
 * List every visible template canvas. Property filters stay on the client:
 * Canvas/v1 rejects `visibility`, and the name prefix is case-insensitive.
 * Newest canvases are sorted first. Same names are all kept.
 */
export async function listTemplateCanvases(
	client: CogniteClient,
	config?: AppConfig
): Promise<TemplateCanvas[]> {
	const resolvedConfig = resolveAppConfig(config);
	const views = templateCanvasViews(resolvedConfig);
	const prefix = resolvedConfig.templates.namePrefix;
	const userId = await resolveCanvasUserId(client, resolvedConfig);

	const seen = new Set<string>();
	const all: TemplateCanvas[] = [];
	let anySucceeded = false;
	let lastError: unknown;
	let truncated = false;

	for (const schemaView of views) {
		try {
			const page = await listInstanceNodesWithCap(client, {
				sources: [viewIdToSource(schemaView)],
				maxTotalItems: resolvedConfig.limits.maxCanvasListScanItems,
				warnContext: `listTemplateCanvases:${schemaView.externalId}/${schemaView.version}`,
				config: resolvedConfig,
			});
			anySucceeded = true;
			truncated = truncated || page.truncated;
			for (const item of page.items) {
				const node = toCdfNode(item);
				if (!node) continue;
				const key = `${node.space}:${node.externalId}`;
				if (seen.has(key)) continue;
				seen.add(key);

				const props = extractInstanceProperties(node, schemaView);
				const name = getStringProp(props, "name");
				const canvas = mapNodeToTemplateCanvas(node, props, name);
				const archived = props.isArchived === true;
				if (isVisibleTemplate(canvas, prefix, userId, archived)) {
					all.push(canvas);
				}
			}
		} catch (error) {
			lastError = error;
		}
	}

	if (!anySucceeded) {
		throw lastError instanceof Error
			? lastError
			: new Error("Failed to list template canvases");
	}
	markCatalogTruncated(truncated);

	const ownerNames = await lookupOwnerNames(
		client,
		all.flatMap((canvas) => (canvas.createdBy ? [canvas.createdBy] : []))
	);
	for (const canvas of all) {
		if (!canvas.createdBy) continue;
		const name = ownerNames.get(canvas.createdBy);
		if (name) canvas.ownerName = name;
	}

	return all.sort((a, b) =>
		(b.updatedAt ?? "").localeCompare(a.updatedAt ?? "")
	);
}

/** Map canvas `createdBy` ids to profile display names. Missing profiles stay unresolved. */
async function lookupOwnerNames(
	client: CogniteClient,
	userIdentifiers: readonly string[]
): Promise<Map<string, string>> {
	const unique = [...new Set(userIdentifiers)];
	const names = new Map<string, string>();
	if (!client.profiles?.retrieve || unique.length === 0) return names;

	const batchSize = 100;
	for (let index = 0; index < unique.length; index += batchSize) {
		const batch = unique.slice(index, index + batchSize);
		try {
			const profiles = await client.profiles.retrieve(
				batch.map((userIdentifier) => ({ userIdentifier }))
			);
			rememberOwnerNames(names, profiles);
		} catch {
			for (const userIdentifier of batch) {
				try {
					const profiles = await client.profiles.retrieve([{ userIdentifier }]);
					rememberOwnerNames(names, profiles);
				} catch {
					// Keep the raw id when this profile cannot be resolved.
				}
			}
		}
	}
	return names;
}

function rememberOwnerNames(
	names: Map<string, string>,
	profiles: ReadonlyArray<{
		userIdentifier: string;
		displayName?: string | null;
		email?: string | null;
	}>
): void {
	for (const profile of profiles) {
		const name = profile.displayName?.trim() || profile.email?.trim();
		if (name) names.set(profile.userIdentifier, name);
	}
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

	const schemaView = canvasSchemaView(resolveAppConfig(config));
	const canvasNamePropPath = propertyPath(schemaView, "name");

	const { items: pageItems } = await listInstanceNodesWithCap(client, {
		sources: [viewIdToSource(schemaView)],
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

		const props = extractInstanceProperties(node, schemaView);
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
