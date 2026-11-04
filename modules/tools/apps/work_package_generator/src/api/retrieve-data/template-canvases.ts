/**
 * Template canvas listing and child instance/annotation fetches.
 */

import type { CogniteClient } from "@cognite/sdk";
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
import {
	extractInstanceProperties,
	getNumberProp,
	getOptionalStringProp,
	getStringProp,
	toCdfNode,
} from "../../utils/cdf-helpers";
import { resolveApiRuntime } from "../runtime-deps";
import { listInstanceNodesWithCap } from "./list-nodes";

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

/**
 * List all template canvases (names starting with the configured template prefix).
 * Paginates until all results are collected, then deduplicates by name,
 * keeping the most recently updated version and sorting newest-first.
 */
export async function listTemplateCanvases(
	client: CogniteClient,
	config?: AppConfig
): Promise<TemplateCanvas[]> {
	try {
		const resolvedConfig = resolveAppConfig(config);
		const schemaView = canvasSchemaView(resolvedConfig);
		const prefix = resolvedConfig.templates.namePrefix.toLowerCase();

		const pageItems = await listInstanceNodesWithCap(client, {
			sources: [viewIdToSource(schemaView)],
			maxTotalItems: resolvedConfig.limits.maxCanvasListScanItems,
			warnContext: "listTemplateCanvases",
			config: resolvedConfig,
		});

		const all: TemplateCanvas[] = [];
		for (const item of pageItems) {
			const node = toCdfNode(item);
			if (!node) continue;

			const props = extractInstanceProperties(node, schemaView);
			const name = getStringProp(props, "name");

			if (name.toLowerCase().startsWith(prefix)) {
				all.push(mapNodeToTemplateCanvas(node, props, name));
			}
		}

		// Keep most recently updated version per name
		const byName: Record<string, TemplateCanvas> = {};
		for (const canvas of all) {
			const existing = byName[canvas.name];
			if (!existing || (canvas.updatedAt ?? "") > (existing.updatedAt ?? "")) {
				byName[canvas.name] = canvas;
			}
		}

		return Object.values(byName).sort((a, b) =>
			(b.updatedAt ?? "").localeCompare(a.updatedAt ?? "")
		);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.listTemplateCanvases",
			message: "Failed to list template canvases",
			cause: error,
		});
		return [];
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

	const canvasNamePropPath = [
		canvasSchemaView(config).space,
		`${canvasSchemaView(config).externalId}/${canvasSchemaView(config).version}`,
		"name",
	];

	try {
		const pageItems = await listInstanceNodesWithCap(client, {
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
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.findCanvasesByName",
			message: "Failed to find canvases by name",
			cause: error,
			context: { nameLen: trimmedTarget.length },
		});
		return [];
	}
}

/**
 * Get template canvas instances
 */
export async function getTemplateCanvasInstances(
	client: CogniteClient,
	templateId: string,
	config?: AppConfig
): Promise<TemplateInstance[]> {
	try {
		const items = await listInstanceNodesWithCap(client, {
			sources: [viewIdToSource(canvasInstanceView(config))],
			filter: {
				prefix: {
					property: ["node", "externalId"],
					value: templateId,
				},
			},
			maxTotalItems:
				resolveAppConfig(config).limits.maxTemplateCanvasChildItems,
			warnContext: `getTemplateCanvasInstances:${templateId}`,
			config,
		});

		const mapped = items
			.map((item) => {
				const node = toCdfNode(item);
				if (!node) return null;

				const props = extractInstanceProperties(
					node,
					canvasInstanceView(config)
				);

				return {
					label: getStringProp(props, "label", "Unlabeled"),
					viewExternalId: getStringProp(props, "viewExternalId"),
					viewSpace: getStringProp(props, "viewSpace"),
					viewVersion: getStringProp(props, "viewVersion", "v1"),
					instanceExternalId: getOptionalStringProp(
						props,
						"instanceExternalId"
					),
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
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.getTemplateCanvasInstances",
			message: "Failed to get template canvas instances",
			cause: error,
			context: { templateIdLen: templateId.length },
		});
		return [];
	}
}

/**
 * Get template canvas annotations
 */
export async function getTemplateCanvasAnnotations(
	client: CogniteClient,
	templateId: string,
	config?: AppConfig
): Promise<TemplateAnnotation[]> {
	try {
		const items = await listInstanceNodesWithCap(client, {
			sources: [viewIdToSource(canvasAnnotationView(config))],
			filter: {
				prefix: {
					property: ["node", "externalId"],
					value: templateId,
				},
			},
			maxTotalItems:
				resolveAppConfig(config).limits.maxTemplateCanvasChildItems,
			warnContext: `getTemplateCanvasAnnotations:${templateId}`,
			config,
		});

		return items
			.map((item) => {
				const node = toCdfNode(item);
				if (!node) return null;

				const props = extractInstanceProperties(
					node,
					canvasAnnotationView(config)
				);

				return {
					type: getStringProp(props, "annotationType", "Unknown"),
					properties: (props.properties as Record<string, unknown>) || {},
				};
			})
			.filter((item): item is TemplateAnnotation => item !== null);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.getTemplateCanvasAnnotations",
			message: "Failed to get template canvas annotations",
			cause: error,
			context: { templateIdLen: templateId.length },
		});
		return [];
	}
}
