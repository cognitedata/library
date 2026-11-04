/**
 * Canvas item builder for the synthetic "All Resources" base template.
 */

import {
	type AppConfig,
	getResourceConfigs,
	resolveAppConfig,
} from "../../app-config";
import type {
	Asset,
	CanvasConfig,
	CanvasItem,
	ResourceInstance,
	WorkOrder,
} from "../../types";

const CARD_W = 280;
const CARD_H = 120;
const GAP = 16;
/** Cards within a section fill a 2-column grid under the header. */
const COLS = 2;
const HEADER_H = 48;
/** Horizontal gap between resource-type sections. */
const SECTION_GAP = 32;

interface PrimarySource {
	externalId: string;
	space: string;
	label: string;
	viewExternalId: string;
	viewSpace: string;
	viewVersion: string;
}

function groupWidth(): number {
	return COLS * CARD_W + (COLS - 1) * GAP;
}

/**
 * Place a section at (xCursor, 0): sticky header, then cards stacked below.
 * Returns the next x cursor after this section.
 */
function appendGroup(
	items: CanvasItem[],
	config: CanvasConfig,
	title: string,
	sources: PrimarySource[],
	xCursor: number,
	appConfig?: AppConfig
): number {
	if (sources.length === 0) return xCursor;

	const width = groupWidth();

	items.push({
		type: "CanvasAnnotation",
		containerId: config.canvasAnnotationContainerId,
		annotationType: "stickyAnnotation",
		properties: {
			x: xCursor,
			y: 0,
			width,
			height: HEADER_H,
			text: title,
			style: { ...resolveAppConfig(appConfig).canvas.style },
		},
	});

	const cardsTop = HEADER_H + GAP;
	for (let i = 0; i < sources.length; i++) {
		const col = i % COLS;
		const row = Math.floor(i / COLS);
		const source = sources[i];

		items.push({
			type: "fdmInstance",
			containerId: config.canvasInstanceContainerId,
			instanceExternalId: source.externalId,
			instanceSpace: source.space,
			viewExternalId: source.viewExternalId,
			viewSpace: source.viewSpace,
			viewVersion: source.viewVersion,
			label: source.label,
			properties: {},
			x: xCursor + col * (CARD_W + GAP),
			y: cardsTop + row * (CARD_H + GAP),
			width: CARD_W,
			height: CARD_H,
			maxWidth: resolveAppConfig(appConfig).canvas.dimensions.maxWidth,
			maxHeight: resolveAppConfig(appConfig).canvas.dimensions.maxHeight,
		});
	}

	return xCursor + width + SECTION_GAP;
}

function resourceInstanceToSource(
	instance: ResourceInstance,
	typeKey: string,
	appConfig?: AppConfig
): PrimarySource | null {
	const resourceConfig = getResourceConfigs(appConfig)[typeKey];
	if (!resourceConfig) return null;

	const { viewId } = resourceConfig;
	const name =
		(typeof instance.properties.name === "string"
			? instance.properties.name
			: undefined) ||
		instance.name ||
		instance.externalId;

	return {
		externalId: instance.externalId,
		space: instance.space,
		label: name,
		viewExternalId: viewId.externalId,
		viewSpace: viewId.space,
		viewVersion: viewId.version || "v1",
	};
}

/**
 * Build canvas items for the "All Resources" base template.
 * Resource-type sections are laid out left-to-right; each has a sticky header
 * and a 2-column card grid underneath.
 */
export function buildBaseTemplateItems(
	config: CanvasConfig,
	_workOrder: WorkOrder,
	_asset: Asset,
	candidatesByType: Record<string, ResourceInstance[]>,
	appConfig?: AppConfig
): CanvasItem[] {
	const items: CanvasItem[] = [];
	let xCursor = 0;

	const resourceConfigs = getResourceConfigs(appConfig);
	for (const typeKey of Object.keys(candidatesByType).sort()) {
		const candidates = candidatesByType[typeKey] ?? [];
		if (candidates.length === 0) continue;

		const displayName = resourceConfigs[typeKey]?.displayName ?? typeKey;
		const sources = candidates
			.map((candidate) =>
				resourceInstanceToSource(candidate, typeKey, appConfig)
			)
			.filter((source): source is PrimarySource => source !== null);

		xCursor = appendGroup(
			items,
			config,
			displayName,
			sources,
			xCursor,
			appConfig
		);
	}

	return items;
}
