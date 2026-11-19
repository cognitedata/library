/**
 * Canvas item builder for the synthetic "All Resources" base template.
 */

import {
	type AppConfig,
	getResourceConfigs,
	resolveAppConfig,
} from "@/app-config";
import type {
	Asset,
	CanvasConfig,
	CanvasItem,
	ResourceInstance,
	WorkOrder,
} from "@/types";

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

/** A resource-type section placed at `x` with a fixed `width`. */
interface SectionSpec {
	title: string;
	sources: PrimarySource[];
	x: number;
	width: number;
}

type ResolvedCanvas = ReturnType<typeof resolveAppConfig>["canvas"];

/** Sticky header annotation for a section. */
function createSectionHeader(
	section: SectionSpec,
	containerId: string,
	style: ResolvedCanvas["style"]
): CanvasItem {
	return {
		type: "CanvasAnnotation",
		containerId,
		annotationType: "stickyAnnotation",
		properties: {
			x: section.x,
			y: 0,
			width: section.width,
			height: HEADER_H,
			text: section.title,
			style: { ...style },
		},
	};
}

/** Cards of a section laid out in a 2-column grid under its header. */
function createSectionCards(
	section: SectionSpec,
	containerId: string,
	dimensions: ResolvedCanvas["dimensions"]
): CanvasItem[] {
	const cardsTop = HEADER_H + GAP;
	return section.sources.map((source, i) => ({
		type: "fdmInstance",
		containerId,
		instanceExternalId: source.externalId,
		instanceSpace: source.space,
		viewExternalId: source.viewExternalId,
		viewSpace: source.viewSpace,
		viewVersion: source.viewVersion,
		label: source.label,
		properties: {},
		x: section.x + (i % COLS) * (CARD_W + GAP),
		y: cardsTop + Math.floor(i / COLS) * (CARD_H + GAP),
		width: CARD_W,
		height: CARD_H,
		maxWidth: dimensions.maxWidth,
		maxHeight: dimensions.maxHeight,
	}));
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
	const resolvedCanvas = resolveAppConfig(appConfig).canvas;
	const width = COLS * CARD_W + (COLS - 1) * GAP;
	for (const typeKey of Object.keys(candidatesByType).sort()) {
		const candidates = candidatesByType[typeKey] ?? [];
		if (candidates.length === 0) continue;

		const displayName = resourceConfigs[typeKey]?.displayName ?? typeKey;
		const sources = candidates
			.map((candidate) =>
				resourceInstanceToSource(candidate, typeKey, appConfig)
			)
			.filter((source): source is PrimarySource => source !== null);

		if (sources.length === 0) continue;

		const section: SectionSpec = {
			title: displayName,
			sources,
			x: xCursor,
			width,
		};
		items.push(
			createSectionHeader(
				section,
				config.canvasAnnotationContainerId,
				resolvedCanvas.style
			),
			...createSectionCards(
				section,
				config.canvasInstanceContainerId,
				resolvedCanvas.dimensions
			)
		);
		xCursor += width + SECTION_GAP;
	}

	return items;
}
