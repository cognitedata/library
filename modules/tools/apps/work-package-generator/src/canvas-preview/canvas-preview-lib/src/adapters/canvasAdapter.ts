/**
 * Canvas Adapter
 * Converts Canvas SDK FdmInstanceContainerReference to flexible CanvasNode format
 */

import type { CogniteClient, ViewDefinition } from "@cognite/sdk";
import type {
	FdmReference,
	FlexibleFileData,
	FlexibleTimeseriesData,
} from "@dune-canvas/canvas-app";

import type { CanvasNode } from "@dune-canvas/canvas-lib";
import { getNodeTypeFromFileInfo } from "@dune-canvas/canvas-lib";
import type { TableNodeData } from "@dune-canvas/canvas-plugins/table/TableNode";
import { type CdfSchedule, defaultCdfSchedule } from "../../../cdfSchedule";
import type {
	CanvasAnnotation,
	FdmInstanceContainerReference,
} from "../canvasQueries";
import type { CanvasNodeDef } from "../types";
import {
	determineCdmViewType,
	getViewCacheKey,
	resolveViews,
} from "../utils/cdmViewHelpers";
import type { PolylineAnnotationProps } from "./annotationTypes";
import {
	calculateAnnotationSize,
	mapAnnotationDataToPluginData,
	mapAnnotationTypeToNodeType,
	polylineGeometry,
} from "./mappers";

// ============================================================================
// Table Data for Activities/Assets/Unknown Types
// ============================================================================

/** Helper to create a unique key for an FDM container */
const getContainerKey = (props: {
	instanceSpace: string;
	instanceExternalId: string;
}) => `${props.instanceSpace}:${props.instanceExternalId}`;

// ============================================================================
// FDM Reference Builder
// ============================================================================

function buildFdmReference(
	props: FdmInstanceContainerReference["properties"]
): FdmReference {
	return {
		space: props.instanceSpace,
		externalId: props.instanceExternalId,
		viewSpace: props.viewSpace,
		viewExternalId: props.viewExternalId,
		viewVersion: props.viewVersion ?? undefined,
	};
}

// ============================================================================
// Plugin-Specific Data Builders
// ============================================================================

function createTimeseriesData(
	props: FdmInstanceContainerReference["properties"]
): FlexibleTimeseriesData {
	const additionalProps = props.properties as
		| Record<string, unknown>
		| null
		| undefined;

	return {
		source: {
			type: "fdm",
			instance: buildFdmReference(props),
			startDate: (additionalProps?.startDate as string) ?? undefined,
			endDate: (additionalProps?.endDate as string) ?? undefined,
		},
		label: props.label ?? undefined,
		color: (additionalProps?.color as string) ?? undefined,
		minY: (additionalProps?.minY as number) ?? undefined,
		maxY: (additionalProps?.maxY as number) ?? undefined,
	};
}

function createFileData(
	props: FdmInstanceContainerReference["properties"]
): FlexibleFileData {
	const additionalProps = props.properties as
		| Record<string, unknown>
		| null
		| undefined;

	return {
		source: {
			type: "fdm",
			instance: buildFdmReference(props),
		},
		label: props.label ?? undefined,
		currentPage: (additionalProps?.currentPage as number) ?? undefined,
	};
}

function createTableData(
	props: FdmInstanceContainerReference["properties"]
): TableNodeData {
	const viewType = props.viewExternalId.replace(/^Cognite/, "");
	const title = `${viewType} - ${props.label ?? props.instanceExternalId}`;

	return {
		source: {
			type: "fdm",
			instance: {
				space: props.instanceSpace,
				externalId: props.instanceExternalId,
				viewSpace: props.viewSpace,
				viewExternalId: props.viewExternalId,
				viewVersion: props.viewVersion ?? "v1",
			},
		},
		title,
		hideHeader: true,
		hideFooter: true,
	};
}

// ============================================================================
// View Type to Node Type Mapping
// ============================================================================

function mapResolvedViewTypeToNodeType(
	cdmViewType: ReturnType<typeof determineCdmViewType>,
	_fallbackViewExternalId: string
): string {
	switch (cdmViewType) {
		case "FILE":
			return "pdf";
		case "TIMESERIES":
			return "timeseries";
		default:
			return "table";
	}
}

// ============================================================================
// Main Adapter Function
// ============================================================================

export function fdmContainerToNode(
	container: FdmInstanceContainerReference,
	zIndex: number = 0,
	resolvedView?: ViewDefinition,
	nodeTypeOverride?: string
): CanvasNode {
	const props = container.properties;

	const cdmViewType = determineCdmViewType(resolvedView);
	const nodeType =
		nodeTypeOverride ??
		mapResolvedViewTypeToNodeType(cdmViewType, props.viewExternalId);

	let data: unknown;

	switch (cdmViewType) {
		case "TIMESERIES":
			data = createTimeseriesData(props);
			break;
		case "FILE":
			data = createFileData(props);
			break;
		default:
			data = createTableData(props);
	}

	return {
		id: container.externalId,
		type: nodeType,
		position: {
			x: props.x ?? 0,
			y: props.y ?? 0,
		},
		size: {
			width: props.width ?? 200,
			height: props.height ?? 150,
		},
		zIndex,
		data,
	};
}

// ============================================================================
// Annotation Adapter
// ============================================================================

export function annotationToNode(
	annotation: CanvasAnnotation,
	zIndex: number = 0
): CanvasNode {
	const props = annotation.properties;
	const additionalProps = props.properties as
		| Record<string, unknown>
		| null
		| undefined;
	const nodeType = mapAnnotationTypeToNodeType(props.annotationType);

	const pluginData = mapAnnotationDataToPluginData(
		props.annotationType,
		additionalProps
	);
	const polyline =
		props.annotationType === "polylineAnnotation"
			? polylineGeometry(
					((additionalProps ?? {}) as PolylineAnnotationProps).vertices ?? [],
					Number(
						((additionalProps ?? {}) as PolylineAnnotationProps).style
							?.strokeWidth ?? 2
					)
				)
			: undefined;
	const size = polyline
		? { width: polyline.width, height: polyline.height }
		: calculateAnnotationSize(props.annotationType, additionalProps);

	return {
		id: annotation.externalId,
		type: nodeType,
		position: polyline
			? { x: polyline.x, y: polyline.y }
			: {
					x: (additionalProps?.x as number) ?? 0,
					y: (additionalProps?.y as number) ?? 0,
				},
		size,
		zIndex,
		data: pluginData,
		parentId: props.containerId ?? undefined,
	};
}

// ============================================================================
// Canvas Data Aggregation
// ============================================================================

export interface AggregateCanvasNodesOptions {
	fdmContainers: FdmInstanceContainerReference[];
	annotations: CanvasAnnotation[];
	client?: CogniteClient;
	viewCache?: Map<string, ViewDefinition>;
	schedule?: CdfSchedule;
}

export async function aggregateCanvasNodes({
	fdmContainers,
	annotations,
	client: sdk,
	viewCache,
	schedule = defaultCdfSchedule,
}: AggregateCanvasNodesOptions): Promise<CanvasNodeDef[]> {
	const nodes: CanvasNode[] = [];
	let zIndex = 0;

	// Use the provided viewCache, or fall back to a transient local Map
	const cache = viewCache ?? new Map<string, ViewDefinition>();
	let resolvedViewsMap = new Map<string, ViewDefinition>();

	if (sdk && fdmContainers.length > 0) {
		const viewRefs = fdmContainers.map((container) => ({
			space: container.properties.viewSpace,
			externalId: container.properties.viewExternalId,
			version: container.properties.viewVersion ?? undefined,
		}));

		try {
			resolvedViewsMap = await resolveViews(sdk, viewRefs, cache, schedule);
		} catch {
			// Falls back to viewExternalId-based type detection
		}
	}

	const resolvedNodeTypeOverrides = new Map<string, string>();

	if (sdk && fdmContainers.length > 0) {
		const fileContainers = fdmContainers.filter((container) => {
			const props = container.properties;
			const cacheKey = getViewCacheKey({
				space: props.viewSpace,
				externalId: props.viewExternalId,
				version: props.viewVersion ?? undefined,
			});
			const view = resolvedViewsMap.get(cacheKey);
			return determineCdmViewType(view) === "FILE";
		});

		if (fileContainers.length > 0) {
			try {
				const fileInstanceIds = fileContainers.map((c) => ({
					space: c.properties.instanceSpace,
					externalId: c.properties.instanceExternalId,
				}));

				const files = await schedule(() =>
					sdk.files.retrieve(fileInstanceIds.map((id) => ({ instanceId: id })))
				);

				fileContainers.forEach((container, i) => {
					const nodeType = getNodeTypeFromFileInfo(files[i]);
					if (nodeType) {
						resolvedNodeTypeOverrides.set(
							getContainerKey(container.properties),
							nodeType
						);
					}
				});
			} catch {
				// Falls back to default node type for files
			}
		}
	}

	for (const container of fdmContainers) {
		const props = container.properties;
		const cacheKey = getViewCacheKey({
			space: props.viewSpace,
			externalId: props.viewExternalId,
			version: props.viewVersion ?? undefined,
		});
		const resolvedView = resolvedViewsMap.get(cacheKey);
		const nodeTypeOverride = resolvedNodeTypeOverrides.get(
			getContainerKey(props)
		);

		nodes.push(
			fdmContainerToNode(container, zIndex++, resolvedView, nodeTypeOverride)
		);
	}

	for (const annotation of annotations) {
		nodes.push(annotationToNode(annotation, zIndex++));
	}

	return nodes as CanvasNodeDef[];
}
