/**
 * Document Annotations Hook
 *
 * Fetches CDM annotations (CogniteDiagramAnnotation) for PDF documents.
 * These annotations represent detected links to assets, files, timeseries, etc.
 * on P&ID diagrams and other technical documents.
 *
 * Based on fusion's useContainerAnnotations but adapted for dune-canvas paradigm.
 */

import type { CogniteClient, EdgeDefinition } from "@cognite/sdk";
import { useQuery } from "@tanstack/react-query";
import type { CdfSchedule } from "../../../cdfSchedule";
import type { FdmReference, FileSource } from "../adapters/dataSources";
import { useCdfSchedule, useCogniteClientOptional } from "../sdk/SDKProvider";

// ============================================================================
// Types
// ============================================================================

/**
 * Resource types that annotations can link to
 */
export type AnnotationResourceType =
	| "asset"
	| "file"
	| "timeSeries"
	| "sequence"
	| "event"
	| "unknown";

/**
 * A document annotation with its bounding box and metadata
 */
export interface DocumentAnnotation {
	/** Unique identifier for the annotation */
	id: string;
	/** Annotation bounding box (normalized 0-1 coordinates relative to page) */
	x: number;
	y: number;
	width: number;
	height: number;
	/** Page number the annotation is on (1-indexed) */
	page: number;
	/** The resource type this annotation links to */
	resourceType: AnnotationResourceType;
	/** The linked resource instance (if available) */
	linkedResource?: {
		space: string;
		externalId: string;
	};
	/** Text content of the annotation (e.g., tag name) */
	text?: string;
	/** Raw annotation type from CDM */
	annotationType: string;
}

/**
 * Query result for document annotations
 */
export interface UseDocumentAnnotationsResult {
	annotations: DocumentAnnotation[];
	isLoading: boolean;
	error: Error | null;
}

// ============================================================================
// CDM View Constants
// ============================================================================

const CDM_SPACE = "cdf_cdm";
const CDM_VERSION = "v1";
const DIAGRAM_ANNOTATION_VIEW = "CogniteDiagramAnnotation";
const LIMIT = 10000;

interface CdmAnnotationProperties {
	status?: string;
	startNodeText?: string;
	startNodeYMax?: number;
	startNodeYMin?: number;
	startNodeXMax?: number;
	startNodeXMin?: number;
	endNodeText?: string;
	endNodeYMax?: number;
	endNodeYMin?: number;
	endNodeXMax?: number;
	endNodeXMin?: number;
	startNodePageNumber?: number;
	endNodePageNumber?: number;
}

interface FlattenedAnnotation {
	type?: { externalId: string };
	startNode?: { space: string; externalId: string };
	endNode?: { space: string; externalId: string };
	externalId: string;
	space: string;
	properties: CdmAnnotationProperties;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Extracts resource type from CDM annotation type
 */
function getResourceTypeFromAnnotationType(
	annotationType: string
): AnnotationResourceType {
	const lowerType = annotationType.toLowerCase();

	if (lowerType.includes("asset")) return "asset";
	if (lowerType.includes("file")) return "file";
	if (lowerType.includes("timeseries") || lowerType.includes("time_series"))
		return "timeSeries";
	if (lowerType.includes("sequence")) return "sequence";
	if (lowerType.includes("event")) return "event";

	return "unknown";
}

/**
 * Converts raw CDM annotation to DocumentAnnotation
 */
function toDocumentAnnotation(
	rawAnnotation: FlattenedAnnotation,
	containerId: string
): DocumentAnnotation {
	const props = rawAnnotation.properties;
	const annotationType = rawAnnotation.type?.externalId ?? "diagrams.AssetLink";

	// Extract bounding box coordinates
	const xMin = Number(props.startNodeXMin ?? 0);
	const xMax = Number(props.startNodeXMax ?? 0);
	const yMin = Number(props.startNodeYMin ?? 0);
	const yMax = Number(props.startNodeYMax ?? 0);

	return {
		id: `${containerId}-${rawAnnotation.space}-${rawAnnotation.externalId}`,
		x: Math.min(xMin, xMax),
		y: Math.min(yMin, yMax),
		width: Math.abs(xMax - xMin),
		height: Math.abs(yMax - yMin),
		page: Number(props.startNodePageNumber ?? 1),
		resourceType: getResourceTypeFromAnnotationType(annotationType),
		linkedResource: rawAnnotation.endNode
			? {
					space: rawAnnotation.endNode.space,
					externalId: rawAnnotation.endNode.externalId,
				}
			: undefined,
		text: props.startNodeText ?? undefined,
		annotationType,
	};
}

// ============================================================================
// Hook
// ============================================================================

/**
 * Hook to fetch document annotations for a file
 *
 * @param fileSource - The file source (FDM reference required)
 * @param currentPage - Current page number to filter annotations
 * @param options - Additional options
 *
 * @example
 * ```tsx
 * const { annotations, isLoading, error } = useDocumentAnnotations(
 *   data.source,
 *   currentPage,
 *   { enabled: showAnnotations }
 * );
 * ```
 */
export function useDocumentAnnotations(
	fileSource: FileSource | undefined,
	currentPage: number = 1,
	options?: { enabled?: boolean }
): UseDocumentAnnotationsResult {
	const client = useCogniteClientOptional();
	const schedule = useCdfSchedule();
	const enabled = options?.enabled ?? true;

	// Extract FDM instance info from file source
	const fdmInstance =
		fileSource?.type === "fdm" ? fileSource.instance : undefined;

	const query = useQuery({
		queryKey: [
			"document-annotations",
			fdmInstance?.space,
			fdmInstance?.externalId,
			currentPage,
		],
		queryFn: async (): Promise<DocumentAnnotation[]> => {
			if (!fdmInstance || !client) {
				return [];
			}

			try {
				// Query CDM for diagram annotations on this file
				const annotations = await fetchCdmAnnotations(
					client,
					fdmInstance,
					currentPage,
					schedule
				);
				return annotations;
			} catch (err) {
				console.error("Failed to fetch document annotations:", err);
				throw err;
			}
		},
		// Only enable if client is available (SDK required for FDM annotations)
		enabled: enabled && !!client && fdmInstance !== undefined,
		staleTime: 5 * 60 * 1000, // 5 minutes
	});

	return {
		annotations: query.data ?? [],
		isLoading: query.isLoading,
		error: query.error as Error | null,
	};
}

/**
 * Fetches CDM annotations for a file instance
 */
async function fetchCdmAnnotations(
	client: CogniteClient,
	fileInstance: FdmReference,
	pageNumber: number,
	schedule: CdfSchedule
): Promise<DocumentAnnotation[]> {
	const containerId = `${fileInstance.space}:${fileInstance.externalId}`;
	let cursor: string | undefined;
	const allAnnotations: EdgeDefinition[] = [];

	try {
		do {
			const response = await schedule(() =>
				client.instances.query({
					with: {
						files: {
							nodes: {
								filter: {
									and: [
										{
											equals: {
												property: ["node", "externalId"],
												value: fileInstance.externalId,
											},
										},
										{
											equals: {
												property: ["node", "space"],
												value: fileInstance.space,
											},
										},
									],
								},
							},
						},
						annotations: {
							edges: {
								from: "files",
								direction: "outwards",
							},
							limit: LIMIT,
						},
					},
					select: {
						annotations: {
							sources: [
								{
									source: {
										externalId: DIAGRAM_ANNOTATION_VIEW,
										space: CDM_SPACE,
										type: "view" as const,
										version: CDM_VERSION,
									},
									properties: [
										"status",
										"startNodeText",
										"startNodeYMax",
										"startNodeYMin",
										"startNodeXMax",
										"startNodeXMin",
										"endNodeText",
										"endNodeYMax",
										"endNodeYMin",
										"endNodeXMax",
										"endNodeXMin",
										"startNodePageNumber",
										"endNodePageNumber",
									],
								},
							],
							limit: LIMIT,
						},
					},
					cursors: cursor ? { annotations: cursor } : undefined,
				})
			);

			cursor =
				(response.items?.annotations?.length ?? 0) < LIMIT
					? undefined
					: response.nextCursor?.annotations;

			if (response.items?.annotations) {
				allAnnotations.push(
					...response.items.annotations.filter((a) => a.instanceType === "edge")
				);
			}
		} while (cursor);

		// Filter annotations that have properties and are on the current page
		// Also filter out rejected annotations
		const filteredAnnotations = allAnnotations
			.filter((annotation) => {
				const props =
					annotation.properties?.[CDM_SPACE]?.[
						`${DIAGRAM_ANNOTATION_VIEW}/${CDM_VERSION}`
					];
				if (!props) {
					return false;
				}

				// Filter by page number
				const annotationPage = Number(props.startNodePageNumber ?? 1);
				if (annotationPage !== pageNumber) return false;

				// Filter out rejected annotations
				if (props.status === "Rejected") return false;

				return true;
			})
			.map((annotation): FlattenedAnnotation => {
				const props = annotation.properties![CDM_SPACE][
					`${DIAGRAM_ANNOTATION_VIEW}/${CDM_VERSION}`
				] as CdmAnnotationProperties;
				return {
					type: annotation.type,
					startNode: annotation.startNode,
					endNode: annotation.endNode,
					externalId: annotation.externalId,
					space: annotation.space,
					properties: props,
				};
			});

		return filteredAnnotations.map((annotation) =>
			toDocumentAnnotation(annotation, containerId)
		);
	} catch (err) {
		console.error("[useDocumentAnnotations] Query failed:", err);
		throw err;
	}
}

/**
 * Query key factory for document annotations
 */
export const documentAnnotationKeys = {
	all: ["document-annotations"] as const,
	file: (space: string, externalId: string, page: number) =>
		[...documentAnnotationKeys.all, space, externalId, page] as const,
};
