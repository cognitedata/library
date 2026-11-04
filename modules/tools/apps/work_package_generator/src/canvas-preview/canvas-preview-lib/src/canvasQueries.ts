/**
 * Canvas data model types (cdf_industrial_canvas views) consumed by
 * aggregateCanvasNodes.
 */

export interface InstanceId {
	space: string;
	externalId: string;
}

export interface FdmInstanceContainerReferenceProperties {
	id?: string | null;
	containerReferenceType: string;
	instanceExternalId: string;
	instanceSpace: string;
	viewExternalId: string;
	viewSpace: string;
	viewVersion?: string | null;
	label?: string | null;
	properties?: Record<string, unknown> | null;
	x?: number | null;
	y?: number | null;
	width?: number | null;
	height?: number | null;
}

export interface FdmInstanceContainerReference extends InstanceId {
	version?: number;
	createdTime?: number;
	lastUpdatedTime?: number;
	properties: FdmInstanceContainerReferenceProperties;
}

export interface CanvasAnnotationProperties {
	id: string;
	annotationType: string;
	containerId?: string | null;
	isSelectable?: boolean | null;
	isDraggable?: boolean | null;
	isResizable?: boolean | null;
	properties?: Record<string, unknown> | null;
}

export interface CanvasAnnotation extends InstanceId {
	version?: number;
	createdTime?: number;
	lastUpdatedTime?: number;
	properties: CanvasAnnotationProperties;
}
