/**
 * Work Package Generator Types
 * TypeScript interfaces transpiled from Python Streamlit app
 */

import type { DirectRelationReference, ViewReference } from "@cognite/sdk";

// ============================================================================
// Work Order Types
// ============================================================================

export interface WorkOrder {
	externalId: string;
	space: string;
	name?: string;
	description?: string;
	type?: string;
	status?: string;
	priority?: string;
	category?: string;
	workOrderType?: string;
	maintenanceType?: string;
	sourceId?: string;
	scheduledStartTime?: string;
	scheduledEndTime?: string;
	estimatedDuration?: number;
	actualStartTime?: string;
	actualEndTime?: string;
	workOrderNumber?: string;
	requester?: string;
	assignedTo?: string;
	location?: string;
	equipment?: string;
	system?: string;
	frequency?: string;
	criticality?: string;
	assets?: DirectRelationReference[];
}

// ============================================================================
// Asset Types
// ============================================================================

export interface Asset {
	assetExternalId: string;
	space: string;
	assetName?: string;
	description?: string;
	type?: string;
	category?: string;
	subcategory?: string;
	assetType?: string;
	criticality?: string;
	status?: string;
	location?: string;
	parentAsset?: string;
	parentAssetExternalId?: string;
	hierarchy?: string;
	specifications?: string;
	manufacturer?: string;
	model?: string;
	serialNumber?: string;
	installationDate?: string;
	operatingConditions?: string;
	capacity?: string;
	powerRating?: string;
	pressureRating?: string;
	temperatureRating?: string;
	flowRate?: string;
	dimensions?: string;
	weight?: string;
	material?: string;
	process?: string;
	area?: string;
	zone?: string;
	building?: string;
	floor?: string;
	room?: string;
}

// ============================================================================
// Resource Instance Types
// ============================================================================

/** Identity of a resource the user (or AI) picked for a canvas slot. */
export interface Selection {
	externalId: string;
	space: string;
	name?: string;
	description?: string;
}

/** A CDF resource candidate; a {@link Selection} plus view properties. */
export interface ResourceInstance extends Selection {
	properties: Record<string, unknown>;
}

// ============================================================================
// Template Types
// ============================================================================

export interface TemplateCanvas {
	externalId: string;
	name: string;
	visibility: "public" | "private";
	createdBy?: string;
	updatedBy?: string;
	createdAt?: string;
	updatedAt?: string;
	space: string;
}

export interface TemplateField {
	type: string;
	label: string;
	viewExternalId?: string;
	viewSpace?: string;
	viewVersion?: string;
}

export interface TemplateInstance {
	label: string;
	viewExternalId: string;
	viewSpace: string;
	viewVersion: string;
	instanceExternalId?: string;
	instanceSpace?: string;
	description?: string;
	x: number;
	y: number;
	width: number;
	height: number;
	maxWidth: number;
	maxHeight: number;
	properties: Record<string, unknown>;
}

export interface TemplateAnnotation {
	type: string;
	properties: Record<string, unknown>;
}

// ============================================================================
// Selection Types
// ============================================================================

export type AiSelections = Record<string, Selection | null>;

// ============================================================================
// Canvas Item Types
// ============================================================================

export interface CanvasItem {
	type: "fdmInstance" | "CanvasAnnotation";
	containerId: string;
	instanceExternalId?: string;
	instanceSpace?: string;
	viewExternalId?: string;
	viewSpace?: string;
	viewVersion?: string;
	label?: string;
	properties?: Record<string, unknown>;
	x?: number;
	y?: number;
	width?: number;
	height?: number;
	maxWidth?: number;
	maxHeight?: number;
	annotationType?: string;
}

// ============================================================================
// Configuration Types
// ============================================================================

export interface ResourceConfig {
	name: string;
	displayName: string;
	viewId: ViewReference;
	properties: string[];
	resourceName: string;
	trackingKey: string;
	assetRelationProperty: string;
	assetRelationIsArray: boolean;
	/**
	 * The property identifier on the CogniteActivity container that holds
	 * a relation to this resource type (e.g. "timeSeries", "files").
	 * Used for graph-traversal queries from a work order node.
	 */
	activityContainerIdentifier?: string;
}

export interface ContextConfig {
	viewId: ViewReference;
	properties: string[];
}

export interface CanvasConfig {
	canvasSpaceCanvas: string;
	canvasSpaceInstance: string;
	canvasContainerCanvas: string;
	canvasInstanceContainerId: string;
	canvasAnnotationContainerId: string;
}

// ============================================================================
// Validation Types
// ============================================================================

export interface ValidationResult {
	success: boolean;
	canvasExists: boolean;
	itemsCount: number;
	expectedItemsCount: number;
	itemsMatchExpected: boolean;
	canvasId: string;
	errors: string[];
}

// ============================================================================
// Filter Types
// ============================================================================

export interface WorkOrderFilters {
	dateFrom?: string;
	dateTo?: string;
	status?: string;
	priority?: string;
}

// ============================================================================
// UI State Types
// ============================================================================

export type WorkflowStep =
	| "work-order-selection"
	| "canvas-selection"
	| "canvas-generation"
	| "ai-selection"
	| "canvas-creation";
