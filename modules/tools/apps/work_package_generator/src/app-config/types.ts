/**
 * Customer configuration loaded from the WPG config node.
 * Every field is required. There are no in-code fallbacks.
 */

export interface ViewIdConfig {
	space: string;
	externalId: string;
	version: string;
}

export interface ContainerIdConfig {
	space: string;
	externalId: string;
}

export interface ResourceTypeConfig {
	displayName: string;
	/** Key into {@link AppConfig.dataModel.views}. */
	view: string;
	properties: string[];
	assetRelationProperty: string;
	assetRelationIsArray: boolean;
	activityContainerIdentifier?: string;
}

export interface CanvasDimensionsConfig {
	x: number;
	y: number;
	width: number;
	height: number;
	maxWidth: number;
	maxHeight: number;
}

export interface ClusterSettings {
	apiHostSuffix: string;
	hostAliases: Record<string, string>;
}

export const REQUIRED_DATA_MODEL_VIEWS = [
	"asset",
	"equipment",
	"activity",
	"timeSeries",
	"file",
	"solutionTag",
] as const;

export type DataModelViewKey = (typeof REQUIRED_DATA_MODEL_VIEWS)[number];

export const REQUIRED_RESOURCE_TYPES = [
	"activity",
	"asset",
	"equipment",
	"file",
	"timeseries",
] as const;

/** Display stepper pills. `config.ui.stepperLabels` must have this length. */
export const WORKFLOW_STEP_COUNT = 5;

export const RESOURCE_SELECTION_PLACEHOLDERS = [
	"workOrderSection",
	"assetSection",
	"resourceName",
	"resourceNameUpper",
	"label",
	"resourceType",
	"optionsText",
] as const;

export const TEMPLATE_SELECTION_PLACEHOLDERS = [
	"workOrderName",
	"workOrderType",
	"workOrderDescription",
	"assetName",
	"assetType",
	"assetDescription",
	"resourcesSection",
	"canvasSection",
] as const;

export interface CanvasStickyStyle {
	backgroundColor: string;
	color: string;
	borderColor: string;
	borderWidth: number;
	borderRadius: number;
}

export interface WorkOrderPropertyNames {
	name: string;
	description: string;
	type: string;
	status: string;
	priority: string;
	scheduledStartTime: string;
	scheduledEndTime: string;
	workOrderNumber: string;
	sourceId: string;
	assets: string;
}

export interface AgentToolConfig {
	[key: string]: unknown;
}

export interface AppConfig {
	dataModel: {
		views: Record<DataModelViewKey, ViewIdConfig> &
			Record<string, ViewIdConfig>;
		activityContainer: ContainerIdConfig;
	};
	canvas: {
		schemaSpace: string;
		instanceSpace: string;
		canvasView: ViewIdConfig;
		instanceView: ViewIdConfig;
		annotationView: ViewIdConfig;
		dimensions: CanvasDimensionsConfig;
		userId: string;
		visibility: "public" | "private";
		style: CanvasStickyStyle;
	};
	resourceTypes: Record<string, ResourceTypeConfig>;
	context: {
		workOrder: { properties: string[] };
		asset: { properties: string[] };
	};
	agent: {
		model: string;
		runtimeVersion: string;
		externalIdSuffix: string;
		name: string;
		description: string;
		instructions: string;
		/** When set, the app uses this agent and does not create one. */
		existingAgentExternalId?: string;
		tools: AgentToolConfig[];
	};
	fusion: {
		/** Host suffix including the leading dot, e.g. `.fusion.cognite.com`. */
		hostSuffix: string;
		canvasWorkspace: string;
		/**
		 * Org slug used only when the host frame is not Fusion.
		 * Omitted when the config node leaves it empty.
		 */
		fallbackOrganization?: string;
	};
	workOrderProperties: WorkOrderPropertyNames;
	prompts: {
		resourceSelection: string;
		templateSelection: string;
	};
	ui: {
		workOrderStatusOptions: string[];
		workOrderPriorityOptions: string[];
		templateInstructionExamples: {
			workOrderId: string;
			functionalLocationId: string;
		};
		terms: {
			workOrder: string;
			workPackage: string;
			functionalLocation: string;
		};
		stepperLabels: string[];
	};
	templates: {
		namePrefix: string;
		baseTemplate: {
			id: string;
			name: string;
			visibility: "public" | "private";
			space: string;
		};
	};
	solutionTag: {
		instanceSpace: string;
		externalId: string;
		name: string;
	};
	limits: {
		instancePageSize: number;
		upsertBatchSize: number;
		deleteBatchSize: number;
		searchResultLimit: number;
		resourcesDisplayLimit: number;
		maxCanvasListScanItems: number;
		maxResourceInstancesTotal: number;
		maxTemplateCanvasChildItems: number;
		maxCanvasCleanupItems: number;
		findCanvasByNameMaxItems: number;
	};
	/** Host-suffix and alias table from the config node's `cluster` section. */
	clusterSettings: ClusterSettings;
}
