/**
 * Strict config-node parser. Collects every missing, unknown, and mistyped path
 * before throwing so a customer can fix the node in one pass.
 * Unknown top-level properties are ignored. Keys inside a section are strict.
 */

import { AppConfigError } from "./errors";
import {
	type AgentToolConfig,
	type AppConfig,
	type ClusterSettings,
	type ContainerIdConfig,
	REQUIRED_DATA_MODEL_VIEWS,
	REQUIRED_RESOURCE_TYPES,
	RESOURCE_SELECTION_PLACEHOLDERS,
	type ResourceTypeConfig,
	TEMPLATE_SELECTION_PLACEHOLDERS,
	type ViewIdConfig,
	WORKFLOW_STEP_COUNT,
} from "./types";

/** CDF instances.list / upsert / delete accept at most 1000 items per request. */
const DMS_BATCH_MAX = 1000;
const GENERAL_LIMIT_MAX = 1_000_000;

/** Content shape this app can read. Independent of the DMS view version. */
export const SUPPORTED_CONFIG_SCHEMA_VERSION = 1;

export interface ParsedConfigNode {
	dataModel: AppConfig["dataModel"];
	canvas: AppConfig["canvas"];
	resourceTypes: AppConfig["resourceTypes"];
	context: AppConfig["context"];
	agent: AppConfig["agent"];
	templates: AppConfig["templates"];
	solutionTag: AppConfig["solutionTag"];
	limits: AppConfig["limits"];
	clusterSettings: ClusterSettings;
	fusion: AppConfig["fusion"];
	workOrderProperties: AppConfig["workOrderProperties"];
	prompts: AppConfig["prompts"];
	ui: AppConfig["ui"];
}

export function parseConfigNode(raw: unknown): ParsedConfigNode {
	const issues: string[] = [];
	const node = readObject(raw, "WPGConfig", issues);
	if (!node) throw new AppConfigError(issues);

	readSchemaVersion(node, issues);

	const dataModel = readDataModel(node.dataModel, issues);
	const canvas = readCanvas(node.canvas, issues);
	const resourceTypes = readResourceTypes(node.resourceTypes, issues);
	const context = readContext(node.context, issues);
	const agent = readAgent(node.agent, issues);
	const templates = readTemplates(node.templates, issues);
	const solutionTag = readSolutionTag(node.solutionTag, issues);
	const limits = readLimits(node.limits, issues);
	const clusterSettings = readClusterSettings(node.cluster, issues);
	const fusion = readFusion(node.fusion, issues);
	const workOrderProperties = readWorkOrderProperties(
		node.workOrderProperties,
		issues
	);
	const prompts = readPrompts(node.prompts, issues);
	const ui = readUi(node.ui, issues);

	if (dataModel && resourceTypes) {
		for (const [typeKey, resourceType] of Object.entries(resourceTypes)) {
			if (!(resourceType.view in dataModel.views)) {
				issues.push(
					`WPGConfig.resourceTypes.${typeKey}.view: unknown view "${resourceType.view}"`
				);
			}
		}
	}

	if (
		issues.length > 0 ||
		!dataModel ||
		!canvas ||
		!resourceTypes ||
		!context ||
		!agent ||
		!templates ||
		!solutionTag ||
		!limits ||
		!clusterSettings ||
		!fusion ||
		!workOrderProperties ||
		!prompts ||
		!ui
	) {
		throw new AppConfigError(
			issues.length > 0 ? issues : ["WPGConfig: invalid configuration"]
		);
	}

	return {
		dataModel,
		canvas,
		resourceTypes,
		context,
		agent,
		templates,
		solutionTag,
		limits,
		clusterSettings,
		fusion,
		workOrderProperties,
		prompts,
		ui,
	};
}

function readSchemaVersion(
	node: Record<string, unknown>,
	issues: string[]
): void {
	const version = readInteger(node, "schemaVersion", "WPGConfig", issues, 1);
	if (version === null || version === SUPPORTED_CONFIG_SCHEMA_VERSION) return;
	issues.push(
		`WPGConfig.schemaVersion: unsupported version ${version}; this app supports ${SUPPORTED_CONFIG_SCHEMA_VERSION}`
	);
}

function readDataModel(
	value: unknown,
	issues: string[]
): AppConfig["dataModel"] | null {
	const dataModel = readObject(value, "WPGConfig.dataModel", issues);
	if (!dataModel) return null;
	rejectUnknown(
		dataModel,
		["views", "activityContainer"],
		"WPGConfig.dataModel",
		issues
	);

	const views = readObject(
		dataModel.views,
		"WPGConfig.dataModel.views",
		issues
	);
	const parsedViews: Record<string, ViewIdConfig> = {};
	if (views) {
		for (const key of REQUIRED_DATA_MODEL_VIEWS) {
			if (!(key in views)) {
				issues.push(`WPGConfig.dataModel.views.${key}: required`);
			}
		}
		for (const [key, viewValue] of Object.entries(views)) {
			const view = readViewId(
				viewValue,
				`WPGConfig.dataModel.views.${key}`,
				issues
			);
			if (view) parsedViews[key] = view;
		}
	}

	const activityContainer = readContainerId(
		dataModel.activityContainer,
		"WPGConfig.dataModel.activityContainer",
		issues
	);

	if (!views || !activityContainer) return null;
	for (const key of REQUIRED_DATA_MODEL_VIEWS) {
		if (!parsedViews[key]) return null;
	}

	return {
		views: parsedViews as AppConfig["dataModel"]["views"],
		activityContainer,
	};
}

function readCanvas(
	value: unknown,
	issues: string[]
): AppConfig["canvas"] | null {
	const canvas = readObject(value, "WPGConfig.canvas", issues);
	if (!canvas) return null;
	rejectUnknown(
		canvas,
		[
			"schemaSpace",
			"instanceSpace",
			"canvasView",
			"instanceView",
			"annotationView",
			"dimensions",
			"userId",
			"visibility",
			"style",
		],
		"WPGConfig.canvas",
		issues
	);

	const schemaSpace = readRequiredString(
		canvas,
		"schemaSpace",
		"WPGConfig.canvas",
		issues
	);
	const instanceSpace = readRequiredString(
		canvas,
		"instanceSpace",
		"WPGConfig.canvas",
		issues
	);
	const userId = readRequiredString(
		canvas,
		"userId",
		"WPGConfig.canvas",
		issues
	);
	const canvasView = readViewId(
		canvas.canvasView,
		"WPGConfig.canvas.canvasView",
		issues
	);
	const instanceView = readViewId(
		canvas.instanceView,
		"WPGConfig.canvas.instanceView",
		issues
	);
	const annotationView = readViewId(
		canvas.annotationView,
		"WPGConfig.canvas.annotationView",
		issues
	);
	const dimensions = readDimensions(canvas.dimensions, issues);
	const visibility = readVisibility(
		canvas.visibility,
		"WPGConfig.canvas.visibility",
		issues
	);
	const style = readCanvasStyle(canvas.style, issues);

	if (
		!schemaSpace ||
		!instanceSpace ||
		!userId ||
		!canvasView ||
		!instanceView ||
		!annotationView ||
		!dimensions ||
		!visibility ||
		!style
	) {
		return null;
	}

	return {
		schemaSpace,
		instanceSpace,
		canvasView,
		instanceView,
		annotationView,
		dimensions,
		userId,
		visibility,
		style,
	};
}

function readDimensions(
	value: unknown,
	issues: string[]
): AppConfig["canvas"]["dimensions"] | null {
	const dimensions = readObject(value, "WPGConfig.canvas.dimensions", issues);
	if (!dimensions) return null;
	rejectUnknown(
		dimensions,
		["x", "y", "width", "height", "maxWidth", "maxHeight"],
		"WPGConfig.canvas.dimensions",
		issues
	);
	const x = readInteger(
		dimensions,
		"x",
		"WPGConfig.canvas.dimensions",
		issues,
		0
	);
	const y = readInteger(
		dimensions,
		"y",
		"WPGConfig.canvas.dimensions",
		issues,
		0
	);
	const width = readInteger(
		dimensions,
		"width",
		"WPGConfig.canvas.dimensions",
		issues,
		1
	);
	const height = readInteger(
		dimensions,
		"height",
		"WPGConfig.canvas.dimensions",
		issues,
		1
	);
	const maxWidth = readInteger(
		dimensions,
		"maxWidth",
		"WPGConfig.canvas.dimensions",
		issues,
		1
	);
	const maxHeight = readInteger(
		dimensions,
		"maxHeight",
		"WPGConfig.canvas.dimensions",
		issues,
		1
	);
	if (
		x === null ||
		y === null ||
		width === null ||
		height === null ||
		maxWidth === null ||
		maxHeight === null
	) {
		return null;
	}
	return { x, y, width, height, maxWidth, maxHeight };
}

function readResourceTypes(
	value: unknown,
	issues: string[]
): AppConfig["resourceTypes"] | null {
	const resourceTypes = readObject(value, "WPGConfig.resourceTypes", issues);
	if (!resourceTypes) return null;

	for (const key of REQUIRED_RESOURCE_TYPES) {
		if (!(key in resourceTypes)) {
			issues.push(`WPGConfig.resourceTypes.${key}: required`);
		}
	}

	const parsed: Record<string, ResourceTypeConfig> = {};
	for (const [key, entry] of Object.entries(resourceTypes)) {
		const resourceType = readResourceType(
			entry,
			`WPGConfig.resourceTypes.${key}`,
			issues
		);
		if (resourceType) parsed[key] = resourceType;
	}

	for (const key of REQUIRED_RESOURCE_TYPES) {
		if (!parsed[key]) return null;
	}
	return parsed;
}

function readResourceType(
	value: unknown,
	path: string,
	issues: string[]
): ResourceTypeConfig | null {
	const entry = readObject(value, path, issues);
	if (!entry) return null;
	rejectUnknown(
		entry,
		[
			"displayName",
			"view",
			"properties",
			"assetRelationProperty",
			"assetRelationIsArray",
			"activityContainerIdentifier",
		],
		path,
		issues
	);
	const displayName = readRequiredString(entry, "displayName", path, issues);
	const view = readRequiredString(entry, "view", path, issues);
	const assetRelationProperty = readRequiredString(
		entry,
		"assetRelationProperty",
		path,
		issues
	);
	const properties = readStringArray(
		entry.properties,
		`${path}.properties`,
		issues
	);
	let assetRelationIsArray: boolean | null = null;
	if (!("assetRelationIsArray" in entry)) {
		issues.push(`${path}.assetRelationIsArray: required`);
	} else if (typeof entry.assetRelationIsArray !== "boolean") {
		issues.push(`${path}.assetRelationIsArray: expected boolean`);
	} else {
		assetRelationIsArray = entry.assetRelationIsArray;
	}

	let activityContainerIdentifier: string | undefined;
	if ("activityContainerIdentifier" in entry) {
		const identifier = readRequiredString(
			entry,
			"activityContainerIdentifier",
			path,
			issues
		);
		if (identifier) activityContainerIdentifier = identifier;
	}

	if (
		!displayName ||
		!view ||
		!properties ||
		!assetRelationProperty ||
		assetRelationIsArray === null
	) {
		return null;
	}

	return {
		displayName,
		view,
		properties,
		assetRelationProperty,
		assetRelationIsArray,
		...(activityContainerIdentifier ? { activityContainerIdentifier } : {}),
	};
}

function readContext(
	value: unknown,
	issues: string[]
): AppConfig["context"] | null {
	const context = readObject(value, "WPGConfig.context", issues);
	if (!context) return null;
	rejectUnknown(context, ["workOrder", "asset"], "WPGConfig.context", issues);

	const workOrder = readContextEntry(
		context.workOrder,
		"WPGConfig.context.workOrder",
		issues
	);
	const asset = readContextEntry(
		context.asset,
		"WPGConfig.context.asset",
		issues
	);
	if (!workOrder || !asset) return null;
	return { workOrder, asset };
}

function readContextEntry(
	value: unknown,
	path: string,
	issues: string[]
): { properties: string[] } | null {
	const entry = readObject(value, path, issues);
	if (!entry) return null;
	rejectUnknown(entry, ["properties"], path, issues);
	const properties = readStringArray(
		entry.properties,
		`${path}.properties`,
		issues
	);
	if (!properties) return null;
	return { properties };
}

function readAgent(
	value: unknown,
	issues: string[]
): AppConfig["agent"] | null {
	const agent = readObject(value, "WPGConfig.agent", issues);
	if (!agent) return null;
	rejectUnknown(
		agent,
		[
			"model",
			"runtimeVersion",
			"externalIdSuffix",
			"name",
			"description",
			"instructions",
			"existingAgentExternalId",
			"tools",
		],
		"WPGConfig.agent",
		issues
	);
	const model = readRequiredString(agent, "model", "WPGConfig.agent", issues);
	const runtimeVersion = readRequiredString(
		agent,
		"runtimeVersion",
		"WPGConfig.agent",
		issues
	);
	const externalIdSuffix = readRequiredString(
		agent,
		"externalIdSuffix",
		"WPGConfig.agent",
		issues
	);
	const name = readRequiredString(agent, "name", "WPGConfig.agent", issues);
	const description = readRequiredString(
		agent,
		"description",
		"WPGConfig.agent",
		issues
	);
	const instructions = readString(
		agent,
		"instructions",
		"WPGConfig.agent",
		issues,
		{
			allowEmpty: true,
		}
	);
	let existingAgentExternalId: string | undefined;
	if (
		"existingAgentExternalId" in agent &&
		agent.existingAgentExternalId !== undefined
	) {
		const existing = readString(
			agent,
			"existingAgentExternalId",
			"WPGConfig.agent",
			issues
		);
		if (existing) existingAgentExternalId = existing;
	}
	const tools = readAgentTools(agent.tools, issues);
	if (
		!model ||
		!runtimeVersion ||
		!externalIdSuffix ||
		!name ||
		!description ||
		instructions === null ||
		!tools
	) {
		return null;
	}
	return {
		model,
		runtimeVersion,
		externalIdSuffix,
		name,
		description,
		instructions,
		...(existingAgentExternalId ? { existingAgentExternalId } : {}),
		tools,
	};
}

function readTemplates(
	value: unknown,
	issues: string[]
): AppConfig["templates"] | null {
	const templates = readObject(value, "WPGConfig.templates", issues);
	if (!templates) return null;
	rejectUnknown(
		templates,
		["namePrefix", "baseTemplate"],
		"WPGConfig.templates",
		issues
	);
	const namePrefix = readRequiredString(
		templates,
		"namePrefix",
		"WPGConfig.templates",
		issues
	);
	const base = readObject(
		templates.baseTemplate,
		"WPGConfig.templates.baseTemplate",
		issues
	);
	if (!base || !namePrefix) return null;
	rejectUnknown(
		base,
		["id", "name", "visibility", "space"],
		"WPGConfig.templates.baseTemplate",
		issues
	);
	const id = readRequiredString(
		base,
		"id",
		"WPGConfig.templates.baseTemplate",
		issues
	);
	const name = readRequiredString(
		base,
		"name",
		"WPGConfig.templates.baseTemplate",
		issues
	);
	const space = readRequiredString(
		base,
		"space",
		"WPGConfig.templates.baseTemplate",
		issues
	);
	let visibility: "public" | "private" | null = null;
	if (!("visibility" in base)) {
		issues.push("WPGConfig.templates.baseTemplate.visibility: required");
	} else if (base.visibility !== "public" && base.visibility !== "private") {
		issues.push(
			'WPGConfig.templates.baseTemplate.visibility: expected "public" or "private"'
		);
	} else {
		visibility = base.visibility;
	}
	if (!id || !name || !space || !visibility) return null;
	return { namePrefix, baseTemplate: { id, name, visibility, space } };
}

function readFusion(
	value: unknown,
	issues: string[]
): AppConfig["fusion"] | null {
	const fusion = readObject(value, "WPGConfig.fusion", issues);
	if (!fusion) return null;
	rejectUnknown(
		fusion,
		["hostSuffix", "canvasWorkspace", "fallbackOrganization"],
		"WPGConfig.fusion",
		issues
	);
	const hostSuffix = readRequiredString(
		fusion,
		"hostSuffix",
		"WPGConfig.fusion",
		issues
	);
	const canvasWorkspace = readRequiredString(
		fusion,
		"canvasWorkspace",
		"WPGConfig.fusion",
		issues
	);
	const fallbackOrganization = readFallbackOrganization(fusion, issues);
	if (hostSuffix && !hostSuffix.startsWith(".")) {
		issues.push(
			'WPGConfig.fusion.hostSuffix: expected a suffix starting with "."'
		);
	}
	if (
		!hostSuffix ||
		!canvasWorkspace ||
		!hostSuffix.startsWith(".") ||
		fallbackOrganization === null
	) {
		return null;
	}
	return {
		hostSuffix,
		canvasWorkspace,
		...(fallbackOrganization ? { fallbackOrganization } : {}),
	};
}

/** Empty or omitted means no fallback. A non-string is an error (`null`). */
function readFallbackOrganization(
	fusion: Record<string, unknown>,
	issues: string[]
): string | undefined | null {
	if (
		!("fallbackOrganization" in fusion) ||
		fusion.fallbackOrganization === undefined ||
		fusion.fallbackOrganization === ""
	) {
		return undefined;
	}
	if (typeof fusion.fallbackOrganization !== "string") {
		issues.push("WPGConfig.fusion.fallbackOrganization: expected string");
		return null;
	}
	if (fusion.fallbackOrganization.trim() === "") {
		issues.push(
			"WPGConfig.fusion.fallbackOrganization: must be a non-empty string"
		);
		return null;
	}
	return fusion.fallbackOrganization;
}

const WORK_ORDER_PROPERTY_KEYS = [
	"name",
	"description",
	"type",
	"status",
	"priority",
	"scheduledStartTime",
	"scheduledEndTime",
	"workOrderNumber",
	"sourceId",
	"assets",
] as const;

function readWorkOrderProperties(
	value: unknown,
	issues: string[]
): AppConfig["workOrderProperties"] | null {
	const props = readObject(value, "WPGConfig.workOrderProperties", issues);
	if (!props) return null;
	rejectUnknown(
		props,
		WORK_ORDER_PROPERTY_KEYS,
		"WPGConfig.workOrderProperties",
		issues
	);
	const parsed = {} as AppConfig["workOrderProperties"];
	let valid = true;
	for (const key of WORK_ORDER_PROPERTY_KEYS) {
		const propertyName = readRequiredString(
			props,
			key,
			"WPGConfig.workOrderProperties",
			issues
		);
		if (!propertyName) {
			valid = false;
			continue;
		}
		parsed[key] = propertyName;
	}
	return valid ? parsed : null;
}

function readPrompts(
	value: unknown,
	issues: string[]
): AppConfig["prompts"] | null {
	const prompts = readObject(value, "WPGConfig.prompts", issues);
	if (!prompts) return null;
	rejectUnknown(
		prompts,
		["resourceSelection", "templateSelection"],
		"WPGConfig.prompts",
		issues
	);
	const resourceSelection = readRequiredString(
		prompts,
		"resourceSelection",
		"WPGConfig.prompts",
		issues
	);
	const templateSelection = readRequiredString(
		prompts,
		"templateSelection",
		"WPGConfig.prompts",
		issues
	);
	if (resourceSelection) {
		requirePlaceholders(
			resourceSelection,
			RESOURCE_SELECTION_PLACEHOLDERS,
			"WPGConfig.prompts.resourceSelection",
			issues
		);
	}
	if (templateSelection) {
		requirePlaceholders(
			templateSelection,
			TEMPLATE_SELECTION_PLACEHOLDERS,
			"WPGConfig.prompts.templateSelection",
			issues
		);
	}
	if (!resourceSelection || !templateSelection) return null;
	const resourceOk = RESOURCE_SELECTION_PLACEHOLDERS.every((token) =>
		resourceSelection.includes(`{{${token}}}`)
	);
	const templateOk = TEMPLATE_SELECTION_PLACEHOLDERS.every((token) =>
		templateSelection.includes(`{{${token}}}`)
	);
	if (!resourceOk || !templateOk) return null;
	return { resourceSelection, templateSelection };
}

function readUi(value: unknown, issues: string[]): AppConfig["ui"] | null {
	const ui = readObject(value, "WPGConfig.ui", issues);
	if (!ui) return null;
	rejectUnknown(
		ui,
		[
			"workOrderStatusOptions",
			"workOrderPriorityOptions",
			"templateInstructionExamples",
			"terms",
			"stepperLabels",
		],
		"WPGConfig.ui",
		issues
	);
	const workOrderStatusOptions = readStringArray(
		ui.workOrderStatusOptions,
		"WPGConfig.ui.workOrderStatusOptions",
		issues
	);
	const workOrderPriorityOptions = readStringArray(
		ui.workOrderPriorityOptions,
		"WPGConfig.ui.workOrderPriorityOptions",
		issues
	);
	const examples = readObject(
		ui.templateInstructionExamples,
		"WPGConfig.ui.templateInstructionExamples",
		issues
	);
	const terms = readObject(ui.terms, "WPGConfig.ui.terms", issues);
	const stepperLabels = readStringArray(
		ui.stepperLabels,
		"WPGConfig.ui.stepperLabels",
		issues
	);
	if (examples) {
		rejectUnknown(
			examples,
			["workOrderId", "functionalLocationId"],
			"WPGConfig.ui.templateInstructionExamples",
			issues
		);
	}
	if (terms) {
		rejectUnknown(
			terms,
			["workOrder", "workPackage", "functionalLocation"],
			"WPGConfig.ui.terms",
			issues
		);
	}
	const workOrderId = examples
		? readRequiredString(
				examples,
				"workOrderId",
				"WPGConfig.ui.templateInstructionExamples",
				issues
			)
		: null;
	const functionalLocationId = examples
		? readRequiredString(
				examples,
				"functionalLocationId",
				"WPGConfig.ui.templateInstructionExamples",
				issues
			)
		: null;
	const workOrder = terms
		? readRequiredString(terms, "workOrder", "WPGConfig.ui.terms", issues)
		: null;
	const workPackage = terms
		? readRequiredString(terms, "workPackage", "WPGConfig.ui.terms", issues)
		: null;
	const functionalLocation = terms
		? readRequiredString(
				terms,
				"functionalLocation",
				"WPGConfig.ui.terms",
				issues
			)
		: null;
	if (stepperLabels && stepperLabels.length !== WORKFLOW_STEP_COUNT) {
		issues.push(
			`WPGConfig.ui.stepperLabels: expected ${WORKFLOW_STEP_COUNT} labels`
		);
	}
	if (
		!workOrderStatusOptions ||
		!workOrderPriorityOptions ||
		!workOrderId ||
		!functionalLocationId ||
		!workOrder ||
		!workPackage ||
		!functionalLocation ||
		!stepperLabels ||
		stepperLabels.length !== WORKFLOW_STEP_COUNT
	) {
		return null;
	}
	return {
		workOrderStatusOptions,
		workOrderPriorityOptions,
		templateInstructionExamples: { workOrderId, functionalLocationId },
		terms: { workOrder, workPackage, functionalLocation },
		stepperLabels,
	};
}

function readVisibility(
	value: unknown,
	path: string,
	issues: string[]
): "public" | "private" | null {
	if (value === undefined) {
		issues.push(`${path}: required`);
		return null;
	}
	if (value !== "public" && value !== "private") {
		issues.push(`${path}: expected "public" or "private"`);
		return null;
	}
	return value;
}

function readCanvasStyle(
	value: unknown,
	issues: string[]
): AppConfig["canvas"]["style"] | null {
	const style = readObject(value, "WPGConfig.canvas.style", issues);
	if (!style) return null;
	rejectUnknown(
		style,
		["backgroundColor", "color", "borderColor", "borderWidth", "borderRadius"],
		"WPGConfig.canvas.style",
		issues
	);
	const backgroundColor = readRequiredString(
		style,
		"backgroundColor",
		"WPGConfig.canvas.style",
		issues
	);
	const color = readRequiredString(
		style,
		"color",
		"WPGConfig.canvas.style",
		issues
	);
	const borderColor = readRequiredString(
		style,
		"borderColor",
		"WPGConfig.canvas.style",
		issues
	);
	const borderWidth = readInteger(
		style,
		"borderWidth",
		"WPGConfig.canvas.style",
		issues,
		0
	);
	const borderRadius = readInteger(
		style,
		"borderRadius",
		"WPGConfig.canvas.style",
		issues,
		0
	);
	if (
		!backgroundColor ||
		!color ||
		!borderColor ||
		borderWidth === null ||
		borderRadius === null
	) {
		return null;
	}
	return { backgroundColor, color, borderColor, borderWidth, borderRadius };
}

function readAgentTools(
	value: unknown,
	issues: string[]
): AgentToolConfig[] | null {
	const path = "WPGConfig.agent.tools";
	if (value === undefined) {
		issues.push(`${path}: required`);
		return null;
	}
	if (!Array.isArray(value)) {
		issues.push(`${path}: expected an array`);
		return null;
	}
	const tools: AgentToolConfig[] = [];
	let valid = true;
	for (let index = 0; index < value.length; index++) {
		const item = value[index];
		if (!item || typeof item !== "object" || Array.isArray(item)) {
			issues.push(`${path}[${index}]: expected an object`);
			valid = false;
			continue;
		}
		tools.push(item as AgentToolConfig);
	}
	return valid ? tools : null;
}

function requirePlaceholders(
	template: string,
	tokens: readonly string[],
	path: string,
	issues: string[]
): void {
	for (const token of tokens) {
		if (!template.includes(`{{${token}}}`)) {
			issues.push(`${path}: missing placeholder {{${token}}}`);
		}
	}
}

function readSolutionTag(
	value: unknown,
	issues: string[]
): AppConfig["solutionTag"] | null {
	const tag = readObject(value, "WPGConfig.solutionTag", issues);
	if (!tag) return null;
	rejectUnknown(
		tag,
		["instanceSpace", "externalId", "name"],
		"WPGConfig.solutionTag",
		issues
	);
	const instanceSpace = readRequiredString(
		tag,
		"instanceSpace",
		"WPGConfig.solutionTag",
		issues
	);
	const externalId = readRequiredString(
		tag,
		"externalId",
		"WPGConfig.solutionTag",
		issues
	);
	const name = readRequiredString(tag, "name", "WPGConfig.solutionTag", issues);
	if (!instanceSpace || !externalId || !name) return null;
	return { instanceSpace, externalId, name };
}

function readLimits(
	value: unknown,
	issues: string[]
): AppConfig["limits"] | null {
	const limits = readObject(value, "WPGConfig.limits", issues);
	if (!limits) return null;
	const keys = [
		"instancePageSize",
		"upsertBatchSize",
		"deleteBatchSize",
		"searchResultLimit",
		"resourcesDisplayLimit",
		"maxCanvasListScanItems",
		"maxResourceInstancesTotal",
		"maxTemplateCanvasChildItems",
		"maxCanvasCleanupItems",
		"findCanvasByNameMaxItems",
	] as const;
	rejectUnknown(limits, keys, "WPGConfig.limits", issues);

	const dmsKeys = new Set<string>([
		"instancePageSize",
		"upsertBatchSize",
		"deleteBatchSize",
	]);
	const parsed = {} as AppConfig["limits"];
	let complete = true;
	for (const key of keys) {
		const max = dmsKeys.has(key) ? DMS_BATCH_MAX : GENERAL_LIMIT_MAX;
		const number = readInteger(limits, key, "WPGConfig.limits", issues, 1, max);
		if (number === null) {
			complete = false;
			continue;
		}
		parsed[key] = number;
	}
	return complete ? parsed : null;
}

function readClusterSettings(
	value: unknown,
	issues: string[]
): ClusterSettings | null {
	const cluster = readObject(value, "WPGConfig.cluster", issues);
	if (!cluster) return null;
	rejectUnknown(
		cluster,
		["apiHostSuffix", "hostAliases"],
		"WPGConfig.cluster",
		issues
	);
	const apiHostSuffix = readRequiredString(
		cluster,
		"apiHostSuffix",
		"WPGConfig.cluster",
		issues
	);
	const hostAliases = readHostAliases(cluster.hostAliases, issues);
	if (!apiHostSuffix || !hostAliases) return null;
	return { apiHostSuffix, hostAliases };
}

function readHostAliases(
	value: unknown,
	issues: string[]
): Record<string, string> | null {
	const aliases = readObject(value, "WPGConfig.cluster.hostAliases", issues);
	if (!aliases) return null;
	const parsed: Record<string, string> = {};
	let valid = true;
	for (const [host, clusterId] of Object.entries(aliases)) {
		if (host.trim() === "") {
			issues.push("WPGConfig.cluster.hostAliases: host name must be non-empty");
			valid = false;
			continue;
		}
		if (typeof clusterId !== "string" || clusterId.trim() === "") {
			issues.push(
				`WPGConfig.cluster.hostAliases.${host}: expected non-empty string`
			);
			valid = false;
			continue;
		}
		parsed[host] = clusterId;
	}
	return valid ? parsed : null;
}

function readViewId(
	value: unknown,
	path: string,
	issues: string[]
): ViewIdConfig | null {
	const view = readObject(value, path, issues);
	if (!view) return null;
	rejectUnknown(view, ["space", "externalId", "version"], path, issues);
	const space = readRequiredString(view, "space", path, issues);
	const externalId = readRequiredString(view, "externalId", path, issues);
	const version = readRequiredString(view, "version", path, issues);
	if (!space || !externalId || !version) return null;
	return { space, externalId, version };
}

function readContainerId(
	value: unknown,
	path: string,
	issues: string[]
): ContainerIdConfig | null {
	const container = readObject(value, path, issues);
	if (!container) return null;
	rejectUnknown(container, ["space", "externalId"], path, issues);
	const space = readRequiredString(container, "space", path, issues);
	const externalId = readRequiredString(container, "externalId", path, issues);
	if (!space || !externalId) return null;
	return { space, externalId };
}

function readObject(
	value: unknown,
	path: string,
	issues: string[]
): Record<string, unknown> | null {
	if (value === undefined || value === null) {
		issues.push(`${path}: required`);
		return null;
	}
	if (typeof value !== "object" || Array.isArray(value)) {
		issues.push(`${path}: expected object`);
		return null;
	}
	return value as Record<string, unknown>;
}

function rejectUnknown(
	obj: Record<string, unknown>,
	allowed: readonly string[],
	path: string,
	issues: string[]
): void {
	for (const key of Object.keys(obj)) {
		if (!allowed.includes(key)) {
			issues.push(`${path}.${key}: unknown key`);
		}
	}
}

function readRequiredString(
	obj: Record<string, unknown>,
	key: string,
	path: string,
	issues: string[]
): string | null {
	return readString(obj, key, path, issues);
}

function readString(
	obj: Record<string, unknown>,
	key: string,
	path: string,
	issues: string[],
	options?: { allowEmpty?: boolean }
): string | null {
	const fullPath = `${path}.${key}`;
	if (!(key in obj) || obj[key] === undefined) {
		issues.push(`${fullPath}: required`);
		return null;
	}
	if (typeof obj[key] !== "string") {
		issues.push(`${fullPath}: expected string`);
		return null;
	}
	const value = obj[key];
	if (!options?.allowEmpty && value.trim() === "") {
		issues.push(`${fullPath}: must be a non-empty string`);
		return null;
	}
	return value;
}

function readInteger(
	obj: Record<string, unknown>,
	key: string,
	path: string,
	issues: string[],
	min: number,
	max = GENERAL_LIMIT_MAX
): number | null {
	const fullPath = `${path}.${key}`;
	if (!(key in obj) || obj[key] === undefined) {
		issues.push(`${fullPath}: required`);
		return null;
	}
	const value = obj[key];
	if (typeof value !== "number" || !Number.isInteger(value)) {
		issues.push(`${fullPath}: expected integer`);
		return null;
	}
	if (value < min || value > max) {
		issues.push(`${fullPath}: expected integer between ${min} and ${max}`);
		return null;
	}
	return value;
}

function readStringArray(
	value: unknown,
	path: string,
	issues: string[],
	options?: { allowEmpty?: boolean }
): string[] | null {
	if (value === undefined) {
		issues.push(`${path}: required`);
		return null;
	}
	if (!Array.isArray(value)) {
		issues.push(`${path}: expected array of strings`);
		return null;
	}
	if (!options?.allowEmpty && value.length === 0) {
		issues.push(`${path}: must contain at least one item`);
		return null;
	}
	const result: string[] = [];
	let valid = true;
	for (let index = 0; index < value.length; index++) {
		const item = value[index];
		if (typeof item !== "string" || item.trim() === "") {
			issues.push(`${path}[${index}]: expected non-empty string`);
			valid = false;
			continue;
		}
		result.push(item);
	}
	return valid ? result : null;
}
