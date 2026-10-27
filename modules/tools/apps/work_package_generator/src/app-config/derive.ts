/**
 * Pure helpers over {@link AppConfig}. Callers pass config.
 */

import type { ViewReference } from "@cognite/sdk";
import type {
	CanvasConfig,
	ContextConfig,
	ResourceConfig,
	TemplateCanvas,
} from "../types";
import type { AppConfig, ClusterSettings, ViewIdConfig } from "./types";

export function toViewReference(view: ViewIdConfig): ViewReference {
	return {
		type: "view",
		space: view.space,
		externalId: view.externalId,
		version: view.version,
	};
}

export function viewIdToSource(viewId: ViewReference): {
	source: ViewReference;
} {
	return { source: viewId };
}

export function getViewKey(viewId: ViewReference): string {
	return `${viewId.externalId}/${viewId.version}`;
}

export function getResourceConfigs(
	config: AppConfig
): Record<string, ResourceConfig> {
	const result: Record<string, ResourceConfig> = {};

	for (const [typeKey, resourceType] of Object.entries(config.resourceTypes)) {
		const view = config.dataModel.views[resourceType.view];
		if (!view) continue;

		const displayNameLower = resourceType.displayName
			.toLowerCase()
			.replace(/ /g, "_");

		result[typeKey] = {
			name: typeKey,
			displayName: resourceType.displayName,
			viewId: toViewReference(view),
			properties: resourceType.properties,
			resourceName: displayNameLower,
			trackingKey: displayNameLower,
			assetRelationProperty: resourceType.assetRelationProperty,
			assetRelationIsArray: resourceType.assetRelationIsArray,
			activityContainerIdentifier: resourceType.activityContainerIdentifier,
		};
	}

	return result;
}

/** Work-order context follows the activity view; asset context follows the asset view. */
export function getContextConfigs(config: AppConfig): {
	workOrder: ContextConfig;
	asset: ContextConfig;
} {
	return {
		workOrder: {
			viewId: toViewReference(config.dataModel.views.activity),
			properties: config.context.workOrder.properties,
		},
		asset: {
			viewId: toViewReference(config.dataModel.views.asset),
			properties: config.context.asset.properties,
		},
	};
}

/** Canvas write model used by the builder. */
export function toCanvasConfig(config: AppConfig): CanvasConfig {
	return {
		canvasSpaceCanvas: config.canvas.schemaSpace,
		canvasSpaceInstance: config.canvas.instanceSpace,
		canvasContainerCanvas: config.canvas.canvasView.externalId,
		canvasInstanceContainerId: config.canvas.instanceView.externalId,
		canvasAnnotationContainerId: config.canvas.annotationView.externalId,
	};
}

export function getAgentExternalId(project: string, config: AppConfig): string {
	if (config.agent.existingAgentExternalId) {
		return config.agent.existingAgentExternalId;
	}
	return `${project}_${config.agent.externalIdSuffix}`;
}

export function clusterIdFromBaseUrl(
	baseUrl: string,
	clusterSettings: ClusterSettings
): string {
	const host = cdfHostname(baseUrl);
	const alias = clusterSettings.hostAliases[host];
	if (alias) return alias;

	const suffix = `.${clusterSettings.apiHostSuffix}`;
	if (host.endsWith(suffix)) {
		return host.slice(0, -suffix.length);
	}
	return host;
}

export function clusterQueryHost(
	clusterId: string,
	clusterSettings: ClusterSettings
): string {
	const aliasHost = Object.entries(clusterSettings.hostAliases).find(
		([, id]) => id === clusterId
	)?.[0];
	if (aliasHost) return aliasHost;
	if (clusterId.endsWith(`.${clusterSettings.apiHostSuffix}`)) return clusterId;
	return `${clusterId}.${clusterSettings.apiHostSuffix}`;
}

export function isBaseTemplateId(
	templateCanvasId: string | null | undefined,
	config: AppConfig
): boolean {
	return templateCanvasId === config.templates.baseTemplate.id;
}

export function getBaseTemplateCanvas(config: AppConfig): TemplateCanvas {
	const base = config.templates.baseTemplate;
	return {
		externalId: base.id,
		name: base.name,
		visibility: base.visibility,
		space: base.space,
	};
}

export function getAiSelectableTemplates(
	templates: TemplateCanvas[],
	config: AppConfig
): TemplateCanvas[] {
	const baseId = config.templates.baseTemplate.id;
	return templates.filter((template) => template.externalId !== baseId);
}

/**
 * Fusion org subdomain from a hostname such as `cog-demo.fusion.cognite.com`.
 * `hostSuffix` comes from `config.fusion.hostSuffix` (shipped `.fusion.cognite.com`).
 * Private-link hosts are not detected unless that suffix matches them.
 * Returns null when the host is not a Fusion origin (local dev, tests).
 */
export function fusionOrganizationFromHostname(
	hostname: string,
	hostSuffix: string
): string | null {
	const host = hostnameOf(hostname);
	if (!host.endsWith(hostSuffix)) return null;
	const organization = host.slice(0, -hostSuffix.length);
	if (!organization || organization.includes(".")) return null;
	return organization;
}

/**
 * Org of the Fusion page that is hosting this app.
 * Custom apps run in an iframe whose own host is the bundle origin, so the
 * parent origin (`ancestorOrigins`, then `referrer`) is checked before the
 * iframe hostname. Callers use `fusion.fallbackOrganization` when this is null.
 */
export function fusionOrganizationFromRuntime(
	sources: {
		hostname: string;
		ancestorOrigins?: readonly string[];
		referrer?: string;
	},
	hostSuffix: string
): string | null {
	const candidates = [
		...(sources.ancestorOrigins ?? []),
		sources.referrer ?? "",
		sources.hostname,
	];
	for (const candidate of candidates) {
		const organization = fusionOrganizationFromHostname(candidate, hostSuffix);
		if (organization) return organization;
	}
	return null;
}

function hostnameOf(value: string): string {
	const trimmed = value.trim().toLowerCase();
	if (!trimmed) return "";
	try {
		if (trimmed.includes("://")) return new URL(trimmed).hostname;
	} catch {
		return "";
	}
	return trimmed.split("/")[0]?.split(":")[0] ?? "";
}

function cdfHostname(baseUrl: string): string {
	const withoutScheme = baseUrl.trim().replace(/^https?:\/\//i, "");
	const hostPort = withoutScheme.split("/")[0] ?? "";
	return hostPort.split(":")[0]?.toLowerCase() ?? "";
}
