/**
 * Config helpers with a loaded-config default.
 * Pass an AppConfig to override (tests, or a hook that read {@link useAppConfig}).
 */

import type { ViewReference } from "@cognite/sdk";
import {
	getAgentExternalId as agentExternalId,
	getAiSelectableTemplates as aiSelectableTemplates,
	getBaseTemplateCanvas as baseTemplateCanvas,
	isBaseTemplateId as baseTemplateId,
	toCanvasConfig as canvasConfigFrom,
	clusterIdFromBaseUrl,
	clusterQueryHost,
	getContextConfigs as contextConfigs,
	getResourceConfigs as resourceConfigs,
	toViewReference,
} from "./derive";
import { resolveAppConfig } from "./load";
import type { AppConfig, DataModelViewKey } from "./types";

export function dataModelView(
	key: DataModelViewKey,
	config?: AppConfig
): ViewReference {
	return toViewReference(resolveAppConfig(config).dataModel.views[key]);
}

export function canvasSchemaView(config?: AppConfig): ViewReference {
	return toViewReference(resolveAppConfig(config).canvas.canvasView);
}

export function canvasInstanceView(config?: AppConfig): ViewReference {
	return toViewReference(resolveAppConfig(config).canvas.instanceView);
}

export function canvasAnnotationView(config?: AppConfig): ViewReference {
	return toViewReference(resolveAppConfig(config).canvas.annotationView);
}

export function extractClusterFromUrl(
	baseUrl: string,
	config?: AppConfig
): string {
	return clusterIdFromBaseUrl(
		baseUrl,
		resolveAppConfig(config).clusterSettings
	);
}

export function cdfClusterQueryHost(
	clusterId: string,
	config?: AppConfig
): string {
	return clusterQueryHost(clusterId, resolveAppConfig(config).clusterSettings);
}

export function getResourceConfigs(config?: AppConfig) {
	return resourceConfigs(resolveAppConfig(config));
}

export function getContextConfigs(config?: AppConfig) {
	return contextConfigs(resolveAppConfig(config));
}

export function toCanvasConfig(config?: AppConfig) {
	return canvasConfigFrom(resolveAppConfig(config));
}

export function getAgentExternalId(
	project: string,
	config?: AppConfig
): string {
	return agentExternalId(project, resolveAppConfig(config));
}

export function isBaseTemplateId(
	templateCanvasId: string | null | undefined,
	config?: AppConfig
): boolean {
	return baseTemplateId(templateCanvasId, resolveAppConfig(config));
}

export function getBaseTemplateCanvas(config?: AppConfig) {
	return baseTemplateCanvas(resolveAppConfig(config));
}

export function getAiSelectableTemplates(
	templates: Parameters<typeof aiSelectableTemplates>[0],
	config?: AppConfig
) {
	return aiSelectableTemplates(templates, resolveAppConfig(config));
}
