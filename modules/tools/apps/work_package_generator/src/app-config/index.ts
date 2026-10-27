/**
 * Application configuration loaded from the WPG config node.
 *
 * Hooks read config through {@link useAppConfig}. Plain functions take an
 * optional AppConfig argument and fall back with {@link resolveAppConfig}.
 */

export { AppConfigProvider, useAppConfig } from "./context";
export {
	fusionOrganizationFromHostname,
	fusionOrganizationFromRuntime,
	getViewKey,
	toViewReference,
	viewIdToSource,
} from "./derive";
export { AppConfigError } from "./errors";
export {
	getAppConfig,
	installAppConfig,
	loadAppConfig,
	resolveAppConfig,
} from "./load";
export {
	canvasAnnotationView,
	canvasInstanceView,
	canvasSchemaView,
	cdfClusterQueryHost,
	dataModelView,
	extractClusterFromUrl,
	getAgentExternalId,
	getAiSelectableTemplates,
	getBaseTemplateCanvas,
	getContextConfigs,
	getResourceConfigs,
	isBaseTemplateId,
	toCanvasConfig,
} from "./runtime";
export type {
	AppConfig,
	ClusterSettings,
	ResourceTypeConfig,
	ViewIdConfig,
} from "./types";
