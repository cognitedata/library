/**
 * Test factory for the WPG config node.
 * Uses a fixture with real-shaped node properties.
 */

import {
	canvasAnnotationView,
	canvasInstanceView,
	canvasSchemaView,
	dataModelView,
	getBaseTemplateCanvas,
	getResourceConfigs,
	toCanvasConfig,
} from "../app-config";
import { loadAppConfig } from "../app-config/load";
import type { AppConfig } from "../app-config/types";
import rawAppJson from "./app-config.fixture.json";

export function createMockAppConfig(): AppConfig {
	return loadAppConfig(structuredClone(rawAppJson));
}

const mockConfig = createMockAppConfig();

export { getResourceConfigs };

export const ACTIVITY_VIEW_ID = dataModelView("activity", mockConfig);
export const ASSET_VIEW_ID = dataModelView("asset", mockConfig);
export const EQUIPMENT_VIEW_ID = dataModelView("equipment", mockConfig);
export const FILE_VIEW_ID = dataModelView("file", mockConfig);
export const TIME_SERIES_VIEW_ID = dataModelView("timeSeries", mockConfig);
export const CANVAS_VIEW_ID = canvasSchemaView(mockConfig);
export const CANVAS_INSTANCE_VIEW_ID = canvasInstanceView(mockConfig);
export const CANVAS_ANNOTATION_VIEW_ID = canvasAnnotationView(mockConfig);
export const CANVAS_SPACE_INSTANCE = mockConfig.canvas.instanceSpace;
export const DEFAULT_CANVAS_CONFIG = toCanvasConfig(mockConfig);
export const DEFAULT_CANVAS_DIMENSIONS = mockConfig.canvas.dimensions;
export const DEFAULT_USER_ID = mockConfig.canvas.userId;
export const BASE_TEMPLATE_ID = mockConfig.templates.baseTemplate.id;
export const BASE_TEMPLATE_CANVAS = getBaseTemplateCanvas(mockConfig);
export const RESOURCES_DISPLAY_LIMIT = mockConfig.limits.resourcesDisplayLimit;
export const AGENT_NAME = mockConfig.agent.name;
export const AGENT_DESCRIPTION = mockConfig.agent.description;
export const DEFAULT_AGENT_MODEL = mockConfig.agent.model;
export const DEFAULT_AGENT_RUNTIME_VERSION = mockConfig.agent.runtimeVersion;
export const SOLUTION_TAG_VIEW = dataModelView("solutionTag", mockConfig);
export const WORK_PACKAGE_SOLUTION_TAG = {
	instanceSpace: mockConfig.solutionTag.instanceSpace,
	externalId: mockConfig.solutionTag.externalId,
	name: mockConfig.solutionTag.name,
};
