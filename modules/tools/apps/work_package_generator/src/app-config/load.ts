/**
 * Load and cache the WPG config node.
 * API modules should accept an optional AppConfig override and fall back
 * through {@link resolveAppConfig}, not import individual constants.
 */

import { AppConfigError } from "./errors";
import type { AppConfig } from "./types";
import { parseConfigNode } from "./validate";

export function loadAppConfig(raw: unknown): AppConfig {
	const parsed = parseConfigNode(raw);
	return {
		dataModel: parsed.dataModel,
		canvas: parsed.canvas,
		resourceTypes: parsed.resourceTypes,
		context: parsed.context,
		agent: parsed.agent,
		templates: parsed.templates,
		solutionTag: parsed.solutionTag,
		limits: parsed.limits,
		clusterSettings: parsed.clusterSettings,
		fusion: parsed.fusion,
		workOrderProperties: parsed.workOrderProperties,
		prompts: parsed.prompts,
		ui: parsed.ui,
	};
}

let shipped: AppConfig | undefined;

/** Stores a parsed config. The view model calls this after a successful load. */
export function installAppConfig(config: AppConfig): AppConfig {
	shipped = config;
	return shipped;
}

/** Parsed config node. The same object is reused for the life of the page. */
export function getAppConfig(): AppConfig {
	if (!shipped) {
		throw new AppConfigError(["WPG config has not been loaded yet"]);
	}
	return shipped;
}

/** Factory default: explicit override wins, otherwise the loaded config node. */
export function resolveAppConfig(override?: AppConfig): AppConfig {
	return override ?? getAppConfig();
}
