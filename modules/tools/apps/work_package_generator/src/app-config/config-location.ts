/**
 * Fixed location of the single WPG config node.
 * These identifiers are literals in the toolkit module as well.
 * The view version is not fixed: startup reads it from the latest data model.
 */

export const WPG_CONFIG_LOCATION = {
	schemaSpace: "dm_sol_wpg_app",
	instanceSpace: "inst_wpg_app_config",
	dataModelExternalId: "WPGConfig_SOL",
	viewExternalId: "WPGConfig",
	nodeExternalId: "wpg_config",
} as const;
