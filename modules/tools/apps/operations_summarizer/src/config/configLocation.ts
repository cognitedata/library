/**
 * Fixed identifiers for the deployed config node.
 * The schema lives in `CONFIG_SCHEMA_SPACE`. The node lives in
 * `CONFIG_INSTANCE_SPACE`. The toolkit module and the Python function use
 * the same names. Renaming any of them needs a matching app and function release.
 */
export const CONFIG_SCHEMA_SPACE = "dm_sol_ops_summarizer_app";
export const CONFIG_INSTANCE_SPACE = "inst_ops_summarizer_app_config";
export const CONFIG_DATA_MODEL_EXTERNAL_ID = "OpsSummarizerConfig_SOL";
export const CONFIG_VIEW_EXTERNAL_ID = "OpsSummarizerConfig";
export const CONFIG_NODE_EXTERNAL_ID = "ops_summarizer_config";
export const CONFIG_NODE_PATH = `${CONFIG_INSTANCE_SPACE}/${CONFIG_NODE_EXTERNAL_ID}`;

/** Content shape this app and the functions accept. */
export const SUPPORTED_SCHEMA_VERSION = 1;
