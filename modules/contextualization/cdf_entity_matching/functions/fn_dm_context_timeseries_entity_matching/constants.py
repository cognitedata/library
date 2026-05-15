STAT_STORE_MATCH_MODEL_ID = "state_match_model_id"
STAT_STORE_VALUE = "value"
FUNCTION_ID = "entity_matching"
ML_MODEL_FEATURE_TYPE = "bigram-combo"
COL_MATCH_KEY = "name"

# Manual mapping column names in RAW table
COL_KEY_MAN_MAPPING_ENTITY = "TsExternalId"  # ExternalID for TS not mapped related to manual mapping
COL_KEY_MAN_MAPPING_TARGET = "AssetExternalId"  # ExternalID Col name for Asset related to manual mapping
COL_KEY_MAN_CONTEXTUALIZED = "Contextualized"  # Col name for if mapping is done for manual mapping

# Rule mapping column names in RAW table
COL_KEY_RULE_REGEXP_ENTITY = "EntityRegExp"  # Regular expression to extract entity key value out of name column
COL_KEY_RULE_REGEXP_TARGET = "AssetRegExp"  # Regular expression to extract asset key value out of name column

# View property names (DM schema)
PROP_COL_NAME = "name"
PROP_COL_LINK_NAME = "assets"
PROP_COL_EXTERNAL_ID = "externalId"
PROP_COL_SPACE = "space"

# DM filter path for instance lookup
FILTER_PATH_NODE_EXTERNAL_ID = ["node", "externalId"]

# Placeholder strings for unmatched entities/assets
PLACEHOLDER_NO_MATCH_TARGET = "_no_match_on_asset_ext_id_"
PLACEHOLDER_NO_MATCH = "_no_match_"

# Match type labels for output/RAW tables
MATCH_TYPE_MANUAL = "Manual Mapping"
MATCH_TYPE_RULE = "Rule Based Mapping"
MATCH_TYPE_ENTITY = "Entity Matching"

# Batch and limit constants
BATCH_SIZE_ENTITIES = 5000
MATCHING_LIMIT_SOURCES_TARGETS = 10000
MAX_LINKS_PER_ENTITY = 1000
SCORE_MANUAL_RULE_MATCH = 1
BATCH_SIZE_API_SUBMIT = 1000

# Query filter types for get_query_filter
QUERY_FILTER_TYPE_TARGETS = "assets"  # assets property name in the asset view
QUERY_FILTER_TYPE_ENTITIES = "entities"  # entities property name in the entity view

# Match dict keys (internal structures and CDF entity matching API response)
KEY_RULE_KEYS = "rule_keys"
KEY_RULE = "key"
KEY_ENTITY_EXT_ID = "entity_ext_id"
KEY_TARGET_EXT_ID = "asset_ext_id"
KEY_ORG_NAME = "org_name"
KEY_NAME = "name"
KEY_TARGET_LINKS = "assets"
KEY_MATCH_TYPE = "match_type"
KEY_SCORE = "score"
KEY_SOURCE = "source"
KEY_TARGET = "target"
KEY_MATCHES = "matches"
KEY_ENTITY_EXISTING_TARGETS = "entity_existing_assets"
KEY_ENTITY_RULE_KEYS = "entity_rule_keys"
KEY_TARGET_RULE_KEYS = "asset_rule_keys"
KEY_ENTITY_NAME = "entity_name"
KEY_ENTITY_MATCH_VALUE = "entity_match_value"
KEY_ENTITY_VIEW_ID = "entity_view_id"
KEY_TARGET_NAME = "asset_name"
KEY_TARGET_MATCH_VALUE = "asset_match_value"
KEY_TARGET_VIEW_ID = "asset_view_id"

# Entity matching job result
JOB_RESULT_ITEMS = "items"

# Pipeline run status
STATUS_SUCCESS = "success"
STATUS_FAILURE = "failure"

# Log levels
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
