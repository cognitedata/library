# Generated from functions/_entity_matching_core/constants.py - do not edit this copy.
# Change the source and run: python scripts/sync_entity_matching_core.py
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
KEY_ENTITY_SPACE = "entity_space"
KEY_TARGET_EXT_ID = "asset_ext_id"
KEY_TARGET_SPACE = "asset_space"
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

# ===== Asynchronous predict: submit (F1) and collect (F2) =====

# One state store row per submitted predict job, keyed by this prefix plus the job id.
# The prefix keeps the rows apart from STAT_STORE_MATCH_MODEL_ID in the same table.
STAT_STORE_PREDICT_JOB_PREFIX = "state_predict_job_"

# Columns on a predict job row.
JOB_COL_JOB_ID = "jobId"
JOB_COL_JOB_TOKEN = "jobToken"  # noqa: S105 - column name, not a credential
JOB_COL_STATUS = "status"
JOB_COL_CREATED_AT = "createdAt"
JOB_COL_MODEL_ID = "modelId"
JOB_COL_STAGING_PREFIX = "stagingPrefix"
JOB_COL_SOURCE_COUNT = "sourceCount"

# Predict job lifecycle. A collected job has its row deleted rather than a terminal
# status, so the queue only ever holds work that is still outstanding.
JOB_STATUS_SUBMITTED = "submitted"
JOB_STATUS_RUNNING = "running"

# Matches from manual and rule mappings are staged in the good table under this row key
# prefix while the predict job runs, so collect can merge them with the ML matches.
STAGING_ROW_KEY_PREFIX = "pending"
STAGING_COL_JOB_ID = "pendingJobId"

# Terminal job states reported by the entity matching API.
JOB_API_STATUS_COMPLETED = "Completed"
JOB_API_STATUS_FAILED = "Failed"

# Status path a predict job is polled on. The SDK sets this when predict is called; it
# has to be restated here because collect rebuilds the job from the state store row.
ENTITY_MATCHING_JOB_STATUS_PATH = "/context/entitymatching/jobs/"

# Seconds to wait between polls: 5, then 15, then 30 for every poll after that.
POLL_BACKOFF_SECONDS = (5, 15, 30)

# Collect gives up polling this long after the invocation started and leaves the
# remaining jobs queued for the next run.
POLL_BUDGET_SECONDS = 8 * 60
