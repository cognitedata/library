from typing import Final

STAT_STORE_MATCH_MODEL_ID: Final = "state_match_model_id"
STAT_STORE_VALUE: Final = "value"
FUNCTION_ID: Final = "entity_matching"
ML_MODEL_FEATURE_TYPE: Final = "bigram-combo"
COL_MATCH_KEY: Final = "name"

# Manual mapping column names in RAW table
COL_KEY_MAN_MAPPING_ENTITY: Final = "TsExternalId"  # ExternalID for TS not mapped related to manual mapping
COL_KEY_MAN_MAPPING_TARGET: Final = "AssetExternalId"  # ExternalID Col name for Asset related to manual mapping
COL_KEY_MAN_CONTEXTUALIZED: Final = "Contextualized"  # Col name for if mapping is done for manual mapping

# Rule mapping column names in RAW table
COL_KEY_RULE_REGEXP_ENTITY: Final = "EntityRegExp"  # Regular expression to extract entity key value out of name column
COL_KEY_RULE_REGEXP_TARGET: Final = "AssetRegExp"  # Regular expression to extract asset key value out of name column

# View property names (DM schema)
PROP_COL_NAME: Final = "name"
PROP_COL_LINK_NAME: Final = "assets"
PROP_COL_EXTERNAL_ID: Final = "externalId"
PROP_COL_SPACE: Final = "space"

# DM filter path for instance lookup
FILTER_PATH_NODE_EXTERNAL_ID: Final = ["node", "externalId"]

# Placeholder strings for unmatched entities/assets
PLACEHOLDER_NO_MATCH_TARGET: Final = "_no_match_on_asset_ext_id_"
PLACEHOLDER_NO_MATCH: Final = "_no_match_"

# Match type labels for output/RAW tables
MATCH_TYPE_MANUAL: Final = "Manual Mapping"
MATCH_TYPE_RULE: Final = "Rule Based Mapping"
MATCH_TYPE_ENTITY: Final = "Entity Matching"

# Batch and limit constants
BATCH_SIZE_ENTITIES: Final = 5000
MATCHING_LIMIT_SOURCES_TARGETS: Final = 10000
MAX_LINKS_PER_ENTITY: Final = 1000
SCORE_MANUAL_RULE_MATCH: Final = 1
BATCH_SIZE_API_SUBMIT: Final = 1000

# Query filter types for get_query_filter
QUERY_FILTER_TYPE_TARGETS: Final = "assets"  # assets property name in the asset view
QUERY_FILTER_TYPE_ENTITIES: Final = "entities"  # entities property name in the entity view

# Match dict keys (internal structures and CDF entity matching API response)
KEY_RULE_KEYS: Final = "rule_keys"
KEY_RULE: Final = "key"
KEY_ENTITY_EXT_ID: Final = "entity_ext_id"
KEY_ENTITY_SPACE: Final = "entity_space"
KEY_TARGET_EXT_ID: Final = "asset_ext_id"
KEY_TARGET_SPACE: Final = "asset_space"
KEY_ORG_NAME: Final = "org_name"
KEY_NAME: Final = "name"
KEY_TARGET_LINKS: Final = "assets"
KEY_MATCH_TYPE: Final = "match_type"
KEY_SCORE: Final = "score"
KEY_SOURCE: Final = "source"
KEY_TARGET: Final = "target"
KEY_MATCHES: Final = "matches"
KEY_ENTITY_EXISTING_TARGETS: Final = "entity_existing_assets"
KEY_ENTITY_RULE_KEYS: Final = "entity_rule_keys"
KEY_TARGET_RULE_KEYS: Final = "asset_rule_keys"
KEY_ENTITY_NAME: Final = "entity_name"
KEY_ENTITY_MATCH_VALUE: Final = "entity_match_value"
KEY_ENTITY_VIEW_ID: Final = "entity_view_id"
KEY_TARGET_NAME: Final = "asset_name"
KEY_TARGET_MATCH_VALUE: Final = "asset_match_value"
KEY_TARGET_VIEW_ID: Final = "asset_view_id"

# Entity matching job result
JOB_RESULT_ITEMS: Final = "items"

# Pipeline run status
STATUS_SUCCESS: Final = "success"
STATUS_FAILURE: Final = "failure"

# Log levels
LOG_LEVEL_DEBUG: Final = "DEBUG"
LOG_LEVEL_INFO: Final = "INFO"

# ===== Asynchronous predict: submit (F1) and collect (F2) =====

# One state store row per submitted predict job, keyed by this prefix plus the job id.
# The prefix keeps the rows apart from STAT_STORE_MATCH_MODEL_ID in the same table.
STAT_STORE_PREDICT_JOB_PREFIX: Final = "state_predict_job_"

# Columns on a predict job row.
JOB_COL_JOB_ID: Final = "jobId"
JOB_COL_JOB_TOKEN: Final = "jobToken"  # noqa: S105 - column name, not a credential
JOB_COL_STATUS: Final = "status"
JOB_COL_CREATED_AT: Final = "createdAt"
JOB_COL_MODEL_ID: Final = "modelId"
JOB_COL_STAGING_PREFIX: Final = "stagingPrefix"
JOB_COL_SOURCE_COUNT: Final = "sourceCount"

# Predict job lifecycle. A collected job has its row deleted rather than a terminal
# status, so the queue only ever holds work that is still outstanding.
JOB_STATUS_SUBMITTED: Final = "submitted"
JOB_STATUS_RUNNING: Final = "running"

# Matches from manual and rule mappings are staged in the good table under this row key
# prefix while the predict job runs, so collect can merge them with the ML matches.
STAGING_ROW_KEY_PREFIX: Final = "pending"
STAGING_COL_JOB_ID: Final = "pendingJobId"

# Terminal job states reported by the entity matching API.
JOB_API_STATUS_COMPLETED: Final = "Completed"
JOB_API_STATUS_FAILED: Final = "Failed"

# Status path a predict job is polled on. The SDK sets this when predict is called; it
# has to be restated here because collect rebuilds the job from the state store row.
ENTITY_MATCHING_JOB_STATUS_PATH: Final = "/context/entitymatching/jobs/"

# Seconds to wait between polls: 5, then 15, then 30 for every poll after that.
POLL_BACKOFF_SECONDS: Final = (5, 15, 30)

# Collect gives up polling this long after the invocation started and leaves the
# remaining jobs queued for the next run.
POLL_BUDGET_SECONDS: Final = 8 * 60

# ===== Target read: sync cursor and cached content =====

# One state store row per target configuration, keyed by this prefix plus a fingerprint
# of that configuration, so functions reading different targets never share a cursor.
STAT_STORE_TARGET_SYNC_PREFIX: Final = "state_target_sync_"

# Columns on a target sync row.
TARGET_SYNC_COL_CURSOR: Final = "syncCursor"
TARGET_SYNC_COL_FILE: Final = "fileExternalId"
TARGET_SYNC_COL_BATCH_SIZE: Final = "batchSize"
TARGET_SYNC_COL_COUNT: Final = "targetCount"
TARGET_SYNC_COL_VIEW: Final = "targetView"
TARGET_SYNC_COL_UPDATED_AT: Final = "updatedAt"

# External id of the CDF file holding the cached target content, plus that fingerprint.
TARGET_CACHE_FILE_PREFIX: Final = "em_target_cache_"

# Part of that fingerprint. Raise it when a change to the read alters what ends up in
# the cache, so content written by the previous version is read again rather than reused.
TARGET_CACHE_VERSION: Final = 2

# Name of the result set expression in the sync query, and so of its cursor.
TARGET_SYNC_QUERY_NAME: Final = "targets"

# Page size for the sync read. A page that times out is read again 20% smaller, down to
# the minimum, and the size that worked is kept in the state store for the next run.
TARGET_SYNC_BATCH_SIZE: Final = 1000
TARGET_SYNC_MIN_BATCH_SIZE: Final = 100
TARGET_SYNC_BATCH_SIZE_FACTOR: Final = 0.8
TARGET_SYNC_MAX_RETRIES: Final = 4
TARGET_SYNC_RETRY_BACKOFF_SECONDS: Final = 2

# Retries for the cache file itself. Fewer than for the sync read: the cache is an
# optimisation, and a run that cannot reach it reads the data model instead of spending
# its budget on retries.
TARGET_CACHE_MAX_RETRIES: Final = 2

# The read did not finish in time, and the same read over fewer instances may still do.
HTTP_STATUS_REQUEST_TIMEOUT: Final = 408

# The request itself was rejected - for a sync call, that is the cursor.
HTTP_STATUS_BAD_REQUEST: Final = 400
