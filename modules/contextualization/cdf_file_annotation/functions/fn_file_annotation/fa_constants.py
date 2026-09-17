"""Fixed behavior and resource names for file annotation."""

from typing import Final

BATCH_SIZE: Final = 50
PAGE_RANGE: Final = 50
CACHE_TIME_LIMIT_HOURS: Final = 0
MAX_RETRY_ATTEMPTS: Final = 3
MAX_ENTITY_SEARCH_LIMIT: Final = 1000
PREPARE_FILE_LIMIT: Final = 10000
LAUNCH_STATE_LIMIT: Final = 1000
PROMOTE_CANDIDATE_LIMIT: Final = 500
FUNCTION_TIME_BUDGET_MINUTES: Final = 7
LOCAL_RATE_LIMIT_SLEEP_SECONDS: Final = 900
MIN_TOKENS: Final = 1

CORE_ANNOTATION_SCHEMA_SPACE: Final = "cdf_cdm"
CORE_ANNOTATION_EXTERNAL_ID: Final = "CogniteDiagramAnnotation"
CORE_ANNOTATION_VERSION: Final = "v1"
FILE_ANNOTATION_TYPE: Final = "diagrams.FileLink"
TARGET_ANNOTATION_TYPE: Final = "diagrams.AssetLink"

RAW_TABLE_CACHE: Final = "annotation_entities_cache"
RAW_TABLE_DOC_TAG: Final = "annotation_documents_tags"
RAW_TABLE_DOC_DOC: Final = "annotation_documents_docs"
RAW_TABLE_DOC_PATTERN: Final = "annotation_documents_patterns"
RAW_TABLE_PROMOTE_CACHE: Final = "annotation_tags_cache"
RAW_TABLE_MANUAL_PATTERNS: Final = "manual_patterns_catalog"

TAG_TO_ANNOTATE: Final = "ToAnnotate"
TAG_DETECT_IN_DIAGRAMS: Final = "DetectInDiagrams"
TAG_PROMOTE_ATTEMPTED: Final = "PromoteAttempted"
EXCLUDED_PREPARE_TAGS: Final = ["AnnotationInProcess", "Annotated", "AnnotationFailed"]
LAUNCH_STATUSES: Final = ["New", "Retry"]
PROCESSING_STATUS: Final = "Processing"
SUGGESTED_STATUS: Final = "Suggested"

PROMOTE_FILE_ENTITIES: Final = True
PROMOTE_TARGET_ENTITIES: Final = True
DELETE_REJECTED_EDGES: Final = True
DELETE_SUGGESTED_EDGES: Final = False

DEFAULT_NORMALIZATION_SUBSTITUTIONS: Final = [
    (r"[^A-Za-z0-9]", ""),
    (r"(?<!\d)0+(\d+)", r"\1"),
]
