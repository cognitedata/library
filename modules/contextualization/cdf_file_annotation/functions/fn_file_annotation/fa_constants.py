"""Fixed behavior and resource names for file annotation."""

from typing import Final

BATCH_SIZE: Final = 50
PAGE_RANGE: Final = 50
CACHE_TIME_LIMIT_HOURS: Final = 0
MAX_RETRY_ATTEMPTS: Final = 3
MAX_ENTITY_SEARCH_LIMIT: Final = 1000
ENTITY_QUERY_PAGE_SIZE: Final = 1000
PREPARE_FILE_LIMIT: Final = 10000
LAUNCH_STATE_LIMIT: Final = 1000
PROMOTE_CANDIDATE_LIMIT: Final = 500
FUNCTION_TIME_BUDGET_MINUTES: Final = 7
LOCAL_RATE_LIMIT_SLEEP_SECONDS: Final = 900

# Diagram detect config - see DiagramDetectConfig in the Cognite SDK docs. None = use API default.
MIN_TOKENS: Final = 2
ANNOTATION_EXTRACT: Final[bool | None] = None  # cannot be True together with READ_EMBEDDED_TEXT
CASE_SENSITIVE: Final[bool | None] = None
NO_TEXT_INBETWEEN: Final = True
NATURAL_READING_ORDER: Final = True
FUZZINESS_FUZZY_SCORE: Final[float | None] = None
FUZZINESS_MAX_BOXES: Final[int | None] = None
FUZZINESS_MIN_CHARS: Final = 4
DIRECTION_DELTA: Final[float | None] = None
DIRECTION_WEIGHTS: Final[dict[str, float] | None] = {"left": 1.0, "right": 1.0, "up": 1.0, "down": 1.0}  # keys: left, right, up, down
MIN_FUZZY_SCORE: Final = 1
READ_EMBEDDED_TEXT: Final = True
REMOVE_LEADING_ZEROS: Final[bool | None] = None
SUBSTITUTIONS: Final[dict[str, list[str]] | None] = None  # e.g. {"0": ["O", "Q"]}

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

TAG_ANNOTATED: Final = "Annotated"
TAG_ANNOTATION_FAILED: Final = "AnnotationFailed"
TAG_ANNOTATION_IN_PROCESS: Final = "AnnotationInProcess"
TAG_DETECT_IN_DIAGRAMS: Final = "DetectInDiagrams"
TAG_PROMOTE_ATTEMPTED: Final = "PromoteAttempted"
TAG_TO_ANNOTATE: Final = "ToAnnotate"
EXCLUDED_PREPARE_TAGS: Final = [TAG_ANNOTATION_IN_PROCESS, TAG_ANNOTATED, TAG_ANNOTATION_FAILED]
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
