# Instances are read and written one page at a time, so memory stays bounded by the page
# rather than by the number of instances in the configured spaces.
ALIAS_PAGE_SIZE = 1000
# A page that times out is read again 20% smaller, down to the minimum, and the size that
# worked is kept in the state store for the next run.
ALIAS_MIN_PAGE_SIZE = 100
ALIAS_PAGE_SIZE_FACTOR = 0.8
ALIAS_SYNC_MAX_RETRIES = 4
ALIAS_SYNC_RETRY_BACKOFF_SECONDS = 2
ITEMS_QUERY_NAME = "items"
# The only properties alias generation reads.
ALIAS_SOURCE_PROPERTIES = ["name", "aliases"]
TS_NODE = "timeseries"
ASSET_NODE = "assets"
FILE_NODE = "files"
# One state store row per view, keyed by this prefix, the view label and a fingerprint of
# what is read, so a changed view, space or filter never reuses another read's cursor.
SYNC_STATE_KEY_PREFIX = "state_alias_sync_"
# Raise when a change to the read alters which instances a cursor covers.
SYNC_STATE_VERSION = 1
SYNC_COL_CURSOR = "syncCursor"
SYNC_COL_BATCH_SIZE = "batchSize"
SYNC_COL_FULL_READ = "fullReadInProgress"
SYNC_COL_VIEW = "view"
SYNC_COL_UPDATED_AT = "updatedAt"
HTTP_STATUS_BAD_REQUEST = 400
HTTP_STATUS_NOT_FOUND = 404
HTTP_STATUS_REQUEST_TIMEOUT = 408
# Tag shape used to derive an alias from a name when a view configures no aliasPattern,
# e.g. VAL_23-KA-9101 -> 23-KA-9101. The alias is the capture groups joined by "-".
# Spelled with [0-9] rather than \d to stay identical to the configured default: Toolkit
# substitutes variables as a regex replacement, which rejects backslash escapes.
DEFAULT_ALIAS_PATTERN = r"([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})"
