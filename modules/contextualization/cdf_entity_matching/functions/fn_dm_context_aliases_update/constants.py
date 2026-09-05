# Limit used when listing instances: -1 means fetch all matches, letting the SDK
# paginate internally. A positive value is not supported, as get_new_items has no
# cursor and would re-read the first page on every pass.
BATCH_SIZE = -1
TS_NODE = "timeseries"
ASSET_NODE = "assets"
FILE_NODE = "files"
# Tag shape used to derive an alias from a name when a view configures no aliasPattern,
# e.g. VAL_23-KA-9101 -> 23_KA_9101. The alias is the capture groups joined by "_".
# Spelled with [0-9] rather than \d to stay identical to the configured default: Toolkit
# substitutes variables as a regex replacement, which rejects backslash escapes.
DEFAULT_ALIAS_PATTERN = r"([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})"
