# Instances are read and written one page at a time, so memory stays bounded by the page
# rather than by the number of instances in the configured spaces.
ALIAS_PAGE_SIZE = 1000
ITEMS_QUERY_NAME = "items"
# The only properties alias generation reads.
ALIAS_SOURCE_PROPERTIES = ["name", "aliases"]
TS_NODE = "timeseries"
ASSET_NODE = "assets"
FILE_NODE = "files"
# Tag shape used to derive an alias from a name when a view configures no aliasPattern,
# e.g. VAL_23-KA-9101 -> 23-KA-9101. The alias is the capture groups joined by "-".
# Spelled with [0-9] rather than \d to stay identical to the configured default: Toolkit
# substitutes variables as a regex replacement, which rejects backslash escapes.
DEFAULT_ALIAS_PATTERN = r"([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})"
