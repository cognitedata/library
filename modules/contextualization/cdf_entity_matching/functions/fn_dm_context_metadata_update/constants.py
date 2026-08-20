# Limit used when listing instances: -1 means fetch all matches, letting the SDK
# paginate internally. A positive value is not supported, as get_new_items has no
# cursor and would re-read the first page on every pass.
BATCH_SIZE = -1
MANAGED_ASSET_TAG_PREFIX = "root:"
# Literal value an earlier version of this function wrote instead of a root tag.
# Stripped from asset tags whenever encountered.
INVALID_ASSET_TAG = "tag"
TS_NODE = "timeseries"
ASSET_NODE = "assets"
