# Tag pattern library location

The ISA / generic tag and document pattern YAML lives in the **module config** (shared by key extraction authoring and aliasing):

[`../config/tag_patterns.yaml`](../config/tag_patterns.yaml)

Python: `cdf_key_extraction_aliasing.config.tag_patterns_paths.TAG_PATTERNS_YAML`

Registries load it from [`engine/tag_pattern_library.py`](engine/tag_pattern_library.py) (`StandardTagPatternRegistry`, `DocumentPatternRegistry`).

The **`alphanumeric_tag`** entry should stay aligned with the **`&alphanumeric_tag`** anchor in `key_extraction_aliasing.yaml` at module root (shared equipment-tag regex for extraction). See [key extraction / aliasing report](../../docs/key_extraction_aliasing_report.md).
