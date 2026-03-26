# Key Extraction and Aliasing — Documentation Index

Documentation for **`cdf_key_extraction_aliasing`** (`modules/accelerators/contextualization/cdf_key_extraction_aliasing/`).

## Specifications

- [Key extraction](1.%20key_extraction.md) — extraction methods, rules, types
- [Aliasing](2.%20aliasing.md) — transformation types, rules, engine behavior

## Guides

- [Quick start](guides/quick_start.md) — imports, `KeyExtractionEngine` / `AliasingEngine`, `main.py`
- [Configuration guide](guides/configuration_guide.md) — YAML pipelines, `source_views`, filters, aliasing parameters (`alias_writeback_property`, optional `write_foreign_key_references` / `foreign_key_writeback_property`), **`alias_mapping_table`** (RAW catalog rules)

## Operations

- [Workflows](../workflows/README.md) — CDF workflow `cdf_key_extraction_aliasing`, tasks, deployment
- [Workflow diagram source](../workflows/workflow_diagram.md)

## Reference & reports

- [Troubleshooting](troubleshooting/common_issues.md)
- [Sample / generated results summary](key_extraction_aliasing_report.md) — produced by `main.py` / report scripts when runs exist
- [Pipeline configurations README](../pipelines/PIPELINE_CONFIGURATIONS_README.md)

## Other

- [EXTRACTION_PIPELINE_REDUNDANCY_REPORT.md](EXTRACTION_PIPELINE_REDUNDANCY_REPORT.md) — internal analysis
- [aliasing_simplification_analysis.md](aliasing_simplification_analysis.md) — internal notes

## Package entry points

- **CLI**: [README.md](../README.md) — `main.py`, options, tests, CDF functions
- **Engines**: `functions/fn_dm_key_extraction/engine/`, `functions/fn_dm_aliasing/engine/`
- **CDF functions**: `fn_dm_key_extraction`, `fn_dm_aliasing`, `fn_dm_alias_persistence`
