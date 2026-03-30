# Quick Start Guide

This guide covers running the **cdf_key_extraction_aliasing** package from the repository root (`library/`) with correct Python import paths.

## Prerequisites

- Python 3.11+ (see main [README](../../README.md))
- Cognite Data Fusion (CDF) project and credentials (API key or OAuth), if you call CDF APIs
- Dependencies installed (`poetry install` or equivalent)

## Layout

- **Package root**: `modules/accelerators/contextualization/cdf_key_extraction_aliasing/`
- **Combined scopes**: `config/scopes/<scope>/key_extraction_aliasing.yaml` (default: `config/scopes/default/key_extraction_aliasing.yaml`)
- **Example YAML**: `config/examples/key_extraction/` and `config/examples/aliasing/` (`*.key_extraction_aliasing.yaml`). Reference: `config/examples/reference/`.
- **Entry point for CDF-backed runs**: `main.py` (loads `config/scopes/<scope>/key_extraction_aliasing.yaml` or `--config-path`)

## Using the Key Extraction Engine

```python
from pathlib import Path

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    load_config_from_yaml,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)

# Run with cwd = repository root (the folder that contains `modules/`), or set REPO_ROOT
repo_root = Path.cwd()
config_path = (
    repo_root
    / "modules/accelerators/contextualization/cdf_key_extraction_aliasing/config/examples/key_extraction/comprehensive_default.key_extraction_aliasing.yaml"
)

config_dict = load_config_from_yaml(str(config_path))
engine = KeyExtractionEngine(config_dict)

entity = {"name": "P-101", "description": "Main pump", "id": "asset-1"}
result = engine.extract_keys(entity, "asset")

print(f"Candidate keys: {[k.value for k in result.candidate_keys]}")
```

`extract_keys` takes a single **entity** dict and an **entity_type** string (`asset`, `file`, `timeseries`, etc.), matching `main.py` and the CDF function pipeline.

## Using the Aliasing Engine

```python
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)

# Minimal config; production uses rules from config/scopes/.../key_extraction_aliasing.yaml (or examples/aliasing/aliasing_default.key_extraction_aliasing.yaml) via main.py / fn_dm_aliasing
aliasing_engine = AliasingEngine({"rules": [], "validation": {}})
out = aliasing_engine.generate_aliases("P-101", "asset")
print(out.aliases)
```

`generate_aliases` returns an **`AliasingResult`** with `.aliases` (list of strings) and `.metadata`.

## Full CDF pipeline (local)

From the **repository root**:

```bash
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --dry-run
```

Omit `--dry-run` to persist aliases to CogniteDescribable (see [README](../../README.md) **Alias write-back**). Add `--write-foreign-keys` and `--foreign-key-writeback-property <name>` when you want extracted foreign-key strings written to the same persistence step ([Foreign key write-back](../../README.md#foreign-key-write-back)).

## Configuration loading utilities

- **YAML → engine (key extraction)**: `load_config_from_yaml` in `functions/fn_dm_key_extraction/cdf_adapter.py`
- **Environment / structured config**: `config/configuration_manager.py` (`ConfigurationManager`, `load_config_from_env` for typed `KeyExtractionConfig` where used)

## More documentation

- [Configuration guide](configuration_guide.md) — pipeline YAML structure, filters, validation
- [Key extraction spec](../1.%20key_extraction.md) / [Aliasing spec](../2.%20aliasing.md)
- [Troubleshooting](../troubleshooting/common_issues.md)
- [Workflows](../../workflows/README.md) — `cdf_key_extraction_aliasing` workflow and tasks
