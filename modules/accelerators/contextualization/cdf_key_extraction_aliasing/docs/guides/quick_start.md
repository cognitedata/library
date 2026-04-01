# Quick start (developers)

Run and import **`cdf_key_extraction_aliasing`** with the **repository root** (the folder that contains `modules/`) on **`PYTHONPATH`**, or execute scripts with `PYTHONPATH=.` from that root.

## Prerequisites

- Python 3.11+
- Dependencies installed in your environment (this repository may not ship a root `pyproject.toml`; use your team’s env or install required packages manually)
- CDF credentials in `.env` or the environment when calling CDF APIs or `main.py` without `--dry-run`

## Layout (paths in-repo)

- **Module root:** `modules/accelerators/contextualization/cdf_key_extraction_aliasing/`
- **Default scope document:** `key_extraction_aliasing.yaml` at module root
- **Examples:** `config/examples/key_extraction/`, `config/examples/aliasing/`, `config/examples/reference/` — see [config/examples/README.md](../../config/examples/README.md)
- **Entry point for full local pipeline:** `main.py` (loads scope YAML or `--config-path`)

## Key extraction engine

```python
from pathlib import Path

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    load_config_from_yaml,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)

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

`extract_keys(entity_dict, entity_type)` matches the shape used by `main.py` and the CDF function (`asset`, `file`, `timeseries`, etc.).

## Aliasing engine

```python
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)

aliasing_engine = AliasingEngine({"rules": [], "validation": {}})
out = aliasing_engine.generate_aliases("P-101", "asset")
print(out.aliases)
```

Production rules come from `key_extraction_aliasing.yaml`, `--config-path`, or an example `*.key_extraction_aliasing.yaml` (e.g. `config/examples/aliasing/aliasing_default.key_extraction_aliasing.yaml`). For **`alias_mapping_table`** rules that load **RAW**, construct `AliasingEngine(..., client=cognite_client)`.

## Full pipeline locally (`main.py`)

From repository root:

```bash
PYTHONPATH=. python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --dry-run
```

Omit `--dry-run` to persist aliases (see module [README](../../README.md) — **Alias write-back**). Optional FK persistence: `--write-foreign-keys` and `--foreign-key-writeback-property`. **`--instance-space`:** keep only `source_views` whose `instance_space` matches, or whose `filters` include a node `space` filter (`property_scope: node`, `EQUALS` or `IN`) for that space — see [Configuration guide](configuration_guide.md#source-views-configuration).

## Config utilities

- **YAML → key-extraction engine dict:** `load_config_from_yaml` in `functions/fn_dm_key_extraction/cdf_adapter.py`
- **Typed / schema config (tests, tooling):** `config/configuration_manager.py`

## Where to read next

- [Documentation map](../README.md)
- [Configuration guide](configuration_guide.md)
- [Key extraction spec](../specifications/1.%20key_extraction.md) / [Aliasing spec](../specifications/2.%20aliasing.md)
- [Troubleshooting](../troubleshooting/common_issues.md)
- [Workflows](../../workflows/README.md)
