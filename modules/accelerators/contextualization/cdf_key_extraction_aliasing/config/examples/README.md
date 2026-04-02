# Config examples

**Full documentation index:** [docs/README.md](../../docs/README.md).

Examples are grouped by **category** (not one folder per file):

| Folder | Contents |
|--------|----------|
| **`key_extraction/`** | Example scope YAML for extraction demos only (`*.key_extraction_aliasing.yaml`). |
| **`aliasing/`** | Aliasing-focused example scope YAML (minimal `key_extraction` stub + `aliasing`). |
| **`reference/`** | Field-by-field reference (`config_example_complete.yaml`), wrapped `reference_key_extraction_aliasing.yaml`, and **`LEGACY_TO_NEW_*.md`**. |

Each example file uses the same v1 scope shape as `workflow.local.config.yaml` at module root: optional `schemaVersion: 1`, `key_extraction`, optional `aliasing`.

**Not the same as the default scope:** `workflow.local.config.yaml` is the slim **CDM** template (shared `alphanumeric_tag`, six extraction rules, small aliasing stack). Files under **`examples/`** are often **richer** (extra methods, more aliasing rules) for learning and tests.

The local CLI loads **`--config-path`** or the module-root default for **`--scope default`** only; it does not merge or auto-discover files under **`examples/`**.

## Usage

1. **Local run** (cwd = module root `cdf_key_extraction_aliasing/`):  
   `main.py --config-path config/examples/key_extraction/<demo>.key_extraction_aliasing.yaml`  
   or `main.py --config-path config/examples/aliasing/aliasing_default.key_extraction_aliasing.yaml`.  
   From the **repository root** (`library/`), use the full prefix `modules/accelerators/contextualization/cdf_key_extraction_aliasing/` on `main.py` and on these paths (see the module [README.md](../../README.md) and [Quickstart](../../docs/guides/howto_quickstart.md) for `PYTHONPATH` and `.env`).
2. **CDF**: Inline `key_extraction` / `aliasing` from those YAML files into your workflow task payload.

Default aliasing-focused example: **`aliasing/aliasing_default.key_extraction_aliasing.yaml`**.

## Progressive testing (`key_extraction/`)

Suggested order (simple → richer):

1. `regex_pump_tag_simple.key_extraction_aliasing.yaml`
2. `regex_instrument_tag_capture.key_extraction_aliasing.yaml`
3. `fixed_width_single.key_extraction_aliasing.yaml` / `fixed_width_multiline.key_extraction_aliasing.yaml`
4. `token_reassembly.key_extraction_aliasing.yaml`
5. `heuristic_positional.key_extraction_aliasing.yaml`, `heuristic_learning.key_extraction_aliasing.yaml`, `heuristic_comprehensive.key_extraction_aliasing.yaml`
6. `passthrough.key_extraction_aliasing.yaml`

Also in this folder (not in the sequence above): `comprehensive_default.key_extraction_aliasing.yaml`, `multi_field.key_extraction_aliasing.yaml`, `field_selection_demo.key_extraction_aliasing.yaml`.

## Reference files (`reference/`)

- `config_example_complete.yaml` — flat parameters/data field reference.
- `reference_key_extraction_aliasing.yaml` — same content in `key_extraction_aliasing`-shaped scope YAML.

Legacy split `*.config.yaml` layouts were migrated to the current `*.key_extraction_aliasing.yaml` tree in git history; use the repository history if you need to reproduce that migration.
