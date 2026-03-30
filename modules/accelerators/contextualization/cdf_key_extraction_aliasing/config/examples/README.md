# Config examples

Examples are grouped by **category** (not one folder per file):

| Folder | Contents |
|--------|----------|
| **`key_extraction/`** | Combined v1 YAML for extraction demos only (`*.key_extraction_aliasing.yaml`). |
| **`aliasing/`** | Aliasing-focused combined v1 YAML (minimal `key_extraction` stub + `aliasing`). |
| **`reference/`** | Field-by-field reference (`config_example_complete.yaml`), wrapped `reference_key_extraction_aliasing.yaml`, and **`LEGACY_TO_NEW_*.md`**. |

Each combined file uses the same shape as `config/scopes/<scope>/key_extraction_aliasing.yaml`: optional `schemaVersion: 1`, `key_extraction`, optional `aliasing`.

The local CLI loads **`--config-path`** or **`config/scopes/<scope>/key_extraction_aliasing.yaml`** only; it does not merge or auto-discover files under **`examples/`**.

## Optional: bulk-generate combined YAML from split `*.config.yaml`

Maintainers with split pipeline-style YAML can run from the module root:

```bash
python scripts/reorganize_config_examples.py
```
