# Tests — `cdf_file_asset_source`

Pytest layout for the file-asset-source accelerator.

## Run

From repository root:

```bash
export PYTHONPATH=.
pytest modules/accelerators/contextualization/cdf_file_asset_source/tests/unit/ -q
```

## Layout

- `tests/unit/` — unit tests (config validation, scope tree, local runner helpers)
- `conftest.py` — shared fixtures where present

See module [docs/README.md](../docs/README.md) for documentation index.
