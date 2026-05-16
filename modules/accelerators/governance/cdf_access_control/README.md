# cdf_access_control

Offline generator for Cognite Toolkit **Space** and **Group** YAML from `default.config.yaml` (dimensions, spaces, groups).

## Commands

```bash
python module.py build [--config default.config.yaml] [--dry-run] [--force]
python module.py build --clean [--yes]
python module.py ui   # API :8775, Vite :5183
```

## Tests

```bash
pip install -r requirements.txt
PYTHONPATH=scripts pytest tests/unit/ -q
```

## Note on restoration

This module was recovered after an untracked tree was removed by `git clean`. Python sources and English i18n (`ui/src/i18n/en.ts`, 204 keys) were rebuilt from bytecode and the last `ui/dist` bundle. Structured React editors (`DimensionsEditor`, etc.) are not yet restored; use `ui/dist` for the last full UI build or the minimal dev UI under `ui/src/`.
