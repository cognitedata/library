# Tests — `cdf_discovery_aliasing`

## Layout

Paths are relative to this directory.

```
tests/
├── conftest.py
├── fixtures/
│   ├── sample_data.py
│   └── aliasing/
├── unit/
│   ├── cdf_fn_common/        # workflow compile, scope trim, discovery helpers, …
│   ├── aliasing/
│   ├── alias_persistence/    # legacy package marker only (`__init__.py`); discovery tests live under cdf_fn_common/
│   ├── docs/
│   ├── local_runner/
│   └── scope_build/
├── integration/
│   ├── aliasing/
│   ├── contextualization/
│   └── ...
├── run_tests_and_save_results.py
├── view_detailed_results.py
└── results/
```

## Running tests

From **repository root** (the directory that contains `modules/`):

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_discovery_aliasing/tests -q
```

Verbose:

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_discovery_aliasing/tests -v --tb=short
```

Single file or node:

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_discovery_aliasing/tests/unit/local_runner/test_config_loading.py -v
```

Coverage (if `pytest-cov` is installed):

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_discovery_aliasing/tests --cov=modules.accelerators.contextualization.cdf_discovery_aliasing --cov-report=html
```

## Fixtures

Shared sample entities and tags live under `fixtures/`. Prefer importing from `tests.fixtures` package paths consistent with your `PYTHONPATH` (repository root on path).

## Result JSON and reports

- **`results/`** — pytest and harness JSON (e.g. `run_tests_and_save_results.py`, detailed generators). `*.json` and `*.txt` here are **gitignored** at the repo root (`__init__.py` stays tracked as a package marker). **`module.py run`** pipeline pairs live under **`../local_run_results/`** at module root.
- **`generate_detailed_results.py`** — removed; use **`python module.py run`** and inspect **`local_run_results/`**.
- **Run report** — `scripts/generate_report.py` writes `local_run_results/run_report.md` from the latest `local_run_results/*_cdf_extraction.json` pair. Default scope narrative: [configuration guide — Default CDM scope](../docs/guides/configuration_guide.md#default-cdm-scope).

## Documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md) (CLI and architecture overview)
- [Quickstart — `.env` and `module.py`](../docs/guides/howto_quickstart.md)
- [Build configuration with YAML](../docs/guides/howto_config_yaml.md) · [Build configuration with the UI](../docs/guides/howto_config_ui.md)
- [Scoped deployment — `--build`, triggers, Toolkit](../docs/guides/howto_scoped_deployment.md)
