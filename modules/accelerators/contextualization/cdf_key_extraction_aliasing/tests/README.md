# Tests — `cdf_key_extraction_aliasing`

## Layout

Paths are relative to this directory.

```
tests/
├── conftest.py
├── fixtures/
│   ├── sample_data.py
│   ├── key_extraction/
│   └── aliasing/
├── unit/
│   ├── key_extraction/       # engine, handlers, confidence, regex options, FK helpers
│   ├── aliasing/
│   ├── alias_persistence/
│   ├── docs/                 # documentation map / how-to guardrails
│   ├── local_runner/         # scope YAML loading for module.py
│   └── scope_build/
├── integration/
│   ├── key_extraction/       # scenarios (regex, heuristics, …)
│   ├── aliasing/             # handlers, tag_pattern_library
│   ├── contextualization/    # configuration_manager, full workflow, edge cases
│   └── ...
├── test_pipeline_extraction.py
├── generate_detailed_results.py
├── run_tests_and_save_results.py
├── view_detailed_results.py
└── results/                  # JSON outputs from local runs (often gitignored)
```

## Running tests

From **repository root** (the directory that contains `modules/`):

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests -q
```

Verbose:

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests -v --tb=short
```

Single file or node:

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/unit/local_runner/test_config_loading.py -v
```

Coverage (if `pytest-cov` is installed):

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests --cov=modules.accelerators.contextualization.cdf_key_extraction_aliasing --cov-report=html
```

## Fixtures

Shared sample entities and tags live under `fixtures/`. Prefer importing from `tests.fixtures` package paths consistent with your `PYTHONPATH` (repository root on path).

## Result JSON and reports

- **`results/`** — timestamped `*_cdf_extraction.json` / `*_cdf_aliasing.json` from pipeline-style runs (same artifacts described in [Quickstart — local `module.py`](../docs/guides/howto_quickstart.md)). `*.json` and `*.txt` here are **gitignored** at the repo root (`__init__.py` stays tracked as a package marker).
- **`generate_detailed_results.py`** — build structured summaries without executing the full test suite (see script docstring).
- **Module report** — [docs/key_extraction_aliasing_report.md](../docs/key_extraction_aliasing_report.md) documents the default scope; `scripts/generate_report.py` can regenerate it from the latest `tests/results/*_cdf_extraction.json` if you want run-specific stats (overwrites the file).

## Documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md) (CLI and architecture overview)
- [Quickstart — `.env` and `module.py`](../docs/guides/howto_quickstart.md)
- [Build configuration with YAML](../docs/guides/howto_config_yaml.md) · [Build configuration with the UI](../docs/guides/howto_config_ui.md)
- [Scoped deployment — `--build`, triggers, Toolkit](../docs/guides/howto_scoped_deployment.md)
