# Plan: CLI clean-state for incremental RAW tables

## Goal

Add a reusable function and `main.py` flags to **drop RAW tables** that hold incremental pipeline state so a subsequent run can **fully reprocess** state update → key extraction → reference index → aliasing → persistence without stale cohorts, watermarks, inverted index rows, or aliasing RAW caches.

## Current gap

- `main.py` has no clean-state path.
- `local_runner/run.py` already derives the reference-index table name via `_reference_index_raw_table_from_key_extraction_table` (must stay in sync with any cleaner).

## Tables to target (from scope YAML)

Read the v1 scope file (**module-root `key_extraction_aliasing.yaml`** with `--scope default`, or **`--config-path`**). Build a **deduplicated** list of `(raw_db, table)` pairs:

| Source in YAML | Database | Table |
|----------------|----------|--------|
| `key_extraction.config.parameters` | `raw_db` | `raw_table_key` |
| Same as key extraction | `raw_db` | **Reference index** = `f(raw_table_key)` — **same logic as** `local_runner.run._reference_index_raw_table_from_key_extraction_table` |
| `aliasing.config.parameters` (if `aliasing` present) | `raw_db` | `raw_table_state` |
| Same | `raw_db` | `raw_table_aliases` |

**Reference index = FK + document inverted index** (`fn_dm_reference_index`); there is no separate “FK-only” RAW table.

### Explicitly out of scope

- **Data model** properties (e.g. `aliases`, `references_found` on CogniteDescribable) — not cleared by RAW table delete.
- **WorkflowDefinition / deployed functions** — unchanged.

## Implementation steps

1. **`functions/cdf_fn_common/clean_state_tables.py`**
   - `reference_index_table_from_key_extraction_table(raw_table_key: str) -> str` — duplicate the four-branch logic from `run.py` (or refactor `run.py` to import from `cdf_fn_common` to avoid drift; single source of truth preferred).
   - `_collect_db_table_pairs_from_scope_doc(doc: dict) -> list[tuple[str, str]]` — parse `key_extraction` / `aliasing` branches; skip empty strings; dedupe pairs.
   - `_delete_raw_table_if_exists(client, raw_db, table, logger)` — `client.raw.tables.delete(raw_db, [table])`; on `CogniteAPIError` with `code == 404`, log and continue; re-raise other errors.
   - `clean_state_tables_from_scope_yaml(client, logger, scope_yaml_path: Path) -> list[str]` — load YAML with `yaml.safe_load`, collect pairs, delete each, return `["db/table", ...]` for logging.

2. **`functions/cdf_fn_common/__init__.py`**
   - Export `clean_state_tables_from_scope_yaml` (and optionally `reference_index_table_from_key_extraction_table` if tests need it).

3. **`main.py`**
   - Mutually exclusive group:
     - `--clean-state` — run cleaner, then existing `run_pipeline(...)`.
     - `--clean-state-only` — run cleaner, log summary, `sys.exit(0)`.
   - Resolve `scope_yaml_path` **before** clean (same logic as today: `--config-path` or module-root default via `--scope default`).
   - After `create_cognite_client` + `load_configs`, invoke cleaner when either flag is set (configs already validate scope file exists for incremental path; for clean-only, still require readable scope path).
   - Extend module docstring with one line describing the flags.

4. **Optional refactor (recommended)**

   - Move `_reference_index_raw_table_from_key_extraction_table` to `cdf_fn_common` (e.g. `reference_index_naming.py` or inside `clean_state_tables.py`) and have `local_runner/run.py` import it — **one** place for workflow/local parity naming.

5. **Tests** — `tests/unit/test_clean_state_tables.py`
   - Parametrize `reference_index_table_from_key_extraction_table` vs known inputs (`key_extraction_state`, `site_key_extraction_state`, empty, arbitrary suffix).
   - `_collect_db_table_pairs_from_scope_doc` with key-extraction-only and with aliasing block.
   - `clean_state_tables_from_scope_yaml` with temp scope file + `MagicMock` client asserting `raw.tables.delete` calls for expected `(db, [table])` pairs.

6. **Docs (minimal)**

   - One paragraph in `README.md` or `docs/guides/configuration_guide.md` under operations/troubleshooting: when to use `--clean-state` vs `--full-rescan`, and that DM write-back is not cleared.

## Edge cases

| Case | Behavior |
|------|----------|
| Scope missing `raw_db` / `raw_table_key` | Log warning; delete nothing (or only aliasing tables if present). |
| `reference_index_raw_db` ≠ `source_raw_db` in a **custom** workflow | Optional `key_extraction.config.parameters.reference_index_raw_db` and `reference_index_raw_table` are read by the cleaner when set (Pydantic `Config` may ignore extras for other tools; RAW clean loads the scope YAML directly). |
| `incremental_change_processing: false` | Clean still removes RAW tables named in YAML; next incremental-on run recreates via `create_table_if_not_exists`. Harmless if tables unused. |
| `--instance-space` | Does not shrink which tables are dropped — RAW is scope-wide. |

## Verification

- `python -m pytest .../tests/unit/test_clean_state_tables.py -v`
- Manual: `--clean-state-only` against a dev project; confirm tables gone in RAW UI; then run without flag and confirm pipeline recreates and completes.

## Suggested operator flow

Full reprocess in incremental mode:

```bash
python main.py --clean-state --full-rescan
```

Wipe state without running pipeline:

```bash
python main.py --clean-state-only
```
