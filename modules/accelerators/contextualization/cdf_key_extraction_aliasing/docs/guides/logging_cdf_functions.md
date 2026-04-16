# Logging for CDF functions and pipelines

## Workflow payload (`data`)

CDF Data workflow tasks should keep passing:

- **`logLevel`**: `DEBUG` | `INFO` | `WARNING` | `ERROR` (default `INFO` if omitted or invalid in most functions).
- **`verbose`**: boolean — enables `CogniteFunctionLogger.verbose(...)` detail paths in engines.

Do not require new keys for logging; optional handler arguments are for tests and local runners only.

## Logger interface (pipelines and engines)

Pipelines (`key_extraction`, `tag_aliasing`, `persist_reference_index`, `incremental_state_update`, `persist_aliases_to_entities`) and engines expect a logger that supports:

| Method | Role |
|--------|------|
| `info(msg)`, `warning(msg)`, `error(msg)`, `debug(msg)` | Standard messages |
| **`verbose(log_level, msg)`** | Rule/engine detail when `verbose` is true (`log_level` is a string like `"DEBUG"`) |

The default implementation is **`CogniteFunctionLogger`** in [`functions/cdf_fn_common/logger.py`](../../functions/cdf_fn_common/logger.py).

**Do not** pass a plain stdlib `logging.Logger` into pipelines without wrapping: use **`StdlibLoggerAdapter`** from [`functions/cdf_fn_common/function_logging.py`](../../functions/cdf_fn_common/function_logging.py) (same behavior as the local CLI verbose bridge).

## Building the logger

- **`function_logger_from_data(data)`** — from task `data` (`logLevel`, `verbose`); used inside `handle()` when no logger is injected.
- **`resolve_function_logger(data, logger=None, strict_level_names=False)`** — if `logger` is `None`, same as above (or legacy key-extraction level rules when `strict_level_names=True`); if a stdlib `Logger`, wraps with `StdlibLoggerAdapter`; if it already has `verbose` + `info`, uses as-is.
- **`cognite_function_logger(level, verbose, on_invalid_level=..., strict_level_names=False)`** — low-level; most code should use `function_logger_from_data`. **`fn_dm_key_extraction`** uses **`strict_level_names=True`** so only exact `DEBUG|INFO|WARNING|ERROR` strings are accepted (matching the original `dependencies.create_logger_service`).

## Optional `logger` on handlers

Each `fn_dm_*` **`handle(data, client=None, logger=None)`** may receive an optional third argument for tests or orchestration. Workflow deployments should omit it so behavior matches prior `handle(data, client)` calls.

## `create_logger_service` in `dependencies.py`

Local `dependencies.create_logger_service(level, verbose)` delegates to **`cognite_function_logger`** with the same **`strict_level_names`** semantics as **`function_logging`** ( **`fn_dm_key_extraction`**: strict names only; other `fn_dm_*` dependencies: normalized levels with invalid → `INFO` + preserved `verbose`).

## Local CLI (`module.py` / `local_runner.run`)

On **non-incremental** runs, [`local_runner/run.py`](../../local_runner/run.py) builds a single **`StdlibLoggerAdapter`** around the CLI root logger and passes it into **`KeyExtractionEngine`** and **`AliasingEngine`**, so engine `info` / `warning` / `verbose` lines use the same stdlib logging stream as the rest of `module.py` (including [`tag_pattern_library.py`](../../functions/fn_dm_aliasing/engine/tag_pattern_library.py), which already uses `logging`).

- **`--verbose`** — sets the root logger to **DEBUG**; engine `.verbose(...)` calls are forwarded at the requested level via the adapter.
- **`--progress-every N`** (default **0** = off) — logs throttled **INFO** progress for long key discovery and aliasing loops (every **N** entities and every **N** tags, per view). Does not change workflow or deployed functions.
- Before each data-model **`instances.list`**, the runner logs a short **Querying data model instances…** line so slow network calls are easier to distinguish from extraction/aliasing work.
