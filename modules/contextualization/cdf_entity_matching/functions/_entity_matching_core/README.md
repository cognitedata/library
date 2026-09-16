# Entity matching shared code

Code shared by the two asynchronous entity matching functions. **This folder is the only
place it is edited.**

A Cognite Function is deployed as one self-contained folder, so the modules each
function imports have to exist inside `fn_dm_context_entity_matching_submit/` and
`fn_dm_context_entity_matching_collect/`. Those copies are generated:

```bash
python scripts/sync_entity_matching_core.py          # write the copies
python scripts/sync_entity_matching_core.py --check  # fail if a copy is stale (CI)
```

Every copy starts with a banner naming this folder as its source.
`tests/test_entity_matching_core_sync.py` fails the build when a copy drifts, so an edit
made in a function folder by mistake is caught rather than silently deployed.

This folder is not a function: it has no `handler.py` and the Toolkit does not deploy it.

## Modules

| Module | Copied into | Contents |
|---|---|---|
| `em_submit.py` | submit | Manual and rule mappings, staging, starting the predict job |
| `em_collect.py` | collect | Polling the queue, merging results, writing them |
| `em_targets.py` | submit | Reading targets: sync cursor in RAW, content cached in a CDF file |
| `em_job_state.py` | both | The predict job queue in the RAW state store |
| `em_staging.py` | both | Manual and rule matches parked in temporary CDF files while a predict job runs |
| `em_pipeline.py` | both | The matching steps both functions use |
| `em_config.py` | both | Extraction pipeline configuration, and the summary written to the log |
| `em_constants.py`, `em_logger.py`, `em_pipeline_types.py`, `em_pipeline_optimizations.py` | both | Supporting code |

Submit does not ship `em_collect.py`. Collect does not ship `em_submit.py` or `em_targets.py`.

## Selecting unmatched entities (`em_pipeline.py`)

`get_query_filter` scopes a read to the view, spaces and configured `filterProperty`. It
does **not** filter on the link property. Already-linked entities are dropped in
`get_new_entities` immediately after the read, before the duplicate-space warning and the
entity count, by reading the link value off each instance.

The reason is that [`exists` counts an empty array as a value on the query
endpoint](https://docs.cognite.com/cdf/dm/dm_concepts/dm_search#exists-filter-with-empty-array)
that `instances.list` uses. `NOT exists(assets)` therefore matches nothing for entities
whose links were written as `[]` instead of being left unset, and the run reads zero
entities while plenty are unlinked.

## Target cache (`em_targets.py`)

The cursor lives in RAW (`state_target_sync_<key>`). The instance content lives in a CDF
file (`em_target_cache_<key>.json`). When sync reports changes, that file is **overwritten**
(`overwrite=True`) and the RAW row is **upserted**. Neither is deleted. A configuration
change produces a new `<key>` and therefore a new file and row; previous ones are left
in place unused.

A 408 on a sync page shrinks the page size by 20% (1000 down to 100). Every failure,
including timeouts, counts toward `TARGET_SYNC_MAX_RETRIES` (4).

## Retries (`em_pipeline_optimizations.py`)

`is_retryable` decides what is worth another attempt (408, 429, 5xx, and unclassified
transport errors — not 4xx or programming mistakes). `RobustAPIClient` uses that filter
on its `@retry` decorator so a 403 or `TypeError` fails immediately.

Creating a RAW database or table ignores the **400** a re-run gets for one that is
already there, and re-raises `401`, `403` and `5xx`. RAW does not use `409` for this, and
words it differently per resource ("Databases with the following names already exists"
but "Tables already created"), so the status code is what is matched on.

Both handlers **re-raise** on failure. A handler that returns normally is a succeeded
function call in the CDF UI, so returning `{"status": "failure"}` would hide the error.

A failed extraction-pipeline run message includes a traceback only when the call is
inside an active exception. A logical collect failure (predict job status `Failed`) does
not append `NoneType: None`.

## Why the `em_` prefix

A function folder is deployed flat, so every module in it is a top-level module — and
other functions in this repo ship their own `pipeline.py`, `config.py` and `logger.py`.
Tools that analyse the repository as a whole, CodeQL and `pytest` among them, cannot tell
those apart and resolve an import to whichever one they meet first, which reports calls
against the wrong function's signature. The prefix makes each module name unique.

## Adding a module

Add the `.py` file here, named `em_*.py`, list it in `FUNCTION_MODULES` in
`scripts/sync_entity_matching_core.py` for the function(s) that import it, and run the
sync script.
