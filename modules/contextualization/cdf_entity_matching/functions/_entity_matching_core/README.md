# Entity matching shared code

Code shared by the two asynchronous entity matching functions. **This folder is the only
place it is edited.**

A Cognite Function is deployed as one self-contained folder, so the same modules have to
exist inside `fn_dm_context_entity_matching_submit/` and
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

| Module | Contents |
|---|---|
| `em_submit.py` | Function 1: manual and rule mappings, staging, starting the predict job |
| `em_collect.py` | Function 2: polling the queue, merging results, writing them |
| `em_job_state.py` | The predict job queue in the RAW state store |
| `em_staging.py` | Manual and rule matches parked in RAW while a predict job runs |
| `em_targets.py` | Reading the targets: sync cursor in RAW, target content cached in a CDF file |
| `em_pipeline.py` | The matching steps both functions use |
| `em_config.py` | Extraction pipeline configuration, and the summary written to the log |
| `em_constants.py`, `em_logger.py`, `em_pipeline_types.py`, `em_pipeline_optimizations.py` | Supporting code |

## Why the `em_` prefix

A function folder is deployed flat, so every module in it is a top-level module — and
other functions in this repo ship their own `pipeline.py`, `config.py` and `logger.py`.
Tools that analyse the repository as a whole, CodeQL and `pytest` among them, cannot tell
those apart and resolve an import to whichever one they meet first, which reports calls
against the wrong function's signature. The prefix makes each module name unique.

## Adding a module

Add the `.py` file here, named `em_*.py`, and run the sync script; it copies every module
in the folder.
