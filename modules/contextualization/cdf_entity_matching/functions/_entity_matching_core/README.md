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
| `submit.py` | Function 1: manual and rule mappings, staging, starting the predict job |
| `collect.py` | Function 2: polling the queue, merging results, writing them |
| `job_state.py` | The predict job queue in the RAW state store |
| `staging.py` | Manual and rule matches parked in RAW while a predict job runs |
| `pipeline.py` | The matching steps both functions use |
| `config.py` | Extraction pipeline configuration, and the summary written to the log |
| `constants.py`, `logger.py`, `pipeline_types.py`, `pipeline_optimizations.py` | Supporting code |

## Adding a module

Add the `.py` file here and run the sync script; it copies every module in the folder.
