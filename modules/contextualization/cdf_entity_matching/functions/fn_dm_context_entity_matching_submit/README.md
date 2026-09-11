# fn_dm_context_entity_matching_submit — Entity matching, submit

## Overview

Entity matching in CDF runs as a job on the platform. Waiting for that job inside the
function is what makes a large matching run time out. This function does everything that
happens before the wait — manual mappings, rule based mappings, and starting the predict
job — and then ends. [`fn_dm_context_entity_matching_collect`](../fn_dm_context_entity_matching_collect/README.md)
picks the job up once CDF is done with it.

It reads the same extraction pipeline configuration as the original
`fn_dm_context_timeseries_entity_matching` function, which stays in place and unchanged.

## What a run does

1. Reads manual mappings, rule mappings and targets from RAW and the data model.
2. Applies manual mappings, reads the entities that still need a match, applies rule
   based mappings.
3. Starts the predict job with `model.predict(...)` **without** reading its result.
4. Stages the manual and rule matches in the good RAW table under `pending:<jobId>:`.
5. Appends a row `state_predict_job_<jobId>` to the state store table, holding the job
   id, job token, status, creation time and staging prefix.
6. Reports success on the extraction pipeline run, noting that collect is pending.

The queue row is written **last**: collect only ever sees a job whose matches are already
staged.

### Several jobs at once

Each submit run appends a row of its own. A run never rewrites, and never deletes, the
row of a job that is still running, so repeated runs simply lengthen the queue and
collect works through it oldest first.

### `runAll`

A full re-run clears the good and bad tables, but leaves the staged matches of queued
predict jobs — and the queue itself — alone. Deleting those would leave collect with
results it cannot merge.

## Configuration

No parameters of its own: the extraction pipeline configuration is the same one the
existing entity matching function uses (`parameters` and `data.entityView` /
`data.targetView`). Invocation payload:

```json
{ "ExtractionPipelineExtId": "ep_ctx_timeseries_<location>_<source>_entity_matching", "logLevel": "INFO" }
```

## Logging

`INFO` is what the run did; `DEBUG` adds timing and memory. A run at `INFO` shows the
extraction pipeline it read, the effective configuration, the match counts, and the
**job id** it created.

## Code layout

`handler.py` is the entry point. Every other `.py` file is generated from
[`../_entity_matching_core`](../_entity_matching_core/README.md) — edit it there and run
`python scripts/sync_entity_matching_core.py`.

## Tests

```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_entity_matching_submit -q
```

## Support

See the [module README](../../README.md) and Cognite Hub.
