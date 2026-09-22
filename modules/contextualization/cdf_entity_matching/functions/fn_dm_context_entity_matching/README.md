# fn_dm_context_entity_matching — Entity matching (submit and collect)

## Overview

`fn_dm_context_entity_matching` contains the two stages of the asynchronous entity-matching
pipeline. Calls select a stage with `data.stage`; shared configuration, logging, pipeline
helpers, and staging code exist once.

Matching in CDF is a job on the platform, and waiting for it is what makes a large run
time out. **Submit** applies manual and rule based mappings, starts the predict job
without waiting, stages its matches, and queues the job. **Collect** works through that
queue oldest first, polls each job, merges finished results with the staged matches,
writes them, and removes the job. Anything still running is picked up by the next collect
run.

It reads the same extraction pipeline configuration as
`fn_dm_context_timeseries_entity_matching`, which stays in place and unchanged.

## Stages

- `submit`: manual and rule mappings, start the predict job, stage matches, queue the job.
- `collect`: poll queued jobs, merge results, write RAW and the data model, clear staging.

The workflow runs aliases update, then submit, then collect against one function external
ID.

## Input

```json
{
  "stage": "submit",
  "ExtractionPipelineExtId": "ep_ctx_timeseries_<location>_<source>_entity_matching",
  "logLevel": "INFO"
}
```

## What submit does

1. Reads manual mappings, rule mappings and targets from RAW and the data model.
2. Applies manual mappings, reads the entities that still need a match, applies rule
   based mappings.
3. Starts the predict job with `model.predict(...)` **without** reading its result.
4. Stages the manual and rule matches in a temporary CDF file `em_staged_matches_<jobId>.json`.
5. Appends a row `state_predict_job_<jobId>` to the state store table.
6. Reports success on the extraction pipeline run, noting that collect is pending.

The queue row is written **last**: collect only ever sees a job whose matches are already
staged.

### Reading the entities

Entities are read from the configured view and spaces with a `hasData` filter only.
Whether an entity is already linked is decided **after** the read, from the value of its
link property. The filter deliberately does not carry a `NOT exists(<link property>)`
clause — on the query endpoint that `instances.list` uses, [`exists` counts an empty
array as a
value](https://docs.cognite.com/cdf/dm/dm_concepts/dm_search#exists-filter-with-empty-array).

### Reading the targets

Targets are read with the DMS sync endpoint and kept in a cache:

- The cursor lives in the state store table (`state_target_sync_<key>`).
- The target content lives in the CDF file `em_target_cache_<key>.json` (overwritten on
  change, never deleted).
- A page that times out is read again 20% smaller, down to 100 instances.

This needs `filesAcl: READ, WRITE` in addition to the usual capabilities.

## What collect does

1. Reads every `state_predict_job_*` row and orders them oldest first.
2. For each job: poll (5s, then 15s, then 30s). On `Completed`, merge staged matches with
   model results, write good/bad tables and the data model, then clear staging and the
   queue row. On `Failed`, clear staging/queue and report failure without a traceback.
3. Stops when the queue is empty or **8 minutes** have passed.

When predictions regularly outlast a single run, schedule collect as well (every 5–15
minutes) so the queue keeps draining between workflow runs.

## Logging

Pass `"logLevel": "INFO"` or `"DEBUG"` in the function input (workflow already sets DEBUG).

`INFO` is what the run did; `DEBUG` adds timing, memory, and poll status. Each run
brackets the log with `===== SUBMIT =====` / `===== COLLECT =====` at INFO so the stage
is obvious for a shared function external ID in the CDF log viewer.

Set `dmUpdate: false` in the extraction pipeline config to skip writing matches to the
data model. It still processes **all** entities / finished jobs — it does not limit the
run to one.

## Code layout

`handler.py` dispatches on `data.stage`. Stage entrypoints live under `stages/`. Every
`em_*.py` module is edited in this folder (no sync copies).

## Tests

```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_entity_matching -q
```

Locally:

```bash
python handler.py submit
python handler.py collect
```

## Support

See the [module README](../../README.md) and Cognite Hub.
