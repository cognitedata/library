# fn_dm_context_entity_matching_collect — Entity matching, collect

## Overview

Collects the predict jobs that
[`fn_dm_context_entity_matching_submit`](../fn_dm_context_entity_matching_submit/README.md)
started. A job that has finished has its matches merged with the manual and rule based
matches submit staged, written to RAW and to the data model, and is then taken off the
queue. A job that is still running when the run's time is up stays queued — that is a
normal outcome, not a failure.

## What a run does

1. Reads every `state_predict_job_*` row from the state store table and orders them
   oldest first (`createdAt`, then job id).
2. For each job, in that order:
   - Polls the job, waiting **5s, then 15s, then 30s** between polls.
   - On `Completed`: reads the result, reads the staged matches for the job, and runs the
     same selection step as before. Manual and rule matches are already in the list, so
     an entity they cover is never overwritten by a model match.
   - Writes good and bad matches to RAW, updates the data model when `dmUpdate` is set,
     then deletes the staging rows and the job's queue row.
   - On `Failed`: logs the error, clears the job's staging and queue row so it cannot
     block the queue, and reports the failure on the extraction pipeline run.
3. Stops when the queue is empty or **8 minutes** have passed since the run started,
   whichever comes first.

Rows are added to the result tables rather than replacing them, because other queued jobs
have their staged matches in the same table.

## Configuration

No parameters of its own; same extraction pipeline configuration and payload as submit:

```json
{ "ExtractionPipelineExtId": "ep_ctx_timeseries_<location>_<source>_entity_matching", "logLevel": "INFO" }
```

## Running it

The workflow runs it straight after submit. When predictions regularly outlast a single
run, schedule it as well (every 5–15 minutes) so the queue keeps draining between
workflow runs.

## Logging

`INFO` is what the run did; `DEBUG` adds poll status, timing and memory. A run at `INFO`
shows how many jobs were queued, the **job id** of each one it worked on, what it
collected, and a warning naming the job it left running when the 8 minutes were up.

## Code layout

`handler.py` is the entry point. Every other `.py` file is generated from
[`../_entity_matching_core`](../_entity_matching_core/README.md) — edit it there and run
`python scripts/sync_entity_matching_core.py`.

## Tests

```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_entity_matching_collect -q
```

## Support

See the [module README](../../README.md) and Cognite Hub.
