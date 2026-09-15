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

### Reading the targets

Paging every target instance out of the data model is the slowest part of step 1, and on
a large model it is what returns `408 Request timed out`. Targets are therefore read with
the **sync endpoint** and kept in a cache:

- The cursor lives in the state store table, in a row `state_target_sync_<key>`, together
  with the external ID of the cache file, the page size that last worked and the target
  count. `<key>` is a fingerprint of the target configuration — view, spaces, search
  property and filter — so functions reading different targets never share a cursor.
- The target content lives in the CDF file `em_target_cache_<key>.json`. When sync reports
  changes, that **same** file is overwritten (`overwrite=True`) and the RAW row is
  upserted. The file and the row are **not** deleted. A deleted target instance is dropped
  from the in-memory list, then the whole JSON is written back.
- A run where sync reports no changes reads the content from that file and writes only
  the new cursor back.
- A page that times out is read again 20% smaller, down to 100 instances. Timeouts count
  toward the same retry limit as other failures (`TARGET_SYNC_MAX_RETRIES` = 4). The size
  that worked is stored for the next run.
- A cursor the API rejects, or a cache file that is gone, falls back to reading every
  target again — still into the same file and row.
- If the target configuration changes, `<key>` changes, so a **new** file and RAW row are
  created. The previous cache is left in CDF unused; submit does not clean it up.

The cache is an optimisation, so no failure to reach it fails the run. A transient error
on the file is retried twice; after that a download falls back to reading the targets
from the data model, and a failed upload leaves the run with the targets it has already
read. The cursor is stored **only** once the content it describes has been written, so
the cursor and the cache file always describe the same targets — a run that could not
write the file syncs the same changes again next time rather than merging them onto an
older copy. A cursor that cannot be written back is harmless for the same reason.

This needs `filesAcl: READ, WRITE` in addition to the usual capabilities. Only the name
and the search property are selected, so a page stays small; when `filterProperty` is
configured, an index on that property in the container behind the target view is what
keeps the filter itself cheap.

Sync reports instances that were created, changed or deleted — but not ones that stopped
matching the configured filter, so the cache can hold a target the filter no longer
selects. Run with `runAll` to read every target again and rebuild it.

### `runAll`

A full re-run clears the good and bad tables and reads every target from the data model
again, but leaves the staged matches of queued predict jobs — and the queue itself —
alone. Deleting those would leave collect with results it cannot merge.

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

`debug: true` in the extraction pipeline config enables DEBUG logging and skips writing
matches to the data model. It still processes **all** entities — it does not limit the
run to one.

## Code layout

`handler.py` is the entry point. Every `em_*.py` file is generated from
[`../_entity_matching_core`](../_entity_matching_core/README.md) — edit it there and run
`python scripts/sync_entity_matching_core.py`.

## Tests

```bash
uv run pytest modules/contextualization/cdf_entity_matching/functions/fn_dm_context_entity_matching_submit -q
```

## Support

See the [module README](../../README.md) and Cognite Hub.
