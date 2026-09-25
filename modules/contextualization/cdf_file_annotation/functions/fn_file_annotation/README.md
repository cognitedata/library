# Unified file annotation function

## Overview

`fn_file_annotation` contains the four stages of the file-annotation pipeline. Calls
select a stage with `data.stage`; shared configuration, logging, data-model access, and
data structures exist once.

## Stages

- `prepare`: create annotation-state nodes for eligible files.
- `launch`: build scoped entity caches and submit Diagram Detect jobs.
- `finalize`: collect completed jobs and write annotations and RAW reports.
- `promote`: resolve pattern-mode annotations to file or target entities.

The workflow runs prepare, launch, finalize, promote, then the file-to-asset
transformation as one dependency chain.

## Input

```json
{
  "stage": "prepare",
  "ExtractionPipelineExtId": "ep_file_annotation",
  "logLevel": "INFO"
}
```

## Logging (`logLevel`)

Pass `"logLevel": "INFO"` or `"DEBUG"` in the function input (workflow already sets DEBUG).

| Level | What you get |
|-------|----------------|
| **WARNING** | Warnings and errors only, plus the peak memory of the stage |
| **INFO** | Launch input counts (assets, files, missing aliases, pattern sample count, structural flag); detect job `statusCount` / failed items / annotation hit totals; apply messages |
| **DEBUG** | Per-entity aliases (first 40), pattern sample strings, detect per-file texts/errors, full entities JSON submitted to regular detect |

To measure memory, set `"logLevel": "WARNING"` on the workflow task. Each call then ends with
`Peak memory for stage '<stage>': <n> MiB`. It is measured with `tracemalloc`, so it counts the Python allocations
the stage made and excludes the interpreter and imported modules. Compare it between stages and runs, not against
the function's memory limit. Tracing slows the run and adds memory of its own, so INFO, DEBUG and ERROR runs skip it.

If you set DEBUG but see no `[DEBUG]` lines, redeploy this function — older builds had almost no DEBUG statements.

## Development

Run the tests from the repository root:

```bash
uv run pytest modules/contextualization/cdf_file_annotation/functions/fn_file_annotation -q
```

Local debug configurations in `local_setup/launch.json` point to the same handler and
pass the stage as the first argument.

## Consolidation decisions

The four former `DataStructures.py` copies had three meaningful differences. The merged
copy keeps `CacheMarker` for promote, the launch retry guard that prevents a page range
from starting after its end, and prepare's safe protected-property handling and
`CogniteDescribable.tags` helpers. Annotation-state audit fields now identify the unified
`fn_file_annotation` function.

`ConfigService.py`, `LoggerService.py`, `PipelineService.py`, and
`DataModelService.py` are shared once. The launch and promote cache modules are named
`EntityCacheService.py` and `PromoteCacheService.py` to make their different roles
explicit.
