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
