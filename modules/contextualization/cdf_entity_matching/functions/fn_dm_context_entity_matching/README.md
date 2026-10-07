# fn_dm_context_entity_matching — Entity matching (submit and collect)

## Overview

`fn_dm_context_entity_matching` contains the two stages of the asynchronous entity-matching
pipeline. Calls select a stage with `data.stage`; shared configuration, logging, pipeline
helpers, and staging code exist once.

Matching in CDF is a job on the platform, and waiting for it is what makes a large run
time out. **Submit** applies manual and rule based mappings, starts the predict job
without waiting, stages its matches, and queues the job. **Collect** polls that queue in
parallel (one worker per job, at most 10) every 30s for up to 7 minutes, merges finished
results with the staged matches, writes them, and removes the job. Anything still running
is picked up by the next collect run.

It reads the `ep_ctx_entity_matching` extraction pipeline configuration.

Time series are read a page at a time with only the properties matching uses, and each
page is retried on transient errors. A failed read of the time series, the manual or rule
mappings, or a staged match file fails the call rather than matching on partial input. A
job whose staged matches cannot be read stays queued for the next collect run.

## Stages

- `submit`: manual and rule mappings, start the predict job, stage matches, queue the job.
- `collect`: poll queued jobs, merge results, write RAW and the data model, clear staging.

The workflow runs aliases update, then submit, then collect against one function external
ID.

## Input

```json
{
  "stage": "submit",
  "ExtractionPipelineExtId": "ep_ctx_entity_matching",
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

### What the counts mean (entities vs pairs)

Two different units show up in INFO logs and are easy to confuse:

- **Entities** — how many source instances got at least one manual or rule match (for
  example “across 350 entities”). That is also what the extraction-pipeline run reports
  as matched entities.
- **Entity–target pairs** — one row per link, so one entity that matches many targets
  contributes many rows. Staging logs this as
  `Staged N entity-target pair(s) from manual/rule matching across M entities`.

Those staged pairs are **finished matches** from the manual and rule steps — not
candidates still waiting for the ML predict job. Collect merges them with the model’s
results later; for any entity already covered by a staged pair, the model result does
not overwrite it.

A pair count far above the entity count usually means a rule key resolved to many
targets (check `entity_rule_keys` / `asset_rule_keys` on the good RAW table and tighten
`EntityRegExp` / `AssetRegExp` if needed). The same `(entity, target)` pair is only kept
once when matches are merged; `MAX_LINKS_PER_ENTITY` (1000) caps how many links a single
entity can receive when writing to the data model.

### How aliases are used

Aliases are prepared by the workflow’s aliases-update step. Submit/collect then read
`entityViewSearchProperty` / `targetViewSearchProperty` (often `aliases`):

- On **entities**, only the longest alias is used as the match string.
- On **targets**, every alias is kept so alternate forms still match.

Rule-based matching still applies its regexes to the instance `name`, not to aliases.
See the [module README](../../README.md#targetviewsearchproperty-and-entityviewsearchproperty)
for the full property behaviour.

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

This needs `filesAcl: READ, WRITE` on `ds_entity_matching`. Collect also checks a digest
stored in the RAW state row before applying staged matches or (on submit) using the
target cache.

### Primary and secondary scope

Set `primaryScopeProperty` (and optionally `secondaryScopeProperty`) in the extraction
pipeline parameters. Both are read from the entity view and the target view, so they
must exist on both. Both empty (the default) is unscoped matching: one predict job of
every entity against every target.

When scoping is on, submit groups entities by `(primary, secondary)` and queues **one
predict job (and one staging file) per group** that has work to do. Rule matching uses
the same groups. Manual mappings stay global and are staged with the first job; the
model fitted for that job is reused by the later ones. The scope properties and `tags`
are added to the target sync read, which gives a scoped configuration a target cache of
its own. Rows written to `contextualization_good` / `contextualization_bad` then include
`scope_primary` and `scope_secondary` for the entity.

A missing property value is read as empty. That yields two different empty cases:

- **No scope** — primary and secondary both empty. Those entities get a job against
  **all** targets. Submit logs
  `Entities without scope (primary='', secondary='') - N is tried matched against all Targets`.
- **Partial scope** — primary set, secondary empty (or the other way around if only
  secondary were populated). That is a scope of its own: the entity only meets targets
  with the same pair of values, plus `ScopeWideDetect` targets of the same primary.

Other rules:

- An entity with a non-empty scope is only matched to targets with the same primary,
  and the same secondary when that value is set.
- A target whose `tags` include `ScopeWideDetect` is a candidate in every **secondary**
  scope of its own primary — never in another primary.
- A non-empty scope with no matching targets is skipped. Submit logs
  `No assets in scope (primary=…, secondary=…) - N source record(s) not matched`.

## What collect does

1. Reads every `state_predict_job_*` row and orders them oldest first.
2. Polls queued jobs in parallel — one worker per job, at most **10** at a time — every
   **30s**. When a worker finishes and time remains, the next queued job is started.
   On `Completed`, merge staged matches with model results, write good/bad tables and the
   data model, then clear staging and the queue row. On `Failed`, clear staging/queue and
   report failure without a traceback.
3. Stops when the queue is empty or **7 minutes** have passed.

When predictions regularly outlast a single run, schedule collect as well (every 5–15
minutes) so the queue keeps draining between workflow runs.

## Logging

Pass `"logLevel": "INFO"` or `"DEBUG"` in the function input (workflow already sets DEBUG).

`INFO` is what the run did; `DEBUG` adds step timings and poll status. Each run
brackets the log with `===== SUBMIT =====` / `===== COLLECT =====` at INFO so the stage
is obvious for a shared function external ID in the CDF log viewer.

On submit, expect lines such as
`Staged N entity-target pair(s) from manual/rule matching across M entities` — see
[What the counts mean](#what-the-counts-mean-entities-vs-pairs). Collect logs the same
pair/entity counts when it reads the staging file back.

Each predict job logs how many unique entities it still has to match after manual/rule
mapping (`Predict job submitted - jobId: …, N entities to match`). Those N values across
jobs should add up to input entities minus the manual/rule total. Collect then logs how
many result rows the matching API returned versus how many source records were submitted.

Set `dmUpdate: false` in the extraction pipeline config to skip writing matches to the
data model. It still processes **all** entities / finished jobs — it does not limit the
run to one.

## Code layout

`handler.py` dispatches on `data.stage`. Stage entrypoints live under `stages/`. Every
`em_*.py` module is edited in this folder (no sync copies).

## Tests

```bash
uv run pytest modules/contextualization/cdf_entity_matching/tests/fn_dm_context_entity_matching -q
```

Locally:

```bash
python handler.py submit
python handler.py collect
```

## Support

See the [module README](../../README.md) and Cognite Hub.
