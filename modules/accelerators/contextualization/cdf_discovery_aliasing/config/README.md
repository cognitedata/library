# Configuration layout

**Documentation index:** [docs/README.md](../docs/README.md) (maps specs, guides, examples, and workflows). **Authoring walkthroughs:** [How to build configuration with YAML](../docs/guides/howto_config_yaml.md), [How to build configuration with the UI](../docs/guides/howto_config_ui.md). **Scoped build and deploy walkthrough:** [Scoped deployment how-to](../docs/guides/howto_scoped_deployment.md).

Configs for this module are the **authoring source** for **`module.py`** / **`local_runner`** and for **CDF workflow** payloads. CDF functions receive **`workflow.input.configuration`** (trimmed v1 scope, including **`canvas`**) from schedule triggers built by **`scripts/build_scopes.py`** — there is **no** **`workflow.input.compiled_workflow`**; per-step IR is inlined into generated **WorkflowVersion** task **`data`**. Scope YAML is **not** uploaded as Cognite File for this pipeline.

## Directories

| Path | Purpose |
|------|---------|
| **`tag_patterns.yaml`** | **Shared** tag + document regex library for **aliasing** (`tag_pattern_library`) and **authoring alignment** for key-extraction rules. Loaded via [`tag_patterns_paths.py`](tag_patterns_paths.py). |
| **`examples/`** | **`key_extraction/`** and **`aliasing/`** example scope YAML (`*.key_extraction_aliasing.yaml`); **`reference/`** for complete YAML. Not used automatically by the local runner or CDF functions. |

**Deployable Cognite Functions** (`fn_dm_*`) are documented in [`../functions/README.md`](../functions/README.md) and registered in [`../functions/functions.Function.yaml`](../functions/functions.Function.yaml).

**Local default v1 scope file:** [`../workflow.local.config.yaml`](../workflow.local.config.yaml) at the **module root** (used when `--scope default` and no `--config-path`).

**Local incremental (workflow parity):** When `key_extraction.config.parameters.incremental_change_processing` is true, `local_runner/run.py` sets top-level `source_views` on the merged v1 scope dict and passes **`configuration`** plus **`instance_space`** on each step’s task payload, matching workflow **v5** task inputs (see [`local_runner/workflow_payload.py`](../local_runner/workflow_payload.py)). Configure **`workflow_scope`** (leaf scope id; local default `workflow_local` in [`workflow.local.config.yaml`](../workflow.local.config.yaml)) so parallel deployed workflows do not share watermarks or hashes. Use Key Discovery parameters (`key_discovery_instance_space`, `key_discovery_schema_space`, …) when FDM views are deployed; otherwise cross-run state is stored on stable RAW table **`{raw_table_key}__incremental`** (e.g. `discovery_state__incremental`), not on per-run cohort tables deleted by cleanup.

**Incremental hash skip vs performance:** When `incremental_skip_unchanged_source_inputs` is true (default), `fn_dm_view_query` reads prior hashes from Key Discovery FDM or the stable incremental RAW table (filtered by **`workflow_scope`**). On **`module.py run`**, [`local_runner/kahn_workflow_executor.py`](../local_runner/kahn_workflow_executor.py) injects **`discovery_raw_hash_index_cache`** so **one** index build per `(raw_db, incremental_table, workflow_scope)` is shared by parallel view-query tasks in that run. Deployed CDF functions scan independently per invocation unless Key Discovery FDM is active. Edit parameters under **Configure → Key Discovery** in the UI or `key_extraction.config.parameters` in YAML.

**View query listing (`instances.list`, not search):** Bulk discovery uses **`instances.list`** with cursor pagination ([`list_all_instances`](../functions/cdf_fn_common/incremental_scope.py)), not **`instances.search`**. Search is full-text (max **1000** results, relevance order) and does not replace enumerating a view for cohort export. Incremental view-query tasks pass **`sort=node.lastUpdatedTime` ascending** alongside the watermark filter so pagination aligns with [DM `/list` performance guidance](https://docs.cognite.com/cdf/dm/dm_guides/dm_performance_considerations). Ensure **cursorable BTree indexes** on filtered view properties and on change-time fields in your containers. Handler summaries include **`list_page_count`**, **`list_duration_sec`**, and **`list_sort`**. Operator tools: [`scripts/audit_view_query_dm_indexes.py`](../scripts/audit_view_query_dm_indexes.py), [`scripts/benchmark_view_query_list.py`](../scripts/benchmark_view_query_list.py), [`scripts/benchmark_instances_sync_vs_list.py`](../scripts/benchmark_instances_sync_vs_list.py) (list vs sync spike; requires CDF credentials).

**Cohort row index (transform / validate / filter):** Transform cumulative merge and downstream predecessor reads use an **in-memory** `row_key → properties` map built by **one chunked RAW scan per node cohort table** per function invocation ([`build_cohort_row_index`](../functions/cdf_fn_common/cohort_storage.py) in `cdf_fn_common`) — this is **not** a CDF RAW database index. Handlers fall back to `raw.rows.retrieve` only when no index is supplied (tests / legacy). On **`module.py run`**, **`discovery_cohort_row_index_cache`** (same pattern as the hash cache) reuses each table’s map across tasks in one process. **Deployed** workflows get per-invocation indexing automatically after redeploying `fn_dm_*` functions; cross-task reuse in production still requires one scan per Cognite Function call unless you add a future persisted side store.

**RAW run report sampling:** Optional JSON attachment after the DAG caps how many matching rows are returned per table (`--raw-results-rows`) and how many **RAW rows are examined** per table when filtering (e.g. by `RUN_ID`): set env **`KEA_RAW_RESULTS_MAX_RAW_ROWS_SCANNED`** (default `100000`) or **`module.py run --raw-results-max-rows-scanned K`**. When the walk stops early, `raw_results.tables[].raw_scan_truncated` is true.

**Trigger-embedded template:** [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml) — merged, patched per hierarchy leaf, then **trimmed** into each generated trigger’s **`input.configuration`** (no separate per-leaf **`*.canvas.yaml`** from **`build_scopes`**).

Python package code in this folder (`configuration_manager.py`, etc.) lives beside these data directories.

## Scope hierarchy builder

**[`default.config.yaml`](../default.config.yaml)** (module root) includes top-level **`aliasing_scope_hierarchy`**, using the same hierarchy shape as **`cdf_access_control`** `dimensions.<id>` with **`type: hierarchy`** (`order`, `type`, **`levels`**, **`locations`**). Prefer semantic tier labels (`site`, `unit`, …) like **`dimensions.scope_tree`**, not generic `level_1` names. **`levels`** (tier labels) and **`locations`** (root node list; each node nests children under **`locations`**). **Leaves may be at any depth** — you do not need a full path for every declared level name; `levels` labels the first segments only, and tiers beyond that list get synthetic names (`level_3`, …) in build metadata. Run **`python module.py build`** or **`scripts/build_scopes.py`** to validate the tree, **create** missing scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** under **`workflows/<suffix>/`**, and refresh **`workflow_template/workflow.execution.graph.yaml`** from IR every run (use **`--force`** to overwrite **existing** scoped flow artifacts; override with **`--scope-document`** / **`--workflow-trigger-template`**). Use **`module.py build --clean`** to remove generated workflow YAML under **`workflows/`** in bulk (confirmation or **`--yes`**; **`--dry-run --clean`** to preview; no automatic rebuild—run **`module.py build`** again afterward). Each leaf trigger embeds **`input.configuration`** from **`workflow_template/workflow.template.config.yaml`**. The **`workflow_triggers`** builder prepends a **node `space`** filter on each top-level `source_views` row unless one exists; filter value is the leaf **`instance_space`** when set, else the toolkit placeholder **`{{instance_space}}`** (substituted at deploy from **`default.config.yaml`**). Use **`--check-workflow-triggers`** to fail CI if any trigger required by the current hierarchy is missing or out of date (extra **`key_extraction_aliasing.*.WorkflowTrigger.yaml`** files on disk are ignored; a normal **`module.py build`** does not delete them). **`--workflow-trigger-template`** overrides the trigger shell. Use `--dry-run`, `--list-builders`, `--only workflow_triggers`, and `--hierarchy` as needed.

## Scope YAML (v1)

- **`source_views`**: required non-empty list at the **document root** (sibling of `key_extraction`) — which DM views to query.
- **`key_extraction`**: `externalId` + `config` with `parameters` and `data` (`validation`, `extraction_rules`; no `source_views` here in v1 scope YAML).
- **`aliasing`** (optional): same for aliasing. If omitted, or `aliasing_rules` is missing or empty, the local loader uses **identity passthrough** (zero rules).
- **`extraction_rules`**: If omitted or `[]`, the loader injects **passthrough-on-`name`** rules, one per distinct `entity_type` in top-level `source_views`.

Optional: `schemaVersion: 1`, `scope: { name, description }` (informational; **`scope`** is injected into trigger payloads by `build_scopes`).

## Local CLI (`module.py run`)

Pipeline flags apply to **`python module.py run`** (bare **`module.py`** prints help).

| Flag | Behavior |
|------|----------|
| **`--config-path <file>`** | Load that file as a v1 scope document (highest precedence). |
| **`--scope <name>`** | Only **`default`** (or omitted) loads **`workflow.local.config.yaml`** at module root; other names require **`--config-path`**. |
| **`--raw-results-max-rows-scanned K`** | Max RAW rows read per table when sampling for `discovery_run.json` `raw_table_samples` (`0` = use env **`KEA_RAW_RESULTS_MAX_RAW_ROWS_SCANNED`** or default `100000`). See `raw_table_samples.tables[].raw_scan_truncated`. |

## Historical RAW purge (operator CLI)

**`python module.py raw-purge-truncate`** deletes every configured discovery cohort RAW table via Cognite **`raw.tables.delete`** (same tables **`collect_discovery_raw_tables`** would list from your scope and optional merged **`compiled_workflow`**). This is **destructive**: it removes **incremental watermarks** (`scope_wm_*`), **cohort / hash** rows, and **inverted index** tables when they share those sink definitions — the next pipeline run behaves like a **cold start** until watermarks are rebuilt. Always use **`--dry-run`** first, then **`--yes`** to execute. See **`module.py --help`** for flags (`--config-path`, `--scope`, optional **`--tables db:table`** overrides).

## Workflows

Workflow **v5** (generated **`workflows/.../key_extraction_aliasing*.WorkflowVersion.yaml`**) passes **`${workflow.input.configuration}`** on each function task and **inlines** IR-derived fields on task **`data`** (`task_id`, payloads, persistence, rule name lists, etc.). RAW table keys remain in **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`configuration`**. See [`workflows/README.md`](../workflows/README.md).

## Reference docs

- Examples and narrative: [`examples/README.md`](examples/README.md)