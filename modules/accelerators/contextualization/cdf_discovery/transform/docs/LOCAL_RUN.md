# Local run

From `cdf_discovery/` (module root):

```bash
python -m local_runner.run --dry-run          # exercise DAG in-memory (no CDF credentials)
python -m local_runner.run --instance discovery_etl_default
python -m local_runner.run --predecessor-mode cohort   # RAW cohort handoff (deployed parity)
```

**Predecessor mode:** default `in_memory` passes rows via `_predecessor_rows` between tasks. Use `--predecessor-mode cohort` (or `ETL_LOCAL_PREDECESSOR_MODE=cohort`) so query tasks write cohort RAW tables and transform reads them like a deployed workflow.

**Parallelism:** Independent tasks in the same DAG layer run concurrently (default up to 4 workers). Dynamic fan-out child batches use the same limit.

```bash
python -m local_runner.run --max-workers 4 --instance discovery_etl_default
export KEA_LOCAL_MAX_WORKERS=2   # env default when --max-workers omitted
```

Pipeline YAML may set `parameters.local_max_workers`. Use `--max-workers 1` for strictly serial execution (debugging). Parallel branches that fan into a **merge** node with multiple in-memory predecessors should use `--predecessor-mode cohort` for full parity with deployed workflows.

Or via parent module:

```bash
python module.py transform build --pipeline discovery_etl_default
python module.py transform build --template aliasing_template
python module.py transform run --dry-run
python module.py transform run --instance discovery_etl_default
python module.py transform run --template aliasing_template
```

### Pipeline template build

Templates live under `workflow_definitions/templates/{template_id}.template.yaml`. **Build** compiles the canvas and writes CDF workflow manifests under `workflows/` as `etl_{template_id}.*` (Workflow, WorkflowVersion, WorkflowTrigger, trimmed config). The template file is updated with `compiled_workflow` and start-node workflow/trigger pairing; no file is written under `workflow_definitions/instances/`.

Each template defaults to workflow base `wf_all_etl_{template_id}` (unless the start node sets `workflow_base`), so multiple templates do not share the global `workflow` key from `default.config.yaml`.

From the Transform UI, open a template tab and use **Build** (saves dirty canvas first). **Run locally** on a pipeline or template compiles the current canvas, executes the DAG via the same runner, and shows per-task status in the toolbar.

The object tree **Transform** branch lists build scopes (`all`, `global`, …) as folders; each folder contains pipelines that have `workflows/{scope}/etl_{pipeline_id}.{scope}.config.yaml` from the last build.

Credentials: repository root `.env` (`COGNITE_API_KEY` or `COGNITE_*` / `IDP_*` OAuth vars).

Spark **spark_transform** nodes create a short-lived transformation (`tr_<task>__local__<run_id>`), run it, then delete it.
