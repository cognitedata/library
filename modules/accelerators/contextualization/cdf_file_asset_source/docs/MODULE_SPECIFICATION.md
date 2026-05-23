# Module specification — `cdf_file_asset_source`

Canonical description of the **Create asset hierarchy from files** accelerator: business intent, pipeline, configuration, deployment, and CLI. Field-level YAML: [specifications/config_schema.md](specifications/config_schema.md). Pipeline I/O: [specifications/pipeline_api.md](specifications/pipeline_api.md).

---

## 1. Purpose and scope

### 1.1 Business intent

Engineering diagrams (PDF, DWG, etc.) contain equipment tags that must become structured **asset hierarchies** in CDF data modeling. This module:

1. **Extracts** asset tags from files using configurable patterns and diagram APIs
2. **Creates** a hierarchy aligned to organizational scope (sites, units, areas, systems) and file placement
3. **Writes** `CogniteAsset` (or configured view) instances to an instance space

### 1.2 Technical boundaries

| In scope | Out of scope |
| -------- | ------------ |
| Three-stage CDF Functions + Workflow | General document management outside file→asset use case |
| `default.config.yaml` as Toolkit + local source of truth | Real-time diagram revision sync (batch/workflow driven) |
| Local `module.py` runner and operator UI | Hosted multi-tenant operator product |
| `scope_hierarchy` for multi-site Toolkit repeats (when used) | Cross-module runtime imports |

**Module type:** Toolkit deployable (config family **A** / **B** in [ACCELERATOR_CONFIG_CONVENTIONS.md](../../ACCELERATOR_CONFIG_CONVENTIONS.md)).

---

## 2. Actors

| Actor | Role |
| ----- | ---- |
| **Config author** | Edits `default.config.yaml`, patterns, and `scope_hierarchy` |
| **Operator** | Runs local pipeline or CDF workflow; uses operator UI |
| **Toolkit deployer** | `module.py build` → `cdf build` / deploy |
| **Downstream apps** | Consume assets in configured instance space |

---

## 3. System architecture

```
Workflow: create_asset_hierarchy_from_files
    │
    ├─► fn_dm_extract_assets_by_pattern  ──► RAW (results, state)
    ├─► fn_dm_create_asset_hierarchy     ──► RAW (assets) + optional YAML
    └─► fn_dm_write_asset_hierarchy      ──► CDF Data Modeling (CogniteAsset)
```

**Config sections** in `default.config.yaml`:

| Key | Stage |
| --- | ----- |
| `file_asset_source.extract` | Pattern extraction |
| `file_asset_source.create` | Hierarchy generation |
| `file_asset_source.write` | DM upsert |
| `scope_hierarchy` | Location tree and per-leaf `files` lists |
| Top-level Toolkit vars | `function_version`, `workflow`, `workflow_schedule`, OAuth placeholders |

---

## 4. Data flow

1. **Extract** — Query CDF files → diagram/text processing → tag matches → RAW (`db_file_asset_extract`, state table)
2. **Create** — Read extraction RAW → map files to scope leaves → classify tags (optional ISA patterns) → hierarchy in RAW / `results/asset_hierarchy.yaml`
3. **Write** — Read hierarchy → batch apply to instance space (`inst_*` by convention)

See [specifications/pipeline_api.md](specifications/pipeline_api.md) for table names and function contracts.

---

## 5. Configuration (summary)

- **Scope tree:** `scope_hierarchy.levels` + nested `locations`; leaves include `files: [externalId, …]`
- **Patterns:** `file_asset_source.extract.data.patterns` — `category`, `sample` / `samples`, optional ISA fields
- **Industry templates:** `config.template.*.yaml` at module root (manufacturing, oil_gas, utilities, pharmaceuticals)
- **Simple example:** `config.simple.example.yaml`

Authoring detail: [specifications/config_schema.md](specifications/config_schema.md).

---

## 6. CLI surface

| Command | Purpose |
| ------- | ------- |
| `ui` | FastAPI + Vite operator UI |
| `validate` | Validate `default.config.yaml`; optional compliance gates |
| `build` | Sync workflow trigger `input.configuration` from config |
| `run` | Local pipeline (`--step extract\|create\|write\|all`) |

Credentials: repository-root `.env`. Env: `CDF_FILE_ASSET_SOURCE_ROOT`.

---

## 7. Deployment

1. Edit `default.config.yaml`
2. `python module.py validate`
3. `python module.py build` (or `build --check` in CI)
4. Toolkit `cdf build` / deploy including `functions/` and `workflows/`

Schedule: `workflow_schedule` cron on WorkflowTrigger. See [workflows/README.md](../workflows/README.md).

---

## 8. Operator UI

Local config editor: scope/pattern forms, validate, build, run. **No authentication** — localhost only. Launch: [README — Operator UI](../README.md#operator-ui). Guide: [guides/howto_config_ui.md](guides/howto_config_ui.md).

---

## 9. Security and NFRs

- Operator API: no auth; bind `127.0.0.1`
- CDF access via OAuth env vars in `.env` / Toolkit placeholders
- Batch processing: `batch_size`, `limit`, `max_attempts` per step
- Target: high throughput for large file sets; tune `batch_size` and diagram chunk settings

---

## 10. Testing

```bash
export PYTHONPATH=.
pytest modules/accelerators/contextualization/cdf_file_asset_source/tests/unit/ -q
```

See [tests/README.md](../tests/README.md).

---

## 11. Related documents

| Document | Contents |
| -------- | -------- |
| [guides/howto_quickstart.md](guides/howto_quickstart.md) | First-time setup |
| [ACCELERATOR_CONFIG_CONVENTIONS.md](../../ACCELERATOR_CONFIG_CONVENTIONS.md) | Shared naming and `scope_hierarchy` |
| [patterns/validation_rules_examples.md](../patterns/validation_rules_examples.md) | Validation rule examples |
