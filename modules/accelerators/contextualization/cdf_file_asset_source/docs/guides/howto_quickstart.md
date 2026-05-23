# Quickstart — `cdf_file_asset_source`

Extract asset tags from diagram files and build a CDF asset hierarchy.

**Install:** [README — Install](../../README.md#install). **Spec:** [MODULE_SPECIFICATION.md](../MODULE_SPECIFICATION.md).

## Prerequisites

- Python 3.11+, Node.js 18+ (for UI only)
- `pip install -r requirements.txt` (see README)
- Repository-root `.env` for `module.py run`

## What the pipeline does

1. **Extract** — tags from CDF files using patterns
2. **Create** — hierarchy from `scope_hierarchy` and file lists
3. **Write** — upsert assets to data modeling

## Steps

### 1. Plan your scope

Identify:

- **Hierarchy levels** (e.g. site → unit → area → system)
- **Leaf systems** and which **file external ids** belong to each
- **Tag patterns** (e.g. `P-101`, `V-201`)

### 2. Start from an example

Copy a template or edit [default.config.yaml](../../default.config.yaml):

| File | Use case |
| ---- | -------- |
| `config.simple.example.yaml` | Minimal example |
| `config.template.manufacturing.yaml` | Site → plant → area → system |
| `config.template.oil_gas.yaml` | Site → facility → unit → system |
| `config.template.utilities.yaml` | Region → site → building → room → system |
| `config.template.pharmaceuticals.yaml` | Site → building → suite → system |

Production config lives in **`default.config.yaml`** under `file_asset_source.*` and `scope_hierarchy`.

### 3. Configure patterns and scope

Under `file_asset_source.extract.data.patterns`, set samples that match your tags. Under `scope_hierarchy.locations`, nest nodes with `id`, `name`, and leaf `files: []`.

Field reference: [specifications/config_schema.md](../specifications/config_schema.md).

### 4. Validate and sync workflow

```bash
export PYTHONPATH=.
python modules/accelerators/contextualization/cdf_file_asset_source/module.py validate
python modules/accelerators/contextualization/cdf_file_asset_source/module.py build
```

### 5. Run locally

```bash
python modules/accelerators/contextualization/cdf_file_asset_source/module.py run --step all
```

Use `--step extract`, `create`, or `write` for a single stage. Results: `local_run_results/*_pipeline_*.json`.

Test with a small `limit` in extract config before full runs.

### 6. Deploy to CDF

After `module.py build`, run Toolkit `cdf build` / deploy. See [workflows/README.md](../../workflows/README.md).

## Operator UI (optional)

```bash
python modules/accelerators/contextualization/cdf_file_asset_source/module.py ui
```

See [howto_config_ui.md](howto_config_ui.md).

## Troubleshooting

| Problem | Check |
| ------- | ----- |
| Files not matched to systems | File external ids on scope leaves match CDF (case-sensitive) |
| No assets extracted | Patterns vs actual tag format; diagram quality |
| Wrong hierarchy | `scope_hierarchy.levels` matches nesting depth |

More: [config_schema.md — Troubleshooting](../specifications/config_schema.md#troubleshooting).
