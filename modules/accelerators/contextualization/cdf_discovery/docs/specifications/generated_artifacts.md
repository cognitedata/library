# Generated Toolkit artifacts — governance (`governance/`)

Normative config rules: [ACCELERATOR_CONFIG_CONVENTIONS.md](../../../ACCELERATOR_CONFIG_CONVENTIONS.md).

---

## Space YAML

Generated files use Toolkit **Space** resource shape:

- **`space:`** — external id (e.g. `inst_site_a_erp`)
- **`name`**, **`description`** — human-readable metadata
- Not: top-level-only `externalId` + legacy `access_governance` wrapper

Instance space naming pattern: `inst_{snake_case}` composed from scope and dimension context (see conventions doc).

---

## Group YAML

Generated files use Toolkit **Group** resource shape:

- **`name:`** — `gp_*` pattern (from `groups.name_template`, e.g. `gp_{{ data_type_id }}_{{ location_id }}_{{ access_type_id }}`)
- **`sourceId:`** — from `group_source_id` / config `source_id` / `source_ids` map (Toolkit sync can update `groups.global.source_ids` on build unless `--no-toolkit-sync`)
- **Capabilities** — explicit `actions` per capability (not implied defaults)

---

## Golden tests

Unit tests under `tests/unit/` assert:

- Render smoke for representative `default.config.yaml` slices
- Toolkit artifact shape (`space:` / `gp_*` names)
- Hierarchy expansion and dimension combinations
- Clean manifest behavior (`--clean`)

Run from module root:

```bash
pip install -r requirements.txt
pytest tests/unit/ -q
```

---

## Deploy

1. `python module.py build --force` (from `cdf_discovery` module root; config in `governance/`, artifacts in `spaces/` and `auth/`)
2. Include generated `spaces/` and `auth/` in a Toolkit project (`fusion.yaml` / `cdf.toml`)
3. `cdf build` then deploy **spaces** and **auth** resources
