# Discovery governance (declared config)

Authoring for scoped **Space** and **Group** Toolkit YAML: scope hierarchy, dimensions, and Jinja templates.

| Path | Purpose |
|------|---------|
| `default.config.yaml` | Scope hierarchy, dimensions, spaces/groups build settings (edited in Discovery UI) |
| `templates/` | Jinja templates for generated manifests |
| `access_governance_state.json` | Last build manifest (gitignored; lists paths under module `spaces/` and `auth/`) |

**Generated deploy artifacts** (from `python module.py build` or UI **Build**) are written to the **module root**:

| Path | Purpose |
|------|---------|
| `../spaces/` | Generated `*.Space.yaml` (gitignored) |
| `../auth/` | Generated `*.Group.yaml` (gitignored) |

Override the declared config root with `governance.declared_root` in `discovery.local.config.yaml` or `CDF_DISCOVERY_GOVERNANCE_ROOT`.
