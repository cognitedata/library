# Discovery governance (declared root)

CDF Discovery writes generated **Space** and **Group** YAML here when you run **Build** in the UI or `python module.py build`.

| Path | Purpose |
|------|---------|
| `default.config.yaml` | Scope hierarchy, dimensions, spaces/groups templates (edited in Discovery UI) |
| `templates/` | Jinja templates for generated manifests |
| `spaces/` | Generated `*.Space.yaml` (gitignored) |
| `auth/` | Generated `*.Group.yaml` (gitignored) |
| `access_governance_state.json` | Last build manifest (gitignored) |

Override the declared root with `governance.declared_root` in `discovery.local.config.yaml` or `CDF_DISCOVERY_GOVERNANCE_ROOT`.
