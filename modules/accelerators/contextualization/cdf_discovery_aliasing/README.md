# Key Discovery and Aliasing (`cdf_discovery_aliasing`)

Extract candidate keys and foreign-key references from entity metadata, generate aliases for matching, and persist results to CDF data modeling. Runs as Cognite Functions + Workflows (**v5**) or locally via `module.py run`.

**Full documentation:** [docs/README.md](docs/README.md) · [Module specification](docs/MODULE_SPECIFICATION.md)

## Install

From the **repository root** (directory containing `modules/`):

```bash
export PYTHONPATH=.
pip install -r modules/accelerators/contextualization/cdf_discovery_aliasing/requirements.txt
```

## Dependencies

| Category | Packages / tools | In `requirements.txt`? |
|----------|------------------|------------------------|
| Python runtime | 3.11+; `cognite-sdk`, `python-dotenv`, `PyYAML`, `pydantic`, `fastapi`, `uvicorn[standard]` | Yes |
| Operator UI | Node.js 18+, `npm`; see [ui/package.json](ui/package.json) | No |
| Credentials | Repo-root `.env` | No |
| CDF Functions deploy | Per-function `functions/fn_dm_*/requirements.txt` (e.g. `pandas`) | No |
| Toolkit deploy | Cognite Toolkit (`cdf build` / `cdf deploy`) | No |
| Dev / CI | `pytest`, optional Poetry | No |

## `module.py` CLI

Run from repo root with `PYTHONPATH=.`. `python module.py` with no subcommand prints help.

| Command | Purpose | Guide |
| ------- | ------- | ----- |
| `run` | Local pipeline; writes `local_run_results/` | [howto_quickstart.md](docs/guides/howto_quickstart.md) |
| `build` | Generate scoped Workflow/Trigger YAML | [howto_scoped_deployment.md](docs/guides/howto_scoped_deployment.md) |
| `ui` | Operator UI (FastAPI + Vite) | [howto_config_ui.md](docs/guides/howto_config_ui.md) |
| `copy-workflow-config` | Copy trigger configuration between scopes | [config/README.md](config/README.md) |
| `promote-local-templates` | Promote local template to `workflow.template.config.yaml` | [workflow_template/README.md](workflow_template/README.md) |
| `deploy-scope` | SDK deploy Workflow/Version/Trigger | [howto_scoped_deployment.md](docs/guides/howto_scoped_deployment.md) |
| `cdf-workflow-run` | Start deployed workflow and poll | [howto_scoped_deployment.md](docs/guides/howto_scoped_deployment.md) |
| `raw-purge-baseline` / `raw-purge-truncate` | RAW table maintenance | [config/README.md](config/README.md) |

Common `run` flags: `--dry-run`, `--scope default`, `--config-path`, `--all`, `--clean-state`, `--verbose`. Full list: `python module.py run --help`.

## Operator UI

```bash
export PYTHONPATH=.
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py ui
```

| Service | Default URL |
|---------|-------------|
| FastAPI | http://127.0.0.1:8765/ |
| Vite UI | http://127.0.0.1:5173/ |

Flags: `--no-browser`, `--no-reload`, `--api-port`, `--vite-port`. Env: `CDF_KEY_EXTRACTION_ALIASING_ROOT`.

**Security:** no API authentication — localhost only.

Other modules’ ports: [Accelerators README](../../README.md#dev-port-matrix).

## Capabilities

- **Extraction:** `regex_handler` — candidate keys, foreign keys, document references ([key extraction spec](docs/specifications/1.%20key_extraction.md))
- **Aliasing:** character substitution, regex, semantic expansion, `alias_mapping_table`, … ([aliasing spec](docs/specifications/2.%20aliasing.md))
- **Incremental processing:** Key Discovery FDM state + RAW cohort — [MODULE_SPECIFICATION.md §3.4](docs/MODULE_SPECIFICATION.md), [configuration guide](docs/guides/configuration_guide.md)
- **Custom handlers:** [howto_custom_handlers.md](docs/guides/howto_custom_handlers.md)

Default scope: [`workflow.local.config.yaml`](workflow.local.config.yaml). Multi-site: `scope_hierarchy` in [`default.config.yaml`](default.config.yaml) → `module.py build`.

## Configuration entry points

| Layer | File |
| ----- | ---- |
| Local scope | `workflow.local.config.yaml` |
| Toolkit template | `workflow_template/workflow.template.config.yaml` |
| Module defaults + hierarchy | `default.config.yaml` |
| Examples | [config/examples/](config/examples/) |

Authoring: [howto_config_yaml.md](docs/guides/howto_config_yaml.md), [howto_config_ui.md](docs/guides/howto_config_ui.md), [configuration_guide.md](docs/guides/configuration_guide.md).

## Roadmap

- [x] Key Discovery FDM state; RAW inverted index
- [ ] DM projection for inverted index
- [x] Per-entity-type default scope rules
- [ ] Broader non-ISA tag testing

## Repository layout

See [docs/README.md](docs/README.md) and subsystem READMEs: [config/](config/README.md), [functions/](functions/README.md), [workflows/](workflows/README.md), [workflow_template/](workflow_template/README.md).

## Testing

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_discovery_aliasing/tests -q
```

See [tests/README.md](tests/README.md).

## Troubleshooting

[docs/troubleshooting/common_issues.md](docs/troubleshooting/common_issues.md)

## Documentation

| Document | Contents |
| -------- | -------- |
| [docs/MODULE_SPECIFICATION.md](docs/MODULE_SPECIFICATION.md) | Canonical spec |
| [ACCELERATOR_CONFIG_CONVENTIONS.md](../../ACCELERATOR_CONFIG_CONVENTIONS.md) | Shared config and naming rules |
| [configuration_guide — Default CDM scope](docs/guides/configuration_guide.md#default-cdm-scope) | Default scope narrative |

## License

Apache License 2.0 — see [LICENSE](../../../../LICENSE) at repository root.
