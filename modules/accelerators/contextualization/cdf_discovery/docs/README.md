# Documentation map — `cdf_discovery`

Paths are relative to `modules/accelerators/contextualization/cdf_discovery/` unless noted.

## Start here

| Document | Audience | Contents |
| -------- | -------- | -------- |
| [Module specification](MODULE_SPECIFICATION.md) | Everyone | Purpose, boundaries, capabilities, config, API, security |
| [Module README](../README.md) | Everyone | Install, dependencies, `module.py ui`, links |
| [Operator UI guide](guides/howto_operator_ui.md) | Operators | Navigation, tabs, SQL editor, workspace |

## ETL and workflows

| Document | Contents |
| -------- | -------- |
| [submodules/transform/docs/BUILD.md](../submodules/transform/docs/BUILD.md) | Canvas → `workflows/` Toolkit YAML |
| [submodules/transform/docs/LOCAL_RUN.md](../submodules/transform/docs/LOCAL_RUN.md) | `module.py transform run`, predecessor modes |
| [submodules/transform/docs/TRANSFORM.md](../submodules/transform/docs/TRANSFORM.md) | Transform stage handlers and config |
| [submodules/transform/docs/SCORING.md](../submodules/transform/docs/SCORING.md) | Score / match evaluation |
| [submodules/transform/docs/DM_QUERY.md](../submodules/transform/docs/DM_QUERY.md) | Data modeling query nodes |
| [submodules/transform/docs/RECORDS_STREAMS.md](../submodules/transform/docs/RECORDS_STREAMS.md) | Records / streams save paths |
| [workflows/README.md](../workflows/README.md) | Generated workflow artifact names |

Authoring YAML: `submodules/transform/workflow_definitions/`. Functions: `submodules/transform/functions/` and `submodules/inverted_index/functions/`. Module config: `default.config.yaml`.

## Related

| Document | Contents |
| -------- | -------- |
| [ACCELERATOR_CONFIG_CONVENTIONS.md](../../ACCELERATOR_CONFIG_CONVENTIONS.md) | Config families **C** (`governance/`) and **D** (operator prefs) |
| [OPERATOR_UI_STANDARD.md](../../OPERATOR_UI_STANDARD.md) | Shared operator UI patterns |
| [specifications/generated_artifacts.md](specifications/generated_artifacts.md) | Generated Space/Group and workflow YAML |
| [Accelerators README](../../README.md) | Dev port matrix across modules |
