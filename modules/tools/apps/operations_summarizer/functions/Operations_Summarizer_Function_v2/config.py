"""
Configuration for Operations Summarizer v2.

Prompts, defaults, view IDs, and table columns come from the config node.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from datetime import timezone, timedelta
from cognite.client.data_classes.data_modeling import ViewId

from app_config import AppConfigError, load_app_config

# Filled by get_config from the config node. Callers share these mutable objects.
VIEW_ID_MAPPING: Dict[str, ViewId] = {}
DATA_TYPE_ORDER: List[str] = []


class Runtime:
    """Values resolved from the config node, then call-data overrides."""

    summary_space: str = ""
    chunk_size: int = 50
    max_instances_per_datatype: int = 15_000
    instance_query_limit: int = 1000
    helper_agent_name: str = ""
    operation_summary_prefix: str = ""
    prompt_header: str = ""
    holistic_summary_template: str = ""
    agent_instructions: str = ""
    timezone_offset_hours: int = 0
    summary_period_days: int = 7
    persona_presets: Dict[str, tuple] = None  # type: ignore[assignment]


runtime = Runtime()


# =============================================================================
# FORMATTER FUNCTIONS
# =============================================================================


def format_iso_date(date_str):
    if not date_str:
        return "None"
    try:
        from datetime import datetime
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return str(date_str)


def format_assets_list(assets):
    if assets is None or assets == "":
        return "N/A"
    if isinstance(assets, list):
        filtered = [
            str(a.get("externalId") if isinstance(a, dict) else a)
            for a in assets
            if a is not None and a != ""
        ]
        return ", ".join(filtered) or "N/A"
    return str(assets)


# =============================================================================
# DATA TYPE CONFIGURATIONS
# =============================================================================


@dataclass
class TableColumn:
    header: str
    property: str
    formatter: Optional[Callable] = None
    default: Optional[str] = None


@dataclass
class DataTypeConfig:
    name: str
    display_name: str
    view_id: ViewId
    prompt_template: str
    table_columns: List[TableColumn]
    is_special_case: bool = False
    subcategories: Optional[List[str]] = None


DATA_TYPE_CONFIGS: Dict[str, DataTypeConfig] = {}

_FORMATTERS = {
    "isoDate": format_iso_date,
    "assetsList": format_assets_list,
}
_DATA_TYPE_PROMPT_KEYS = ("assets", "files", "work_orders", "time_series", "equipment")


def _require_text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AppConfigError(f"config node {path}: expected a non-empty string")
    return value


def _pick(data: dict, key: str, fallback: Any) -> Any:
    """Call data wins when the key is present and not None."""
    if key in data and data[key] is not None:
        return data[key]
    return fallback


def _view_id(views: dict, name: str) -> ViewId:
    source = views[name]
    return ViewId(space=source["space"], external_id=source["externalId"], version=source["version"])


def _bind_prompts_and_defaults(node: dict) -> None:
    prompts = node.get("prompts")
    if not isinstance(prompts, dict):
        raise AppConfigError("config node prompts: expected an object")
    runtime.prompt_header = _require_text(prompts.get("header"), "prompts.header")
    runtime.holistic_summary_template = _require_text(prompts.get("holistic"), "prompts.holistic")
    runtime.agent_instructions = _require_text(
        prompts.get("agentInstructions"), "prompts.agentInstructions"
    )
    data_type_prompts = prompts.get("dataTypes")
    if not isinstance(data_type_prompts, dict):
        raise AppConfigError("config node prompts.dataTypes: expected an object")
    for key in _DATA_TYPE_PROMPT_KEYS:
        _require_text(data_type_prompts.get(key), f"prompts.dataTypes.{key}")

    defaults = node.get("defaults")
    if not isinstance(defaults, dict):
        raise AppConfigError("config node defaults: expected an object")
    offset = defaults.get("timezoneOffsetHours")
    period = defaults.get("summaryPeriodDays")
    presets = defaults.get("personaPresets")
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise AppConfigError("config node defaults.timezoneOffsetHours: expected an integer")
    if not isinstance(period, int) or isinstance(period, bool) or period < 1:
        raise AppConfigError("config node defaults.summaryPeriodDays: expected a positive integer")
    if not isinstance(presets, dict) or not presets:
        raise AppConfigError("config node defaults.personaPresets: expected an object")
    runtime.timezone_offset_hours = offset
    runtime.summary_period_days = period
    runtime.persona_presets = {
        str(key): tuple(value) if isinstance(value, list) else value
        for key, value in presets.items()
    }


def bind_from_node(node: dict) -> None:
    """Fill shared runtime objects from a validated config node."""
    views = node["dataModel"]["views"]
    VIEW_ID_MAPPING.clear()
    for name, source in views.items():
        VIEW_ID_MAPPING[name] = _view_id(views, name)

    DATA_TYPE_CONFIGS.clear()
    DATA_TYPE_ORDER.clear()
    DATA_TYPE_ORDER.extend(node["dataTypes"].keys())
    for name, source in node["dataTypes"].items():
        columns: List[TableColumn] = []
        for column in source["tableColumns"]:
            formatter_name = column.get("formatter")
            formatter = None
            if formatter_name:
                formatter = _FORMATTERS.get(formatter_name)
                if formatter is None:
                    raise AppConfigError(
                        f"config node dataTypes.{name}: unknown formatter {formatter_name!r}"
                    )
            columns.append(
                TableColumn(
                    header=column["header"],
                    property=column["property"],
                    formatter=formatter,
                    default=column.get("default"),
                )
            )
        prompt = _require_text(
            (node.get("prompts") or {}).get("dataTypes", {}).get(name) if isinstance(node.get("prompts"), dict) else None,
            f"prompts.dataTypes.{name}",
        )
        DATA_TYPE_CONFIGS[name] = DataTypeConfig(
            name=name,
            display_name=source["displayName"],
            view_id=_view_id(views, source["view"]),
            prompt_template=prompt,
            table_columns=columns,
            is_special_case=bool(source.get("isSpecialCase", False)),
            subcategories=source.get("subcategories"),
        )

    limits = node["limits"]
    runtime.summary_space = node["spaces"]["summaries"]
    runtime.chunk_size = int(limits["chunkSize"])
    runtime.max_instances_per_datatype = int(limits["maxInstancesPerDatatype"])
    runtime.instance_query_limit = int(limits["instanceQueryLimit"])
    runtime.helper_agent_name = node["agent"]["name"]
    runtime.operation_summary_prefix = node["externalIdPrefixes"]["operationSummary"]
    _bind_prompts_and_defaults(node)


def _contents_for_persona(persona_key: str, data_type_names: List[str]) -> Dict[str, bool]:
    """Turn a persona preset into per-type flags. Unknown keys and empty matches fail."""
    preset = runtime.persona_presets.get(persona_key)
    if preset is None:
        known = ", ".join(sorted(runtime.persona_presets))
        raise AppConfigError(
            f"unknown persona_key {persona_key!r}. Known presets: {known}."
        )
    selected = [name for name in preset if name in data_type_names]
    if not selected:
        raise AppConfigError(
            f"persona_key {persona_key!r} does not match any data type on the config node."
        )
    return {name: name in selected for name in data_type_names}


def get_config(data: dict, client: Any) -> dict:
    """Merge call ``data`` overrides over the config node."""
    node = load_app_config(client)
    bind_from_node(node)

    prompt_templates = {name: cfg.prompt_template for name, cfg in DATA_TYPE_CONFIGS.items()}
    prompt_templates.update(data.get("prompt_templates", {}))

    data_type_names = list(node["dataTypes"].keys())
    persona_key = data.get("persona_key")
    if persona_key:
        summary_contents = _contents_for_persona(persona_key, data_type_names)
    else:
        summary_contents = {
            name: bool(source.get("enabledByDefault", True))
            for name, source in node["dataTypes"].items()
        }
        explicit_contents = data.get("summary_contents")
        if explicit_contents and isinstance(explicit_contents, dict):
            summary_contents.update(explicit_contents)

    summary_space = _pick(data, "summary_space", runtime.summary_space)
    chunk_size = int(_pick(data, "chunk_size", runtime.chunk_size))
    runtime.summary_space = summary_space
    runtime.chunk_size = chunk_size

    return {
        "summary_space": summary_space,
        "chunk_size": chunk_size,
        "max_instances_per_datatype": runtime.max_instances_per_datatype,
        "prompt_templates": prompt_templates,
        "agent_instructions": data.get("agent_instructions") or runtime.agent_instructions,
        "holistic_summary_template": data.get("holistic_summary_template") or runtime.holistic_summary_template,
        "prompt_header": data.get("prompt_header") or runtime.prompt_header,
        "save_to_cdf": data.get("save_to_cdf", True),
        "generate_pdf": data.get("generate_pdf", True),
        "return_pdf_bytes": data.get("return_pdf_bytes", False),
        "fusion_domain": data.get("fusion_domain"),
        "summary_contents": summary_contents,
        "helper_agent_name": runtime.helper_agent_name,
    }
