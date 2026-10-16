"""Context Quality metrics function.

One function, two actions. The payload decides what runs:

    {"action": "compute_metrics", "dry_run": false}
    {"action": "validate"}

Any key in payload["config"] overrides the matching key in cq.config.DEFAULT_CONFIG.

compute_metrics also takes payload["extraLinks"], a list of {"model": "space:externalId:version", "links":
["View.property", ...]}. Each link is measured per facility next to the built-in metrics (cq.extra_links).
"""

from collections.abc import Callable
from typing import Any

from cognite.client import CogniteClient
from cq.config import build_config
from cq.runner import compute_metrics, validate
from cq.types import RunConfig

ACTIONS: dict[str, Callable[[CogniteClient, RunConfig, dict[str, Any]], dict[str, Any]]] = {
    "compute_metrics": compute_metrics,
    "validate": validate,
}


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    action = (data or {}).get("action")
    if action not in ACTIONS:
        raise ValueError(f"Unknown action {action!r}. Expected one of {sorted(ACTIONS)}")
    cfg = build_config(data.get("config"))
    return ACTIONS[action](client, cfg, data)
