"""Context Quality metrics function.

One function, several actions. The payload decides what runs:

    {"action": "compute_metrics", "dry_run": false}   the whole run in one call: for small projects and dry runs
    {"action": "plan", "functionExternalId": "..."}    step one of a fanned-out run: finds the shards, starts the run
    {"action": "compute_shard", ...}                   step two, once per shard of whole spaces (made by plan)
    {"action": "compute_batch", ...}                   step two, once per slice of a space that is too big (made by plan)
    {"action": "finalize", ...}                        step three: completes the run when every shard has reported
    {"action": "export", ...}                          step four: the workbook of every unlinked item of the run
    {"action": "validate"}

The workflow runs plan, then a dynamic task that calls compute_shard once per shard, then finalize (cq.fanout).

Any key in payload["config"] overrides the matching key in cq.config.DEFAULT_CONFIG.

compute_metrics also takes payload["extraLinks"], a list of {"model": "space:externalId:version", "links":
["View.property", ...]}. Each link is measured per facility next to the built-in metrics (cq.extra_links).
"""

from collections.abc import Callable
from typing import Any

from cognite.client import CogniteClient
from cq.config import build_config
from cq.runner import compute_batch, compute_metrics, compute_shard, export_workbook, finalize, plan, validate
from cq.types import RunConfig

ACTIONS: dict[str, Callable[[CogniteClient, RunConfig, dict[str, Any]], dict[str, Any]]] = {
    "compute_metrics": compute_metrics,
    "plan": plan,
    "compute_shard": compute_shard,
    "compute_batch": compute_batch,
    "finalize": finalize,
    "export": export_workbook,
    "validate": validate,
}


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    action = (data or {}).get("action")
    if action not in ACTIONS:
        raise ValueError(f"Unknown action {action!r}. Expected one of {sorted(ACTIONS)}")
    cfg = build_config(data.get("config"))
    return ACTIONS[action](client, cfg, data)
