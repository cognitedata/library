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

plan also takes payload["scope"] (cq.scope): "all", "configured" or {"dimensions": [...], "configured": bool}, so that a run
measures some dimensions only; the pieces it creates carry it, and finalize writes it into the complete marker.

compute_metrics also takes payload["extraLinks"], a list of {"model": "space:externalId:version", "links":
["View.property", ...]}. Each link is measured per facility next to the built-in metrics (cq.extra_links).
"""

from collections.abc import Callable
from typing import Any

from cognite.client import CogniteClient
from pydantic import BaseModel, ConfigDict, model_validator
from cq.config import build_config
from cq.resilience import begin_call, harden
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


# What an action reads with payload[...] and so cannot run without
REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    "compute_shard": ("runId", "runTimestamp", "shard"),
    "compute_batch": ("runId", "runTimestamp", "shard"),
    "finalize": ("runId", "runTimestamp"),
    "export": ("runId", "shardCount"),
}


class Payload(BaseModel):
    """The call's data. Keys an action reads on its own (scope, extraLinks, runId, ...) are kept as they come."""

    model_config = ConfigDict(extra="allow")

    action: str | None = None
    config: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _known_action_with_its_keys(self) -> "Payload":
        if self.action not in ACTIONS:
            raise ValueError(f"Unknown action {self.action!r}. Expected one of {sorted(ACTIONS)}")
        missing = [key for key in REQUIRED_KEYS.get(self.action, ()) if key not in (self.model_extra or {})]
        if missing:
            raise ValueError(f"{self.action} needs {', '.join(missing)} in its data")
        return self


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    payload = Payload.model_validate(data or {}).model_dump()
    cfg = build_config(payload.get("config"))
    begin_call()
    return ACTIONS[payload["action"]](harden(client), cfg, payload)


def run_local() -> None:
    """Run one action against the project in the environment: `python handler.py '{"action": "validate"}'`.

    Reads the client from the same variables the Cognite Toolkit uses (CDF_PROJECT, CDF_CLUSTER, IDP_* ...)."""
    import json
    import os
    import sys

    from cognite.client import ClientConfig
    from cognite.client.credentials import OAuthClientCredentials

    project, cluster = os.environ["CDF_PROJECT"], os.environ["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=os.environ["IDP_TOKEN_URL"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cluster}.cognitedata.com/.default"],
    )
    client = CogniteClient(
        ClientConfig(
            client_name="context-quality-local",
            project=project,
            credentials=credentials,
            base_url=f"https://{cluster}.cognitedata.com",
        )
    )
    data = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {"action": "validate"}
    print(json.dumps(handle(data, client), indent=2, default=str))


if __name__ == "__main__":
    run_local()
