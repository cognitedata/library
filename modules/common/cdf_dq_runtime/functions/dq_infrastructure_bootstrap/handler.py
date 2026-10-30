"""Bootstrap Data Quality Validation infrastructure from the released package."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from cognite_data_quality import deploy_validation_infrastructure

if TYPE_CHECKING:
    from cognite.client import CogniteClient


BOOTSTRAP_FUNCTION_EXTERNAL_ID = "dq-infrastructure-bootstrap"
BOOTSTRAP_SCHEDULE_NAME = "dq-infrastructure-bootstrap"


def _delete_bootstrap_schedule(client: CogniteClient) -> None:
    """Delete the Toolkit-managed schedule after bootstrap converges."""
    schedules = client.functions.schedules.list(
        name=BOOTSTRAP_SCHEDULE_NAME,
        function_external_id=BOOTSTRAP_FUNCTION_EXTERNAL_ID,
        limit=-1,
    )
    for schedule in schedules:
        client.functions.schedules.delete(id=schedule.id)


def handler(
    data: dict[str, Any], client: CogniteClient, secrets: dict[str, str] | None = None
) -> dict[str, object]:
    """Deploy Data Quality Validation infrastructure from this module's rendered config."""
    config_dir = Path(__file__).parent / "config"
    result = deploy_validation_infrastructure(
        client=client,
        settings_path=config_dir / "settings.yaml",
        views_dir=(config_dir / "views") if (config_dir / "views").is_dir() else None,
        timeseries_dir=(config_dir / "timeseries") if (config_dir / "timeseries").is_dir() else None,
        shacl_rules_dir=(config_dir / "shacl_rules") if (config_dir / "shacl_rules").is_dir() else None,
        function_secrets=secrets,
        force=bool(data.get("force", False)),
    )
    _delete_bootstrap_schedule(client)
    return result
