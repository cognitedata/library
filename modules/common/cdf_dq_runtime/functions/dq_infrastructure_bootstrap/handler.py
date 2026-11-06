"""Bootstrap Data Quality Validation infrastructure from the released package."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cognite.client.exceptions import CogniteAPIError
from cognite_data_quality import deploy_validation_infrastructure

if TYPE_CHECKING:
    from cognite.client import CogniteClient


BOOTSTRAP_FUNCTION_EXTERNAL_ID = "dq-infrastructure-bootstrap"
BOOTSTRAP_SCHEDULE_NAME = "dq-infrastructure-bootstrap"

logger = logging.getLogger(__name__)


def _delete_bootstrap_schedule(client: "CogniteClient") -> None:
    """Delete the Toolkit-managed schedule after bootstrap converges."""
    try:
        schedules = client.functions.schedules.list(function_external_id=BOOTSTRAP_FUNCTION_EXTERNAL_ID, limit=-1)
        for schedule in schedules:
            if schedule.name == BOOTSTRAP_SCHEDULE_NAME:
                client.functions.schedules.delete(id=schedule.id)
    except CogniteAPIError as e:
        logger.warning(f"Failed to delete bootstrap schedule {BOOTSTRAP_SCHEDULE_NAME}: {e}")


def handler(data: dict[str, Any], client: "CogniteClient", secrets: dict[str, str] | None = None) -> dict[str, object]:
    """Deploy Data Quality Validation infrastructure from this module's rendered config."""
    config_dir = Path(__file__).parent / "config"
    try:
        return deploy_validation_infrastructure(
            client=client,
            settings_path=config_dir / "settings.yaml",
            views_dir=(config_dir / "views") if (config_dir / "views").is_dir() else None,
            timeseries_dir=(config_dir / "timeseries") if (config_dir / "timeseries").is_dir() else None,
            shacl_rules_dir=(config_dir / "shacl_rules") if (config_dir / "shacl_rules").is_dir() else None,
            function_secrets=secrets,
            force=bool(data.get("force", False)),
        )
    finally:
        _delete_bootstrap_schedule(client)
