"""Bootstrap Data Quality Validation infrastructure from the released package."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from cognite_data_quality import deploy_validation_infrastructure


def handler(data: dict[str, Any], client, secrets: dict[str, str] | None = None) -> dict[str, object]:
    """Deploy Data Quality Validation infrastructure from this module's rendered config."""
    config_dir = Path(__file__).parent / "config"
    return deploy_validation_infrastructure(
        client=client,
        settings_path=config_dir / "settings.yaml",
        views_dir=config_dir / "views",
        timeseries_dir=config_dir / "timeseries",
        shacl_rules_dir=config_dir / "shacl_rules",
        function_secrets=secrets,
        force=bool(data.get("force", False)),
    )
