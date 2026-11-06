"""Bootstrap Data Quality Validation infrastructure from the released package."""

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from cognite_data_quality import deploy_validation_infrastructure

if TYPE_CHECKING:
    from cognite.client import CogniteClient


BOOTSTRAP_FUNCTION_EXTERNAL_ID = "dq_infrastructure_bootstrap"
BOOTSTRAP_SCHEDULE_NAME = "dq_infrastructure_bootstrap"

# Toolkit cannot substitute module variables inside function code bundles. Pass these via
# Schedule.data (see dq_infrastructure_bootstrap.Schedule.yaml) so config.<env>.yaml overrides apply.
_SETTINGS_OVERRIDE_KEYS = frozenset(
    {
        "dataQualitySpace",
        "recordsStreamId",
        "functionCodeDatasetExternalId",
        "dataProductExternalId",
        "dataProductVersion",
    }
)

logger = logging.getLogger(__name__)


def _delete_bootstrap_schedule(client: "CogniteClient") -> None:
    """Delete the Toolkit-managed schedule after bootstrap converges."""
    try:
        schedules = client.functions.schedules.list(function_external_id=BOOTSTRAP_FUNCTION_EXTERNAL_ID, limit=-1)
        for schedule in schedules:
            if schedule.name == BOOTSTRAP_SCHEDULE_NAME:
                client.functions.schedules.delete(id=schedule.id)
    except Exception as e:
        # Must not raise from finally: any failure here would mask a deploy error.
        logger.warning(f"Failed to delete bootstrap schedule {BOOTSTRAP_SCHEDULE_NAME}: {e}")


def _credential_pair(
    secrets: dict[str, str],
    *,
    id_keys: tuple[str, ...],
    secret_keys: tuple[str, ...],
) -> dict[str, str] | None:
    client_id = next((secrets[key] for key in id_keys if secrets.get(key)), None)
    client_secret = next((secrets[key] for key in secret_keys if secrets.get(key)), None)
    if client_id and client_secret:
        return {"client_id": client_id, "client_secret": client_secret}
    return None


def _runtime_credentials_from_secrets(secrets: dict[str, str] | None) -> dict[str, dict[str, str]] | None:
    """Map CDF Function secrets to settings.runtime_credentials for workflow trigger deploy.

    Schedule-authenticated clients inside CDF Functions expose a token, not client_id/client_secret.
    The package needs explicit credentials to upsert workflow triggers (data_product_sync, etc.).
    """
    if not secrets:
        return None
    functions = _credential_pair(secrets, id_keys=("client-id",), secret_keys=("client-secret",))
    workflows = _credential_pair(
        secrets,
        id_keys=("workflow-client-id", "client-id"),
        secret_keys=("workflow-client-secret", "client-secret"),
    )
    schedules = _credential_pair(
        secrets,
        id_keys=("schedule-client-id", "client-id"),
        secret_keys=("schedule-client-secret", "client-secret"),
    )
    if not any((functions, workflows, schedules)):
        return None
    credentials: dict[str, dict[str, str]] = {}
    if functions:
        credentials["functions"] = functions
    if workflows:
        credentials["workflows"] = workflows
    if schedules:
        credentials["schedules"] = schedules
    return credentials


def _write_settings_with_overrides(
    settings_path: Path,
    data: dict[str, Any],
    secrets: dict[str, str] | None = None,
) -> Path | None:
    """Merge Schedule.data overrides and function secrets into settings.yaml; return a temp path when needed."""
    runtime_credentials = _runtime_credentials_from_secrets(secrets)
    if not _SETTINGS_OVERRIDE_KEYS.intersection(data) and runtime_credentials is None:
        return None

    loaded = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected a mapping in {settings_path}")
    settings: dict[str, Any] = loaded

    if "dataQualitySpace" in data:
        space = data["dataQualitySpace"]
        settings["config_space"] = space
        records = settings.setdefault("records", {})
        if isinstance(records, dict):
            records["space"] = space
    if "recordsStreamId" in data:
        records = settings.setdefault("records", {})
        if isinstance(records, dict):
            records["stream_id"] = data["recordsStreamId"]
    if "functionCodeDatasetExternalId" in data:
        settings["function_code_dataset_external_id"] = data["functionCodeDatasetExternalId"]
    if "dataProductExternalId" in data or "dataProductVersion" in data:
        products = settings.get("external_dataproducts")
        product: dict[str, Any] = dict(products[0]) if isinstance(products, list) and products else {}
        if "dataProductExternalId" in data:
            product["external_id"] = data["dataProductExternalId"]
        if "dataProductVersion" in data:
            product["version"] = data["dataProductVersion"]
        settings["external_dataproducts"] = [product]
    if runtime_credentials is not None:
        settings["runtime_credentials"] = runtime_credentials

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml", encoding="utf-8") as tmp:
        yaml.safe_dump(settings, tmp, sort_keys=False)
        return Path(tmp.name)


def handle(data: dict[str, Any], client: "CogniteClient", secrets: dict[str, str] | None = None) -> dict[str, object]:
    """Deploy Data Quality Validation infrastructure from this module's rendered config."""
    config_dir = Path(__file__).parent / "config"
    bundled_settings = config_dir / "settings.yaml"
    override_path: Path | None = None
    try:
        override_path = _write_settings_with_overrides(bundled_settings, data, secrets)
        return deploy_validation_infrastructure(
            client=client,
            settings_path=override_path or bundled_settings,
            views_dir=(config_dir / "views") if (config_dir / "views").is_dir() else None,
            timeseries_dir=(config_dir / "timeseries") if (config_dir / "timeseries").is_dir() else None,
            shacl_rules_dir=(config_dir / "shacl_rules") if (config_dir / "shacl_rules").is_dir() else None,
            function_secrets=secrets,
            force=bool(data.get("force", False)),
        )
    finally:
        if override_path is not None:
            override_path.unlink(missing_ok=True)
        _delete_bootstrap_schedule(client)
