"""Bootstrap Data Quality Validation infrastructure from the released package."""

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from cognite.client.exceptions import CogniteAPIError
from cognite_data_quality import deploy_validation_infrastructure

if TYPE_CHECKING:
    from cognite.client import CogniteClient


BOOTSTRAP_FUNCTION_EXTERNAL_ID = "fn_dataproduct_all_bootstrap_infrastructure"
BOOTSTRAP_SCHEDULE_NAME = "fn_dataproduct_all_bootstrap_infrastructure"

# Toolkit cannot substitute module variables inside function code bundles. Pass these via
# Schedule.data (see fn_dataproduct_all_bootstrap_infrastructure.Schedule.yaml) so config.<env>.yaml overrides apply.
_SETTINGS_OVERRIDE_KEYS = frozenset(
    {
        "dataQualitySpace",
        "recordsStreamId",
        "functionCodeDatasetExternalId",
        "dataProductExternalId",
        "dataProductVersion",
    }
)


def _delete_bootstrap_schedule(client: "CogniteClient") -> None:
    """Delete Toolkit bootstrap schedules after a successful deploy.

    Authenticated Toolkit schedules are stored under numeric functionId (session/nonce create
    path); listing by function_external_id returns nothing. Resolve the function, then delete
    every schedule for that function_id. Never raises — delete failures must not mask a
    successful deploy result. Requires functionsAcl:READ + WRITE on the schedule identity.
    """
    try:
        function = client.functions.retrieve(external_id=BOOTSTRAP_FUNCTION_EXTERNAL_ID)
    except CogniteAPIError as e:
        print(f"Failed to retrieve bootstrap function {BOOTSTRAP_FUNCTION_EXTERNAL_ID}: {e} | code={e.code}")
        return
    except Exception as e:
        print(f"Failed to retrieve bootstrap function {BOOTSTRAP_FUNCTION_EXTERNAL_ID}: {e}")
        return

    if function is None:
        print(f"Bootstrap function not found: {BOOTSTRAP_FUNCTION_EXTERNAL_ID}")
        return

    function_id = function.id
    try:
        schedules = client.functions.schedules.list(function_id=function_id, limit=None)
    except CogniteAPIError as e:
        print(f"Failed to list bootstrap schedules for function_id={function_id}: {e} | code={e.code}")
        return
    except Exception as e:
        print(f"Failed to list bootstrap schedules for function_id={function_id}: {e}")
        return

    if not schedules:
        print(f"No bootstrap schedules found for {BOOTSTRAP_FUNCTION_EXTERNAL_ID} (function_id={function_id})")
        return

    for schedule in schedules:
        schedule_id = schedule.id
        schedule_name = schedule.name or ""
        try:
            client.functions.schedules.delete(id=schedule_id)
            print(f"Deleted bootstrap schedule id={schedule_id} name={schedule_name}")
        except CogniteAPIError as e:
            print(f"Failed to delete bootstrap schedule id={schedule_id} name={schedule_name}: {e} | code={e.code}")
        except Exception as e:
            print(f"Failed to delete bootstrap schedule id={schedule_id} name={schedule_name}: {e}")


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


class _CredentialsWithClientSecret:
    """Credential provider proxy that exposes client_id/client_secret for trigger upsert.

    cognite-data-quality 0.5.x does not forward ``function_secrets`` into
    ``_deploy_external_dataproduct_workflows``. ``_build_trigger_credentials`` then only
    reads ``client.config.credentials.client_id`` / ``client_secret``. Schedule auth is
    token-only, so overlay Function secrets while keeping the original provider for API calls.
    """

    __slots__ = ("_base", "client_id", "client_secret")

    def __init__(self, base: object, client_id: str, client_secret: str) -> None:
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "client_id", client_id)
        object.__setattr__(self, "client_secret", client_secret)

    def __getattr__(self, name: str) -> object:
        return getattr(self._base, name)


def _overlay_client_credentials_from_secrets(client: "CogniteClient", secrets: dict[str, str] | None) -> None:
    """Attach client_id/client_secret from Function secrets onto the in-function CogniteClient."""
    workflows = _credential_pair(
        secrets or {},
        id_keys=("workflow-client-id", "client-id"),
        secret_keys=("workflow-client-secret", "client-secret"),
    )
    if workflows is None:
        return
    config = getattr(client, "config", None)
    if config is None:
        return
    existing = getattr(config, "credentials", None)
    if existing is None:
        return
    if getattr(existing, "client_id", None) and getattr(existing, "client_secret", None):
        return
    config.credentials = _CredentialsWithClientSecret(existing, workflows["client_id"], workflows["client_secret"])


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
        records = settings.get("records")
        if not isinstance(records, dict):
            records = {}
            settings["records"] = records
        records["space"] = space
    if "recordsStreamId" in data:
        records = settings.get("records")
        if not isinstance(records, dict):
            records = {}
            settings["records"] = records
        records["stream_id"] = data["recordsStreamId"]
    if "functionCodeDatasetExternalId" in data:
        settings["function_code_dataset_external_id"] = data["functionCodeDatasetExternalId"]
    if "dataProductExternalId" in data or "dataProductVersion" in data:
        products = settings.get("external_dataproducts")
        if not isinstance(products, list):
            products = []
            settings["external_dataproducts"] = products
        if not products:
            products.append({})
        product = products[0]
        if isinstance(product, dict):
            if "dataProductExternalId" in data:
                product["external_id"] = data["dataProductExternalId"]
            if "dataProductVersion" in data:
                product["version"] = data["dataProductVersion"]
    if runtime_credentials is not None:
        settings["runtime_credentials"] = runtime_credentials

    tmp_path: Path | None = None
    success = False
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml", encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            yaml.safe_dump(settings, tmp, sort_keys=False)
        success = True
        return tmp_path
    finally:
        if not success and tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def handle(data: dict[str, Any], client: "CogniteClient", secrets: dict[str, str] | None = None) -> dict[str, object]:
    """Deploy Data Quality Validation infrastructure from this module's rendered config."""
    config_dir = Path(__file__).parent / "config"
    bundled_settings = config_dir / "settings.yaml"
    override_path: Path | None = None
    try:
        _overlay_client_credentials_from_secrets(client, secrets)
        override_path = _write_settings_with_overrides(bundled_settings, data, secrets)
        result = deploy_validation_infrastructure(
            client=client,
            settings_path=override_path or bundled_settings,
            views_dir=(config_dir / "views") if (config_dir / "views").is_dir() else None,
            timeseries_dir=(config_dir / "timeseries") if (config_dir / "timeseries").is_dir() else None,
            shacl_rules_dir=(config_dir / "shacl_rules") if (config_dir / "shacl_rules").is_dir() else None,
            function_secrets=secrets,
            force=bool(data.get("force", False)),
        )
    except Exception:
        # Leave the five-minute schedule in place so bootstrap can retry.
        raise
    else:
        _delete_bootstrap_schedule(client)
        return result
    finally:
        if override_path is not None:
            override_path.unlink(missing_ok=True)
