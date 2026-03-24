"""
Upload extraction pipeline config YAML to CDF.

The extraction pipeline resource must exist before config can be stored.
This script creates the pipeline if missing, then uploads the config.

Run from library_fresh root with .env containing CDF_PROJECT, CDF_CLUSTER, IDP_TENANT_ID, IDP_CLIENT_ID, IDP_CLIENT_SECRET:

  poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/upload_config_to_cdf.py
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_MODULE_DIR = Path(__file__).resolve().parent
_DEFAULT_CONFIG = _MODULE_DIR / "extraction_pipelines" / "ctx_key_extraction_site_prod.config.yaml"


def main() -> None:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig
    from cognite.client.credentials import OAuthClientCredentials
    from cognite.client.data_classes import ExtractionPipelineConfigWrite, ExtractionPipelineWrite
    from cognite.client.exceptions import CogniteNotFoundError

    site_abbreviation = os.getenv("SITE_ABBREVIATION", "SITE")
    ext_id = os.getenv(
        "EXTRACTION_PIPELINE_EXT_ID", f"ctx_key_extraction_{site_abbreviation}_prod"
    )
    config_path = Path(
        os.getenv("EXTRACTION_PIPELINE_CONFIG_PATH", str(_DEFAULT_CONFIG))
    )
    if not config_path.is_absolute():
        config_path = _MODULE_DIR / config_path

    cfg = config_path.read_text()

    cdf_project = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    tenant_id = os.environ["IDP_TENANT_ID"]
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]

    creds = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
    )
    client = CogniteClient(
        ClientConfig(
            client_name="upload_extraction_pipeline_config",
            project=cdf_project,
            base_url=f"https://{cdf_cluster}.cognitedata.com",
            credentials=creds,
        )
    )

    # Extraction pipeline must exist before config can be stored. Create it if missing.
    try:
        client.extraction_pipelines.retrieve(external_id=ext_id)
    except CogniteNotFoundError:
        print(f"Creating extraction pipeline: {ext_id}")
        client.extraction_pipelines.create(
            ExtractionPipelineWrite(external_id=ext_id, name=ext_id)
        )
        print(f"Created pipeline: {ext_id}")

    created = client.extraction_pipelines.config.create(
        ExtractionPipelineConfigWrite(external_id=ext_id, config=cfg)
    )
    print(f"Stored config for {ext_id} (revision={created.revision}) from {config_path}")


if __name__ == "__main__":
    main()
