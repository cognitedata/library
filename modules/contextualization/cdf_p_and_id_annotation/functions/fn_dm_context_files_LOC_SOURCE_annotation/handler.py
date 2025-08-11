from __future__ import annotations

import os
import sys
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from logger import CogniteFunctionLogger
from config import load_config_parameters
from pipeline import annotate_p_and_id


def handle(data: dict, client: CogniteClient) -> dict:
    try:
        loglevel = data.get("logLevel", "INFO")
        logger = CogniteFunctionLogger(loglevel)
        logger.info(
            f"Starting diagram parsing annotation with loglevel = {loglevel},  reading parameters from extraction pipeline config: {data.get('ExtractionPipelineExtId')}"
        )
        config = load_config_parameters(client, data)
        logger.debug("Loaded config successfully")
        annotate_p_and_id(client, logger, data, config)
        return {"status": "succeeded", "data": data}
    except Exception as e:
        message = f"failed, Message: {e!s}"
        logger.error(message)
        return {"status": "failure", "message": message}


def run_locally():
    required_envvars = (
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
        "IDP_TOKEN_URL",
    )
    if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
        raise ValueError(f"Missing one or more env.vars: {missing}")

    cdf_project_name = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    token_uri = os.environ["IDP_TOKEN_URL"]
    base_url = f"https://{cdf_cluster}.cognitedata.com"

    client = CogniteClient(
        ClientConfig(
            client_name="Toolkit P&ID pipeline",
            base_url=base_url,
            project=cdf_project_name,
            credentials=OAuthClientCredentials(
                token_url=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=[f"{base_url}/.default"],
            ),
        )
    )
    data = {
        "logLevel": "INFO",
        "ExtractionPipelineExtId": "ep_ctx_files_LOC_SOURCE_pandid_annotation",
    }
    handle(data, client)


if __name__ == "__main__":
    run_locally()
