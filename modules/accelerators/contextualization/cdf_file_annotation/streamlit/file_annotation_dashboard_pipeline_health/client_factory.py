import os
from typing import Optional

from cognite.client import CogniteClient, ClientConfig, global_config
from cognite.client.credentials import OAuthClientCredentials


class CogniteClientFactory:
    @staticmethod
    def create_from_env(cluster_prefix: str = "", debug: bool = False) -> Optional[CogniteClient]:
        project = os.getenv("CDF_PROJECT")
        cluster = os.getenv("CDF_CLUSTER")
        tenant_id = os.getenv("IDP_TENANT_ID")
        client_id = os.getenv("IDP_CLIENT_ID")
        client_secret = os.getenv("IDP_CLIENT_SECRET")

        if not (project and cluster and tenant_id and client_id and client_secret):
            return None

        scopes = [f"https://{cluster}.cognitedata.com/.default"]
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        creds = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )
        cnf = ClientConfig(
            client_name="DEV_Working",
            project=project,
            base_url=f"https://{cluster_prefix}{cluster}.cognitedata.com",
            credentials=creds,
            debug=debug,
        )
        client = CogniteClient(cnf)
        global_config.apply_settings({
            "max_retries": 5,
            "disable_ssl": True
        })
        return client
