import os

from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials


class CogniteClientFactory:
    @staticmethod
    def create_from_env(base_url_prefix: str = "", debug: bool = False) -> CogniteClient | None:
        """Client from environment variables, for local runs. Set IDP_TOKEN_URL for providers other than Entra ID."""
        project = os.getenv("CDF_PROJECT")
        cluster = os.getenv("CDF_CLUSTER")
        tenant_id = os.getenv("IDP_TENANT_ID")
        client_id = os.getenv("IDP_CLIENT_ID")
        client_secret = os.getenv("IDP_CLIENT_SECRET")

        if not (project and cluster and tenant_id and client_id and client_secret):
            return None

        token_url = os.getenv("IDP_TOKEN_URL", f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")
        creds = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"https://{cluster}.cognitedata.com/.default"],
        )
        cnf = ClientConfig(
            client_name="file_annotation_dashboard",
            project=project,
            base_url=f"https://{base_url_prefix}{cluster}.cognitedata.com",
            credentials=creds,
            debug=debug,
        )
        client = CogniteClient(cnf)
        global_config.apply_settings({"max_retries": 5})
        return client
