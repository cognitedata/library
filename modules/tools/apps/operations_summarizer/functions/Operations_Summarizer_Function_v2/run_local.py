"""
Local test runner for Operations Summarizer v2.

Loads a `.env` file from this directory when present:

    CDF_PROJECT=<project>
    CDF_CLUSTER=<cluster>
    CDF_URL=https://<cluster>.cognitedata.com
    IDP_TENANT_ID=<your-entra-tenant-id>

For client-credentials auth, also set IDP_CLIENT_ID and IDP_CLIENT_SECRET.
For token auth, set CDF_TOKEN instead.

The config node `inst_ops_summarizer_app_config/ops_summarizer_config` must
already be deployed in the project. This runner uses the local Cognite client
to read it.

Then run:
    python run_local.py
"""

from pathlib import Path

from dotenv import load_dotenv

from cdf_client import get_local_client
from handler import handle

load_dotenv(Path(__file__).resolve().parent / ".env")

if __name__ == "__main__":
    client = get_local_client()
    print(f"Connected to project: {client.config.project}\n")

    result = handle(
        data={
            # Relative preset: "last 24 hours" | "last 72 hours" | "last week" | "last month" | "last quarter"
            # Or use explicit dates: "summary_period_start": "2026-06-15", "summary_period_end": "2026-06-22"
            "summary_period_start": "last week",

            # All 5 data types enabled by default; override to test a subset:
            "summary_contents": {
                "assets": True,
                "files": True,
                "work_orders": True,
                "time_series": True,
                "equipment": True,
            },

            # Set to True when you're ready to write results to CDF
            "save_to_cdf": False,
            "generate_pdf": False,

            # Uncomment to use a specific existing agent:
            # "selected_agent": "my-agent-external-id",

            # Uncomment to provide the Fusion domain (required when save_to_cdf=True):
            # "fusion_domain": "your-org.fusion.cognite.com",
        },
        client=client,
        function_call_info={"function_id": "local-test"},
    )

    print()
    print("=" * 80)
    print("RESULT")
    print("=" * 80)
    print(f"Success: {result.get('success')}")

    md = result.get("markdown", "")
    print(f"Markdown length: {len(md)} chars")

    if result.get("truncation_warnings"):
        print(f"Truncation warnings: {result['truncation_warnings']}")

    if md:
        print("-" * 80)
        print(md[:3000])
        if len(md) > 3000:
            print(f"... ({len(md) - 3000} more chars)")
        print("-" * 80)

    if result.get("cdf_result"):
        print(f"Saved to CDF: {result['cdf_result']}")

    if result.get("error"):
        print(f"Error: {result['error']}")
