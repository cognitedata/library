"""Generate per-package pyproject.toml files for the uv workspace.

Run from the repository root after editing PACKAGE_SPECS:

    python scripts/generate_uv_member_projects.py
    python scripts/export_deploy_requirements.py
"""


import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Local dev / uv lock dependencies (may include broader ranges than deploy).
FILE_ANNOTATION_RUNTIME = [
    "cognite-sdk>=7.76.0,<8",
    "msal==1.37.0",
    "pydantic>=2.12.4,<3.0.0",
    "PyJWT>=2.13.0,<3.0",
    "python-dotenv>=1.2.2,<2.0",
    "PyYAML>=6.0.2,<7.0",
    "mixpanel>=4.10.0",
    "protobuf>=6.33.5,<7.0",
]

FILE_ANNOTATION_STREAMLIT_RUNTIME = [
    "pandas",
    "altair",
    "PyYAML",
    "pyodide-http==0.2.1",
    "cognite-sdk>=7.73.4,<8",
    "python-dotenv>=1.0.0",
]

# CDF deploy: direct packages installed on top of the Functions runtime.
FILE_ANNOTATION_DEPLOY = [
    "annotated-types==0.7.0",
    "certifi==2025.4.26",
    "cffi>=1.17.1",
    "charset-normalizer==3.4.2",
    "cognite-sdk==7.76.0",
    "cryptography==48.0.1",
    "idna==3.15",
    "msal==1.37.0",
    "oauthlib==3.2.2",
    "packaging==25.0",
    "protobuf>=6.33.5",
    "pycparser==2.22",
    "pydantic>=2.12.4,<3.0.0",
    "PyJWT==2.13.0",
    "python-dotenv==1.2.2",
    "PyYAML==6.0.2",
    "requests==2.33.0",
    "requests-oauthlib==1.3.1",
    "typing-inspection==0.4.0",
    "typing_extensions==4.13.2",
    "urllib3==2.7.0",
    "mixpanel>=4.10.0",
]

FILE_ANNOTATION_STREAMLIT_DEPLOY = [
    "pandas",
    "altair",
    "PyYaml",
    "pyodide-http==0.2.1",
    "cognite-sdk==7.73.4",
    "python-dotenv>=1.0.0",
]

PACKAGE_SPECS: list[dict[str, object]] = [
    {
        "path": "modules/common/cdf_common/functions/contextualization_connection_writer",
        "name": "contextualization-connection-writer",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "cognite-sdk>=7,<8",
            "pydantic>=2.12.4,<3.0.0",
            "pyyaml>=6",
            "mixpanel>=4.10.0",
        ],
        "deploy_dependencies": [
            "cognite-sdk == 7.*",
            "pydantic>=2.12.4,<3.0.0",
            "pyyaml==6.*",
            "mixpanel>=4.10.0",
        ],
    },
    {
        "path": "modules/contextualization/cdf_entity_matching/functions/fn_dm_context_metadata_update",
        "name": "fn-dm-context-metadata-update",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "cognite-extractor-utils>=7",
            "cognite-sdk>=7,<8",
            "pyyaml>=6.0.1",
            "tenacity>=8.0.0",
            "psutil>=5.9.0",
            "mixpanel>=4.10.0",
        ],
        "deploy_dependencies": [
            "cognite-extractor-utils>=7",
            "cognite-sdk == 7.*",
            "pyyaml >= 6.0.1",
            "tenacity >= 8.0.0",
            "psutil >= 5.9.0",
            "mixpanel>=4.10.0",
        ],
    },
    {
        "path": "modules/contextualization/cdf_entity_matching/functions/fn_dm_context_timeseries_entity_matching",
        "name": "fn-dm-context-timeseries-entity-matching",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "cognite-extractor-utils>=7",
            "cognite-sdk>=7,<8",
            "pyyaml>=6.0.1",
            "tenacity>=8.0.0",
            "psutil>=5.9.0",
            "mixpanel>=4.10.0",
        ],
        "deploy_dependencies": [
            "cognite-extractor-utils>=7",
            "cognite-sdk == 7.*",
            "pyyaml >= 6.0.1",
            "tenacity >= 8.0.0",
            "psutil >= 5.9.0",
            "mixpanel>=4.10.0",
        ],
        "dev_dependencies": ["pytest>=7.0.0"],
        "pytest": True,
    },
    {
        "path": "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_finalize",
        "name": "fn-file-annotation-finalize",
        "requires_python": ">=3.11,<3.14",
        "dependencies": FILE_ANNOTATION_RUNTIME,
        "deploy_dependencies": FILE_ANNOTATION_DEPLOY,
    },
    {
        "path": "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_launch",
        "name": "fn-file-annotation-launch",
        "requires_python": ">=3.11,<3.14",
        "dependencies": FILE_ANNOTATION_RUNTIME,
        "deploy_dependencies": FILE_ANNOTATION_DEPLOY,
    },
    {
        "path": "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_prepare",
        "name": "fn-file-annotation-prepare",
        "requires_python": ">=3.11,<3.14",
        "dependencies": FILE_ANNOTATION_RUNTIME,
        "deploy_dependencies": FILE_ANNOTATION_DEPLOY,
    },
    {
        "path": "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_promote",
        "name": "fn-file-annotation-promote",
        "requires_python": ">=3.11,<3.14",
        "dependencies": FILE_ANNOTATION_RUNTIME,
        "deploy_dependencies": FILE_ANNOTATION_DEPLOY,
    },
    {
        "path": "modules/contextualization/cdf_file_annotation/streamlit/file_annotation_dashboard_annotation_quality",
        "name": "file-annotation-dashboard-annotation-quality",
        "requires_python": ">=3.11,<3.14",
        "dependencies": FILE_ANNOTATION_STREAMLIT_RUNTIME,
        "deploy_dependencies": FILE_ANNOTATION_STREAMLIT_DEPLOY,
    },
    {
        "path": "modules/contextualization/cdf_file_annotation/streamlit/file_annotation_dashboard_pipeline_health",
        "name": "file-annotation-dashboard-pipeline-health",
        "requires_python": ">=3.11,<3.14",
        "dependencies": FILE_ANNOTATION_STREAMLIT_RUNTIME,
        "deploy_dependencies": FILE_ANNOTATION_STREAMLIT_DEPLOY,
    },
    {
        "path": "modules/contextualization/cdf_p_and_id_annotation/functions/fn_dm_context_files_annotation",
        "name": "fn-dm-context-files-annotation",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "cognite-extractor-utils>=7",
            "cognite-sdk>=7,<8",
            "pyyaml>=6.0.1",
            "mixpanel>=4.10.0",
        ],
        "deploy_dependencies": [
            "cognite-extractor-utils>=7",
            "cognite-sdk == 7.*",
            "pyyaml >= 6.0.1",
            "mixpanel>=4.10.0",
        ],
        "pytest": True,
    },
    {
        "path": "modules/dashboards/context_quality/functions/context_quality_handler",
        "name": "context-quality-handler",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "cognite-sdk>=7,<8",
            "mixpanel>=4.10.0",
        ],
        "deploy_dependencies": [
            "cognite-sdk>=7.0.0",
            "mixpanel>=4.10.0",
        ],
    },
    {
        "path": "modules/dashboards/context_quality/streamlit/context_quality_dashboard",
        "name": "context-quality-dashboard",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "pyodide-http>=0.2.1",
            "cognite-sdk>=7.89.0,<8",
            "cognite-pygen",
            "packaging",
            "plotly",
            "matplotlib",
            "fpdf2>=2.7.0",
        ],
        "deploy_dependencies": [
            "pyodide-http>=0.2.1",
            "cognite-sdk>=7.89.0,<8",
            "cognite-pygen",
            "packaging",
            "plotly",
            "matplotlib",
            "fpdf2>=2.7.0",
        ],
    },
    {
        "path": "modules/solutions/cdf_ai_extractor/functions/fn_ai_property_extractor",
        "name": "fn-ai-property-extractor",
        "requires_python": ">=3.11,<3.14",
        "dependencies": [
            "cognite-sdk>=7,<8",
            "pyyaml>=6.0.1",
            "pydantic>=2.12.4,<3.0.0",
            "mixpanel>=4.10.0",
        ],
        "deploy_dependencies": [
            "cognite-sdk>=7.0.0",
            "pyyaml>=6.0.1",
            "pydantic>=2.12.4,<3.0.0",
            "mixpanel>=4.10.0",
        ],
    },
]


def _quote_dep(dep: str) -> str:
    return json.dumps(dep)


def _render_pyproject(spec: dict[str, object]) -> str:
    name = spec["name"]
    requires_python = spec["requires_python"]
    dependencies = spec["dependencies"]
    dev_dependencies = spec.get("dev_dependencies", [])
    pytest_enabled = bool(spec.get("pytest"))

    lines = [
        "[project]",
        f"name = {json.dumps(name)}",
        'version = "0.0.0"',
        f"requires-python = {json.dumps(requires_python)}",
        "dependencies = [",
    ]
    for dep in dependencies:
        lines.append(f"    {_quote_dep(dep)},")
    lines.append("]")
    lines.append("")

    if dev_dependencies:
        lines.extend(
            [
                "[dependency-groups]",
                "dev = [",
            ]
        )
        for dep in dev_dependencies:
            lines.append(f"    {_quote_dep(dep)},")
        lines.append("]")
        lines.append("")

    if pytest_enabled:
        lines.extend(
            [
                "[tool.pytest.ini_options]",
                'testpaths = ["."]',
                'pythonpath = ["."]',
                'addopts = "-ra"',
                "",
            ]
        )

    return "\n".join(lines)


def main() -> None:
    members: list[str] = ["modules/datamodels/cfihos_oil_and_gas_extension/cfihos_model_config"]
    for spec in PACKAGE_SPECS:
        package_dir = REPO_ROOT / str(spec["path"])
        package_dir.mkdir(parents=True, exist_ok=True)
        pyproject_path = package_dir / "pyproject.toml"
        pyproject_path.write_text(_render_pyproject(spec), encoding="utf-8")
        members.append(str(spec["path"]).replace("\\", "/"))

    manifest_path = REPO_ROOT / "scripts" / "uv_workspace_members.json"
    manifest_path.write_text(json.dumps(members, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(PACKAGE_SPECS)} package pyproject.toml files.")
    print(f"Wrote workspace member manifest to {manifest_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
