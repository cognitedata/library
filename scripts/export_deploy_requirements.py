"""Export pinned requirements.txt files for CDF deployable Python packages.

Regenerate deploy requirements after changing any workspace member dependency:

    uv lock
    python scripts/export_deploy_requirements.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "scripts" / "uv_workspace_members.json"

EXPORT_TARGETS = [
    "modules/common/cdf_common/functions/contextualization_connection_writer",
    "modules/contextualization/cdf_entity_matching/functions/fn_dm_context_metadata_update",
    "modules/contextualization/cdf_entity_matching/functions/fn_dm_context_timeseries_entity_matching",
    "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_finalize",
    "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_launch",
    "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_prepare",
    "modules/contextualization/cdf_file_annotation/functions/fn_file_annotation_promote",
    "modules/contextualization/cdf_file_annotation/streamlit/file_annotation_dashboard_annotation_quality",
    "modules/contextualization/cdf_file_annotation/streamlit/file_annotation_dashboard_pipeline_health",
    "modules/contextualization/cdf_p_and_id_annotation/functions/fn_dm_context_files_annotation",
    "modules/dashboards/context_quality/functions/context_quality_handler",
    "modules/dashboards/context_quality/streamlit/context_quality_dashboard",
    "modules/solutions/cdf_ai_extractor/functions/fn_ai_property_extractor",
    "modules/sourcesystem/cdf_oid_sync/functions/fn_oid_sync",
]


def _package_name(package_dir: Path) -> str:
    pyproject = package_dir / "pyproject.toml"
    for line in pyproject.read_text(encoding="utf-8").splitlines():
        if line.startswith("name = "):
            return json.loads(line.split("=", 1)[1].strip())
    raise ValueError(f"Could not read [project].name from {pyproject}")


def export_requirements(package_rel_path: str) -> None:
    package_dir = REPO_ROOT / package_rel_path
    requirements_path = package_dir / "requirements.txt"
    package_name = _package_name(package_dir)
    command = [
        "uv",
        "export",
        "--package",
        package_name,
        "--no-dev",
        "--no-hashes",
        "--frozen",
        "--format",
        "requirements-txt",
        "-o",
        str(requirements_path),
    ]
    subprocess.run(command, cwd=REPO_ROOT, check=True)
    print(f"Exported {requirements_path.relative_to(REPO_ROOT)}")


def main() -> None:
    if not MANIFEST_PATH.exists():
        print("Missing scripts/uv_workspace_members.json — run generate_uv_member_projects.py first.", file=sys.stderr)
        raise SystemExit(1)

    for package_rel_path in EXPORT_TARGETS:
        export_requirements(package_rel_path)


if __name__ == "__main__":
    main()
