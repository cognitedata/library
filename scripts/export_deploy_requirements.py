"""Write requirements.txt for CDF deploy from direct deploy dependencies only.

`requirements.txt` lists packages that must be installed on top of the CDF
Functions runtime — not the full transitive lockfile. Source of truth is
`deploy_dependencies` in `scripts/generate_uv_member_projects.py`.

Regenerate after changing deploy dependencies:

    python scripts/export_deploy_requirements.py
"""


import sys

from generate_uv_member_projects import PACKAGE_SPECS, REPO_ROOT


def _deploy_dependencies(spec: dict[str, object]) -> list[str]:
    deploy = spec.get("deploy_dependencies")
    if deploy is not None:
        return list(deploy)  # type: ignore[arg-type]
    return list(spec["dependencies"])  # type: ignore[arg-type]


def export_requirements(package_rel_path: str, deploy_dependencies: list[str]) -> None:
    requirements_path = REPO_ROOT / package_rel_path / "requirements.txt"
    body = "\n".join(deploy_dependencies)
    if body:
        body += "\n"
    requirements_path.write_text(body, encoding="utf-8")
    print(f"Wrote {requirements_path.relative_to(REPO_ROOT)}")


def main() -> None:
    manifest_path = REPO_ROOT / "scripts" / "uv_workspace_members.json"
    if not manifest_path.exists():
        print("Missing scripts/uv_workspace_members.json — run generate_uv_member_projects.py first.", file=sys.stderr)
        raise SystemExit(1)

    for spec in PACKAGE_SPECS:
        export_requirements(str(spec["path"]), _deploy_dependencies(spec))


if __name__ == "__main__":
    main()
