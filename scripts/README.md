# Repository scripts

Python tooling at the repository root for managing the **uv workspace** and **CDF deploy
`requirements.txt`** files for functions and Streamlit apps under `modules/`.

These scripts are for **contributors** maintaining the library — not for end users deploying
modules with the Cognite Toolkit.

> Module-specific scripts (for example the foundation setup wizard) live under
> `modules/common/cdf_project_foundation/scripts/` and are documented in that module's
> README.

## Scripts

| Script | Purpose |
|--------|---------|
| [`generate_uv_member_projects.py`](generate_uv_member_projects.py) | Source of truth for workspace members. Writes per-package `pyproject.toml` files and [`uv_workspace_members.json`](uv_workspace_members.json). |
| [`export_deploy_requirements.py`](export_deploy_requirements.py) | Writes each member's `requirements.txt` from `deploy_dependencies` in `PACKAGE_SPECS`. |

[`uv_workspace_members.json`](uv_workspace_members.json) is generated — do not edit by hand.

## Typical workflows

Run all commands from the **repository root**.

### After pulling dependency changes

```bash
uv sync --group dev
```

### Change local dev / test dependencies

Edit `dependencies` (and optional `dev_dependencies`) for the package in `PACKAGE_SPECS`, then:

```bash
python scripts/generate_uv_member_projects.py
uv lock
```

Commit the updated member `pyproject.toml`, `uv.lock`, and `scripts/uv_workspace_members.json`.

### Change CDF deploy dependencies

CDF Functions install packages listed in each handler's `requirements.txt` on top of the
runtime. Edit `deploy_dependencies` in `PACKAGE_SPECS` (or add a shared constant at the top
of `generate_uv_member_projects.py` when several packages share the same deploy set), then:

```bash
python scripts/generate_uv_member_projects.py   # if pyproject.toml also changed
python scripts/export_deploy_requirements.py
```

Commit the updated `requirements.txt` file(s).

### Add a new Python package (function, Streamlit app, or helper)

1. Create the package folder under `modules/` with handler code (and tests if applicable).
2. Add an entry to `PACKAGE_SPECS` in [`generate_uv_member_projects.py`](generate_uv_member_projects.py):
   - `path` — directory relative to the repo root (forward slashes)
   - `name` — PEP 503 project name for the generated `pyproject.toml`
   - `requires_python` — CDF runtime range (typically `>=3.11,<3.14`)
   - `dependencies` — local dev / `uv lock` dependencies (ranges allowed)
   - `deploy_dependencies` — **pinned direct** packages for CDF deploy (optional; defaults to `dependencies`)
   - `pytest` — set `True` to emit `[tool.pytest.ini_options]` in the generated `pyproject.toml`
3. Register the same `path` in the root [`pyproject.toml`](../pyproject.toml) under `[tool.uv.workspace] members`.
4. Regenerate artifacts:

```bash
python scripts/generate_uv_member_projects.py
uv lock
python scripts/export_deploy_requirements.py
```

5. Register the module in [`modules/packages.toml`](../modules/packages.toml) if it is new deployable content (see [ADDING_PACKAGES_AND_MODULES.md](../ADDING_PACKAGES_AND_MODULES.md)).
6. Run checks:

```bash
uv run pytest tests/test_uv_workspace.py -q
uv run ruff check scripts/
uv run pyright scripts/
```

Commit generated `pyproject.toml`, `requirements.txt`, `scripts/uv_workspace_members.json`, and `uv.lock` together with your code changes.

## Contributing changes under `scripts/`

- Keep scripts **stdlib-only** where possible so they run with `python scripts/…` after a clone.
- Follow [AGENTS.md](../AGENTS.md): Python 3.13+, type hints, no `from __future__ import annotations`, ruff/pyright clean.
- **`PACKAGE_SPECS` is the source of truth** for deploy deps; do not hand-edit `requirements.txt` without updating the spec and re-running `export_deploy_requirements.py`.
- Add or extend tests in [`tests/test_uv_workspace.py`](../tests/test_uv_workspace.py) when you change member registration or export behaviour.
- Prefer **shared constants** (for example `FILE_ANNOTATION_DEPLOY`) when multiple packages share identical dependency sets — avoids drift.

## Related documentation

- [AGENTS.md](../AGENTS.md) — agent/contributor guide, local checks, uv workflow
- [modules/README.md](../modules/README.md) — module layout and validation
- [ADDING_PACKAGES_AND_MODULES.md](../ADDING_PACKAGES_AND_MODULES.md) — registering modules and packs
