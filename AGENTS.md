# AGENTS.md

Guidance for AI agents and contributors working in this repository. Read it before
making changes. For module/package authoring details, defer to the existing docs
(see [Documentation](#documentation)) rather than duplicating them here.

## Project overview

This repo holds **Cognite deployment packs** — Cognite Toolkit modules
(YAML/TOML configuration) under `modules/`, plus a small set of standalone **Python**
helper scripts at the repo root that validate the package registry and build the
release archive. Most code you touch will be that Python tooling; the rest is
declarative Toolkit configuration.

## Repository layout

- **`modules/`** — all deployable content (data models, transformations, functions,
  workflows, auth, dashboards, tools), organised by domain folder.
- **`modules/packages.toml`** — the registry. Every module and deployment pack must be
  registered here; nothing ships unless it is listed.
- **`validate_packages.py`** — validates `packages.toml` structure and that referenced
  module paths exist.
- **`build_packages.py`** — builds the `packages.zip` release archive.
- **`tests/`** — pytest suite for the Python tooling.
- **`.github/workflows/`** — CI (lint, type-check, validate, build, CodeQL).

## Python: how we write code

The full style contract is in [.gemini/styleguide.md](.gemini/styleguide.md). It is the
source of truth. The points below are the ones that bite most often.

- **Target Python 3.14+**, line length **120**, 4-space indentation.
- **Type hints everywhere** — every function, method, and attribute. Avoid `Any`.
  Prefer `dataclass` or Pydantic models over `dict[str, Any]`; parse files into typed
  structures.
- **Use Pydantic** for data classes where validation or parsing is involved.
- **Never** use `from __future__ import annotations`.
- **Imports at the top** of the file, grouped (stdlib, third-party, local) and absolute.
  Use `TYPE_CHECKING` for type-only imports.
- **Specific exceptions**, not broad `except Exception`. Return `None` / Union types for
  fallible operations and log with context (`log.warning(f"...: {var}")`).
- **No hard-coded secrets, project/cluster names, or customer identifiers** — anywhere,
  Python or YAML. Use environment variables or Toolkit variables (`{{ variable_name }}`).
- Concise docstrings with `Args:` / `Returns:` for non-trivial functions.

### Minimum code that solves the problem

Write the smallest change that solves the actual problem. **Nothing speculative** — no
"might need it later" abstractions, options, or generality that no current caller uses.
Delete dead and commented-out code rather than leaving it. If a simpler version passes
the same tests, prefer it.

## Test-driven development

We work test-first and keep tests **pragmatic and minimalistic** — cover the bug or
feature, don't over-test.

1. **Write the failing test first.** Capture the behaviour or reproduce the bug before
   touching the implementation.
2. **Make it pass** with the minimum code (see above).
3. **Refactor** with the test as your safety net.

Tests live in `tests/` and run with `pytest`. Test behaviour, not implementation
details. Choose the test shape that fits:

- **Unit tests** — pure functions and small logic units. Fast, isolated, no I/O. Use
  `tmp_path` and fixtures for file-touching helpers. This is the default for the helper
  scripts.
- **Integration tests** — exercise a script end-to-end (e.g. run `validate_packages.py`
  against a fixture `packages.toml`, or `build_packages.py` against a temp module tree)
  and assert the real output. Use these to lock in behaviour that spans several units or
  reads/writes real files.

Cover edge cases and the specific bug a fix addresses; add an example-based test that
pins the fixed behaviour so it can't regress.

```bash
# Run the whole suite
pytest

# Run one file or test
pytest tests/test_foundation_cicd_generator.py
pytest tests/test_foundation_cicd_generator.py::test_name
```

## Local checks

Run these before committing; CI runs the same and must stay green:

```bash
ruff check .          # lint
ruff format .         # format (line length 120)
pyright               # type-check (Python 3.14)
pytest                # tests
python validate_packages.py   # registry is valid (run if you touched modules/ or packages.toml)
```

CI (`.github/workflows/build-packages.yml`) runs ruff, pyright, `validate_packages.py`,
and `build_packages.py`; `check-no-artifacts.yml` confirms `packages.zip` is not
committed; `codeql.yml` runs security analysis. Do not commit `packages.zip` — it is
generated and git-ignored.

## Work incrementally

Each PR should do **one thing** and ideally stay under **~500 new lines**. Commit in
small, buildable steps while lint, type-check, and tests stay green. Split unrelated
edits into separate commits — and separate PRs — before opening one. Break larger work
into a stack of small PRs, each independently green.

## Commits and pull requests

Use [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

- Commit in **small, buildable steps** while lint and tests remain green for this repo.
  Split **unrelated** edits into separate commits before opening a pull request. Ideally
  a 500 line PR.
- **Subject line:** `type[(scope)][!]: description` — imperative mood, no trailing
  period, blank line before an optional body. Use `!` before `:` and/or a
  **`BREAKING CHANGE:`** footer for incompatible changes (full rules in the link above).
- **Types** (pick the narrowest match): `feat`, `fix`, `docs`, `style`, `refactor`,
  `perf`, `test`, `build`, `ci`, `chore`. **Scope:** optional short area (`auth`,
  `chat`, `deps`); omit if it would be vague.
- **Body:** only for non-obvious motivation or behaviour; keep it short and do not repeat
  the diff. **Footers** (for example `Fixes #123`) when this project tracks issues that
  way.
- **Pull requests:** title and **Summary** should match the same vocabulary; do not
  replace conventional commits with only a PR headline.
- Before committing: review **`git status`** and **`git diff`** (including staged);
  unstage and commit separately if the index mixes unrelated concerns.

### PR reviews and summaries

Follow the review and summary guidance in
[.gemini/styleguide.md](.gemini/styleguide.md): main point first, concise, one issue per
comment, actionable code over prose. When referencing code in a review, link to the
exact lines on the PR branch:
`https://github.com/cognitedata/library/blob/<branch_name>/<path>#L10-L12`.

## Toolkit modules

When adding or changing modules under `modules/`:

- Follow the naming prefixes — `cdf_` for Cognite-built platform capabilities, `cdm_` for
  solutions on Cognite Data Model, no prefix for industry models, dashboards, and tools.
- Register every new module and package in `modules/packages.toml`.
- Use the existing folder layout (`auth/`, `data_models/`, `transformations/`,
  `functions/`, `workflows/`, …); use Toolkit variables for cross-module references, not
  absolute paths.
- Validate with `python validate_packages.py` before committing.

### Every module needs a README

Each module must contain a `README.md` following the structure of
[`modules/data_models/rmdm/README.md`](https://github.com/cognitedata/library/blob/main/modules/data_models/rmdm/README.md).
Adapt the sections to the module, but keep this skeleton and order:

1. **Title** — module name plus a short descriptive name.
2. **Overview** — what the module is and why it's needed (the problem it solves).
3. **Module Components / contents** — the key resources the module ships (e.g. data
   model entities, transformations, functions, workflows).
4. **Deployment** — Prerequisites (minimum Toolkit version) and the setup paths:
   adding it to an existing Toolkit project, and starting from scratch (build and
   deploy steps).
5. **Module Structure** — the folder/file layout.
6. **Support** — where to get help, and any reference links (docs, Cognite Hub).

A module is not complete without its README; add or update it in the same PR as the
module change.

Authoring details live in
[ADDING_PACKAGES_AND_MODULES.md](ADDING_PACKAGES_AND_MODULES.md) and
[modules/README.md](modules/README.md) — follow them rather than inventing new patterns.

## Documentation

- [README.md](README.md) — consumer overview and naming conventions.
- [ADDING_PACKAGES_AND_MODULES.md](ADDING_PACKAGES_AND_MODULES.md) — how to add modules
  and packages.
- [modules/README.md](modules/README.md) — folder layout, registry, module structure.
- [RELEASE_WORKFLOW.md](RELEASE_WORKFLOW.md) — release process.
- [.gemini/styleguide.md](.gemini/styleguide.md) — full Python and review style guide.

When behaviour or a convention changes, update the relevant doc in the same PR.
