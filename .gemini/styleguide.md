# Cognite Library Style Guide

This repository contains **Cognite deployment packs** — Toolkit modules
(YAML/TOML configuration) plus Python helper scripts for building and
validating packages. Reviews should cover both the configuration content of
modules under `modules/` and the Python tooling at the repo root.

## Code Review Guardrails (anti-false-positive)

Apply these **before** any other rule. Prefer silence over a speculative or
recycled comment.

1. **Do not re-raise declined findings.** If a human already declined or
   corrected a Gemini suggestion on this PR (same file/topic), do **not**
   post the same claim again with different wording.
2. **Skip checks CI already enforces.** CI runs `ruff check` (incl. `I001`
   import order), `ruff format --check`, `pyright`, `pytest`, and
   `validate_packages.py`. **Do NOT flag** import sorting/grouping,
   formatting, or line length when the tree already matches ruff. For CDF
   Function tests, `handler` is a same-folder import with **no**
   `known-first-party` entry — ruff may place `from handler import …`
   alphabetically next to third-party `from` imports; that order is correct
   when `ruff check` passes. Do **not** invent a blank-line “local group”
   that would fail I001.
3. **Verify Cognite SDK / CDM facts before claiming TypeError or schema
   mismatch.** Prefer official Cognite docs and the pinned `cognite-sdk`
   version in the function’s `requirements.txt` over training memory.
   Known facts in this repo (do **not** contradict):
   - `client.data_modeling.records.sync(...)` yields **`SyncRecordList`
     chunks** (`for chunk in sync(...): for record in chunk:`). It does
     **not** yield bare `Record` objects one-by-one. Nested
     `Iterable[Iterable[…]]` helpers (e.g. `collect_existing_external_ids`)
     and unit tests that pass a **list-of-chunks** are correct. **Do NOT**
     claim `'Record' object is not iterable`, demand a flat
     `Iterable[Record]` signature, or rewrite matching tests to a flat list.
   - `sync(..., chunk_size=...)` **is a valid keyword** (1–1000, default
     1000) on cognite-sdk ≥8.24 Records API. **Do NOT** claim
     `chunk_size` raises `TypeError` or suggest removing it.
   - `sync` requires exactly one of `initialize_cursor` or `cursor`.
     `initialize_cursor` values are `{duration}-ago` (e.g. `365d-ago`),
     not `"beginning"`.
   - `records.ingest(items, *, stream_id=...)` — first positional arg is
     the write items; `stream_id` is keyword-only. Do **not** suggest
     swapping args or a `records=` kwarg.
   - `global_config.silence_feature_preview_warnings` is the **correct**
     Cognite SDK attribute. **Do NOT** suggest renaming it to
     `disable_feature_preview_warnings` (that name does not exist).
   - Retry limits live on **`global_config.max_retries`** (and related
     GlobalConfig fields). **`ClientConfig` has no `max_retries`.** Do
     **not** suggest `client.config.max_retries = …` or
     `return CogniteClient(client.config)` to “apply” retries inside a
     CDF Function `handle()`. Setting `global_config.max_retries` (as in
     `configure_sdk_retries()`) is the correct API; speculative
     re-instantiation of the runtime-injected client is not a required
     fix.
   - Cognite RAW `rows.list(..., limit=-1)` is the SDK **unlimited**
     sentinel. **Do NOT** claim it causes HTTP 400 or suggest
     `limit=None` instead.
   - Cognite RAW **row keys are unique** within a table. Building
     `candidate_ids` (or similar) from `row["key"]` / `row.key` does **not**
     need a `set(...)` wrap for de-duplication — that is speculative
     micro-optimization, not a bug. Prefer silence when ingest/plan code
     already dedupes by `external_id`.
   - DMS container `maxListSize` is a **supported** list-length constraint
     on `type:` for `list: true` properties (distinct from `maxTextSize`).
     Values up to the documented CDF limits are valid. **Do NOT** claim it
     is unsupported, invalid YAML, or that Toolkit/DMS will reject deploy
     because of `maxListSize`. Do **not** suggest removing it.
   - `CogniteTimeSeries.equipment` is a **list** of direct relations
     (`list: true`), same pattern as `assets`. In PI/SAP sample transforms,
     `collect_set(node_reference(...)) AS equipment` and assigning that
     array column is correct. **Do NOT** claim `list: false`, demand
     `min`/`first` + a scalar `node_reference`, or re-raise this after it
     was declined on the same PR.
4. **Toolkit YAML conventions.** Unquoted `{{ variable }}` placeholders in
   Cognite Toolkit YAML are normal in this repo. **Do NOT flag** them as
   YAML syntax errors or demand quoting solely for standard YAML parsers.
5. **CDF Functions logging.** When a handler documents intentional `print`
   for structured logs (Azure Functions hosts often hide `logging`
   tracebacks), **Do NOT** demand `logging` instead.
6. **Intentional `status="deferred"` (verify-then-complete).** When a
   Records ingest handler returns `deferred` because post-ingest
   verification found fewer ids than written (stream eventual
   consistency / search lag), that is an **intentional** soft-complete so
   a later re-run can confirm — **not** a silent pipeline bug. **Do NOT**
   demand `raise RuntimeError` solely to force workflow failure/retry
   unless the handler claims hard-fail semantics.
7. **Catch real bugs, not speculative rewrites.** Do not propose defensive
   `getattr` reshapes, alternate SDK call shapes, or “efficiency” rewrites
   (set-dedup of already-unique RAW keys, micro-optimizations with no
   correctness impact) that contradict the Records API surface
   (`filter` / `sync` / `aggregate` — there is no Records `retrieve` /
   by-ids API).

## Key Principles

- **Security first**: Flag hard-coded credentials, secrets, tokens, private
  keys, customer/project identifiers, or any sensitive data. Configuration
  values that look like credentials must come from environment variables or
  Toolkit variables, never be committed.
- **Logic gaps**: Call out missing error handling, unhandled edge cases,
  silently-swallowed exceptions, and assumptions that aren't validated.
- **Clean code**: Readability, small focused changes, clear naming, and no
  dead/commented-out code.
- **Strong typing** (Python): Use type hints extensively. Avoid `Any` when
  possible. Prefer dataclasses or Pydantic models over untyped dictionaries.
- **Consistency**: Follow established patterns across the codebase and the
  Toolkit module conventions documented in `CONTRIBUTING.md`.

## Principles on doing pull request reviews

- **Main point first.** Start with the key feedback or required action.
- **Be concise.** Use short, direct comments. Avoid unnecessary explanations.
- **Actionable suggestions.** If something needs fixing, state exactly what and how.
- **One issue per comment.** Separate unrelated feedback for clarity.
- **Code, not prose.** Prefer code snippets or examples over long text.
- **Background only if needed.** Add context only if the main point isn't obvious.

## How to do pull request summaries

- **Short recap.** Summarize the main point of the PR in one or two sentences.
- **Don't repeat the PR description.** Only add new or clarifying information.
- **Be brief unless needed.** Only write a longer summary if the PR description
  is missing crucial details.
- **Extend, don't duplicate.** If more detail is needed, clearly state what is
  missing from the PR description and add only the necessary context.

## Security & Configuration Review Focus

- **No hard-coded secrets.** API keys, client secrets, tokens, passwords,
  private keys, connection strings with embedded credentials, or service
  account JSON must never appear in the diff. Flag any such occurrence as a
  HIGH severity issue.
- **CDF project / cluster names** and customer-specific identifiers should
  be parameterised via Toolkit variables (`{{ variable_name }}`), not
  hard-coded in module YAML.
- **`.env` files, credentials.json, *.pem, *.key`** and similar files must
  not be committed.
- **Permissions / capabilities.** Flag overly broad capabilities (e.g.
  wildcards `*` for `dataSetScope`, `idScope`, ACLs) in group definitions
  unless clearly justified.
- **Logic gaps.** Watch for: unguarded dictionary access, missing `None`
  checks, broad `except Exception` blocks, ignored return values, and
  validation logic that doesn't cover all branches.

## Toolkit Module Conventions

- Module names follow the prefix conventions in `README.md`:
  - `cdf_` for Cognite-built platform capabilities
  - `cdm_` for solutions on Cognite Data Model
  - no prefix for industry models, dashboards, and tools
- New modules and packages must be registered in `modules/packages.toml`.
- YAML resource files should be valid against the Toolkit schema; prefer
  the existing folder layout (`auth/`, `data_models/`, `transformations/`,
  `functions/`, `workflows/`, etc.) over ad-hoc placement.
- Cross-module references should use Toolkit variables, not absolute paths.

## Python: Line Length and Formatting

- **Maximum line length**: 120 characters
- **Target Python version**: 3.13+
- **Indentation**: 4 spaces per level

## Python: Type Hints

- **Required**: All functions, methods, and class attributes must have type hints
- **Avoid `Any`**: Use specific types whenever possible
- **Complex data**: Use dataclasses or Pydantic models instead of `dict[str, Any]`
- **File operations**: Always parse file content into typed structures

```python
# Good - typed data structure
@dataclass
class PackageSpec:
    name: str
    modules: list[str]

def load_spec(path: Path) -> PackageSpec:
    data = tomllib.loads(path.read_text())
    return PackageSpec(**data)

# Bad - untyped dictionary
def load_spec(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text())
```

## Python: Imports

- **Authority**: `ruff check` (`I` / isort) is the source of truth. If ruff
  is green, do not open a review comment about import order.
- **Group imports**: Standard library, third-party, local application when
  first-party packages are configured; otherwise ruff’s single sorted
  `from` block is fine.
- **Absolute imports**: Always use absolute imports for clarity
- **Sort alphabetically** within groups, following ruff's isort order (the `I` rule in `pyproject.toml`): plain
  `import x` statements come before `from x import y` statements, each sorted by module name. Do not reorder imports
  to interleave the two; `ruff check` flags that as `I001`.
- **Type checking imports**: Use `TYPE_CHECKING` for type-only imports

## Naming Conventions

- **Variables/functions**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Classes**: `PascalCase`
- **Modules**: `snake_case`
- **Private members**: Single leading underscore `_private`

## Docstrings

Use concise docstrings with Args/Returns format.

```python
def validate_package(path: Path) -> list[str]:
    """
    Validate a deployment pack and return a list of errors.

    Args:
        path: path to the package directory

    Returns:
        list of human-readable error messages; empty if the package is valid.
    """
```

**Docstring patterns**:

- Start with a concise description
- Use `Args:` and `Returns:` for complex functions
- Omit obvious parameter descriptions
- Keep descriptions brief and factual
- Ok for `__init__` methods to omit docstring

## Error Handling

- **Specific exceptions**: Avoid broad `Exception` catches
- **Graceful handling**: Provide meaningful error messages
- **Type safety**: Return `None` or use Union types for fallible operations

```python
def load_manifest(response: str) -> dict[str, Any] | None:
    try:
        return json.loads(response)
    except (TypeError, json.JSONDecodeError) as e:
        log.warning(f"Failed to parse manifest: {e}")
        return None
```

## Data Structures

**Prefer typed structures**:

```python
# Good
@dataclass
class ValidationError:
    module: str
    message: str

# Avoid
error_data = {"module": "foo", "message": "bar"}
```

## Logging

- Use the `logging` module with appropriate levels
- Include contextual information for debugging
- Format: `log.warning(f"Description: {variable}")`

## Code references

When referencing code in your reviews, please include a full working link to the code you are referencing.
Instead of writing "According to `modules/packages.toml` (lines 10-12)", give the url
`https://github.com/cognitedata/library/blob/<branch_name>/modules/packages.toml#L10-L12`
where `<branch_name>` is the name of the PR branch.
