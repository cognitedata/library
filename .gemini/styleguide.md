# Cognite Library Style Guide

This repository contains **Cognite deployment packs** — Toolkit modules
(YAML/TOML configuration) plus Python helper scripts for building and
validating packages. Reviews should cover both the configuration content of
modules under `modules/` and the Python tooling at the repo root.

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
  Toolkit module conventions documented in `ADDING_PACKAGES_AND_MODULES.md`.

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
- **Target Python version**: 3.14+
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

- **Group imports**: Standard library, third-party, local application
- **Absolute imports**: Always use absolute imports for clarity
- **Sort alphabetically** within groups
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
