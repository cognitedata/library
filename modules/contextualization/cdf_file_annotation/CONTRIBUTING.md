# Contributing to CDF File Annotation

Changes to this module follow the repository [contributing guide](../../../CONTRIBUTING.md) and [AGENTS.md](../../../AGENTS.md). Branch from `main`, use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) (`fix: ...`, `feat: ...`), and open a pull request. Python style, tests, and local checks are defined there.

## Configuration changes

If you change the shape of `extraction_pipelines/ep_file_annotation.config.yaml`:

- Update the Pydantic models in `functions/fn_file_annotation/services/config_service.py`. `Parameters` ignores unknown keys, so a typo or a missing field passes silently.
- Update [CONFIG.md](detailed_guides/CONFIG.md).
- Keep existing deployments working, or document a migration path in that guide.

Values that rarely differ between deployments stay constants in `functions/fn_file_annotation/fa_constants.py`. Promote one to a Toolkit variable when environments need different values. See [Customizing a constant](detailed_guides/CONFIG.md#customizing-a-constant).

## Interfaces

Extend behavior by implementing an existing service interface. `General...Service` is the default implementation. See [DEVELOPING.md](detailed_guides/DEVELOPING.md).

## Docstrings

Add a docstring with `Args` and `Returns` on non-trivial functions, as in the repository style guide. A one-line helper does not need one.

Return to [Main README](README.md)
