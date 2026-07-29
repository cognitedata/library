"""Repository-local Python startup settings for tests and tooling."""

from contextlib import suppress

with suppress(ImportError):
    from cognite.client.config import global_config

    global_config.disable_pypi_version_check = True
