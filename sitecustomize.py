"""Repository-local Python startup settings for tests and tooling."""

try:
    from cognite.client.config import global_config

    global_config.disable_pypi_version_check = True
except ImportError:
    # cognite-sdk is optional for some local tooling commands.
    pass
