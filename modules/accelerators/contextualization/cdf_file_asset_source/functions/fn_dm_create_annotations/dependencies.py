"""
Dependencies module for create asset hierarchy.

Re-exports dependencies from fn_dm_extract_assets_by_pattern.
"""

from ..fn_dm_extract_assets_by_pattern.dependencies import (
    create_client,
    create_logger_service,
    create_write_logger_service,
    get_env_variables,
)

__all__ = [
    "get_env_variables",
    "create_client",
    "create_logger_service",
    "create_write_logger_service",
]
