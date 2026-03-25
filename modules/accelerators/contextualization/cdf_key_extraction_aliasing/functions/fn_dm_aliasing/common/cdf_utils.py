"""
CDF utility functions for RAW table operations.

This module provides shared utilities for working with CDF RAW tables,
including table creation and data upload operations.
"""

from typing import Any, Optional

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    CogniteClient = None

from .logger import CogniteFunctionLogger


def create_table_if_not_exists(
    client: CogniteClient,
    raw_db: str,
    tbl: str,
    logger: Optional[CogniteFunctionLogger] = None,
) -> None:
    """
    Create RAW database and table if they don't exist.

    Args:
        client: CogniteClient instance
        raw_db: RAW database name
        tbl: RAW table name
        logger: Optional logger instance for error logging
    """
    if not CDF_AVAILABLE or not client:
        if logger:
            logger.warning("CDF client not available, skipping table creation")
        return

    try:
        if raw_db not in client.raw.databases.list(limit=-1).as_names():
            client.raw.databases.create(raw_db)
    except Exception:
        pass

    try:
        if tbl not in client.raw.tables.list(raw_db, limit=-1).as_names():
            client.raw.tables.create(raw_db, tbl)
    except Exception:
        pass
