#!/usr/bin/env python3
"""
Common utilities for extract_assets_by_pattern scripts.

This module provides shared functionality to avoid code duplication
across multiple scripts in this directory.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

try:
    from dotenv import load_dotenv

    load_dotenv()  # Load .env file if it exists
except ImportError:
    # dotenv not available, try to load .env manually
    def load_dotenv():
        env_file = Path(".env")
        if env_file.exists():
            with open(env_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        os.environ[key.strip()] = value.strip().strip('"').strip("'")

    load_dotenv()

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling import ViewId

# Constants for file ID property names (native CogniteFile properties only)
FILE_ID_PROPERTY_NAMES = ["id", "file"]


def setup_cognite_client(client_name: str = "CDF-Script") -> CogniteClient:
    """Set up and return CogniteClient with OAuth credentials.

    Args:
        client_name: Name for the client (used in ClientConfig)

    Returns:
        Configured CogniteClient instance

    Raises:
        SystemExit: If required environment variables are missing
    """
    # Get environment variables
    cdf_project = os.getenv("CDF_PROJECT")
    cdf_cluster = os.getenv("CDF_CLUSTER")
    cdf_url = os.getenv("CDF_URL")
    client_id = os.getenv("IDP_CLIENT_ID")
    client_secret = os.getenv("IDP_CLIENT_SECRET")
    tenant_id = os.getenv("IDP_TENANT_ID")
    token_url = os.getenv("IDP_TOKEN_URL")

    if not all(
        [cdf_project, cdf_cluster, client_id, client_secret, tenant_id, token_url]
    ):
        print("❌ Missing required environment variables for CDF authentication")
        print(f"CDF_PROJECT: {cdf_project}")
        print(f"CDF_CLUSTER: {cdf_cluster}")
        print(f"IDP_CLIENT_ID: {client_id}")
        print(f"IDP_CLIENT_SECRET: {'***' if client_secret else None}")
        print(f"IDP_TENANT_ID: {tenant_id}")
        print(f"IDP_TOKEN_URL: {token_url}")
        sys.exit(1)

    # Create client configuration
    credentials = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"{cdf_url}/.default"],
    )

    client_config = ClientConfig(
        client_name=client_name,
        project=cdf_project,
        base_url=cdf_url,
        credentials=credentials,
    )

    return CogniteClient(client_config)


def extract_uploaded_time_from_node(cognite_file_node) -> Optional[str]:
    """Extract uploadedTime from CogniteFile data model node.

    Args:
        cognite_file_node: CogniteFile data model node instance

    Returns:
        ISO format datetime string, or None if not found
    """
    uploaded_time = None

    if not cognite_file_node:
        return None

    # Method 1: Check node.properties directly (primary method)
    if hasattr(cognite_file_node, "properties") and cognite_file_node.properties:
        try:
            view_id = ViewId("cdf_cdm", "CogniteFile", "v1")
            if hasattr(cognite_file_node.properties, "get"):
                props = cognite_file_node.properties.get(view_id)
                if props and isinstance(props, dict) and "uploadedTime" in props:
                    uploaded_time_value = props["uploadedTime"]
                    uploaded_time = _normalize_datetime_value(uploaded_time_value)
        except Exception:
            pass

        # Also try accessing as dict directly
        if not uploaded_time and isinstance(cognite_file_node.properties, dict):
            for view_props in cognite_file_node.properties.values():
                if isinstance(view_props, dict) and "uploadedTime" in view_props:
                    uploaded_time_value = view_props["uploadedTime"]
                    uploaded_time = _normalize_datetime_value(uploaded_time_value)
                    break

    # Method 2: Check node.data for uploadedTime property
    if (
        not uploaded_time
        and hasattr(cognite_file_node, "data")
        and isinstance(cognite_file_node.data, dict)
    ):
        for view_key, view_data in cognite_file_node.data.items():
            if isinstance(view_data, dict) and "uploadedTime" in view_data:
                uploaded_time_value = view_data["uploadedTime"]
                uploaded_time = _normalize_datetime_value(uploaded_time_value)
                break

    # Method 3: Check node.sources for uploadedTime
    if (
        not uploaded_time
        and hasattr(cognite_file_node, "sources")
        and cognite_file_node.sources
    ):
        for source in cognite_file_node.sources:
            if hasattr(source, "properties") and source.properties:
                if (
                    isinstance(source.properties, dict)
                    and "uploadedTime" in source.properties
                ):
                    uploaded_time_value = source.properties["uploadedTime"]
                    uploaded_time = _normalize_datetime_value(uploaded_time_value)
                    break

    return uploaded_time


def _normalize_datetime_value(value: Any) -> Optional[str]:
    """Normalize datetime value to ISO format string.

    Args:
        value: Datetime value in various formats (str, int, float, datetime object)

    Returns:
        ISO format datetime string, or None if conversion fails
    """
    if isinstance(value, str):
        return value
    elif isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
    elif hasattr(value, "isoformat"):
        return value.isoformat()
    return None


def extract_file_id_from_node(
    node, property_names: Optional[List[str]] = None
) -> Optional[int]:
    """Extract file ID from a CogniteFile node.

    Args:
        node: CogniteFile data model node instance
        property_names: List of property names to check (default: FILE_ID_PROPERTY_NAMES)

    Returns:
        File ID as integer, or None if not found
    """
    if property_names is None:
        property_names = FILE_ID_PROPERTY_NAMES

    file_id = None

    # Method 1: Check node.data (properties nested by view)
    if hasattr(node, "data") and isinstance(node.data, dict):
        for view_key, view_data in node.data.items():
            if isinstance(view_data, dict):
                file_id = _extract_file_id_from_properties(view_data, property_names)
                if file_id:
                    break

    # Method 2: Check node.properties directly
    if file_id is None and hasattr(node, "properties") and node.properties:
        try:
            view_id = ViewId("cdf_cdm", "CogniteFile", "v1")
            if hasattr(node.properties, "get"):
                view_props = node.properties.get(view_id)
                if isinstance(view_props, dict):
                    file_id = _extract_file_id_from_properties(
                        view_props, property_names
                    )

                # Try iterating through all view properties
                if file_id is None and hasattr(node.properties, "items"):
                    for prop_data in node.properties.values():
                        if isinstance(prop_data, dict):
                            file_id = _extract_file_id_from_properties(
                                prop_data, property_names
                            )
                            if file_id:
                                break
        except Exception:
            pass

    # Method 3: Check node.sources for properties
    if file_id is None and hasattr(node, "sources") and node.sources:
        for source in node.sources:
            if (
                hasattr(source, "properties")
                and source.properties
                and isinstance(source.properties, dict)
            ):
                file_id = _extract_file_id_from_properties(
                    source.properties, property_names
                )
                if file_id:
                    break

    return file_id


def _extract_file_id_from_properties(
    props_dict: dict, property_names: List[str]
) -> Optional[int]:
    """Extract file ID from a properties dictionary.

    Args:
        props_dict: Dictionary containing node properties
        property_names: List of property names to check in order

    Returns:
        File ID as integer, or None if not found
    """
    for prop_name in property_names:
        if prop_name in props_dict:
            value = props_dict[prop_name]
            try:
                # Case 1: Direct integer
                if isinstance(value, int):
                    return value
                # Case 2: DirectRelationReference object (typically not used for file property)
                if hasattr(value, "external_id") or hasattr(value, "id"):
                    # For DirectRelationReference, we need to resolve it
                    # But typically the file property stores the file ID directly
                    pass
                # Case 3: Dict with id or fileId
                if isinstance(value, dict):
                    if "id" in value:
                        return int(value["id"])
                    if "fileId" in value:
                        return int(value["fileId"])
                # Case 4: Try to convert to int
                return int(value)
            except (ValueError, TypeError, AttributeError):
                continue
    return None
