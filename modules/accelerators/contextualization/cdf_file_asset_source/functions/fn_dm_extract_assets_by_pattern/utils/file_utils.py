"""
File utility functions for extract annotation tags.

This module provides utilities for working with CDF files and CogniteFile instances.
"""

from typing import Any, Dict, List, Optional, Tuple

from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import FileReference
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.exceptions import CogniteAPIError

# Import common utilities from the old common.py
try:
    from modules.create_asset_hierarchy_from_files.common import (
        FILE_ID_PROPERTY_NAMES,
        extract_file_id_from_node,
        extract_uploaded_time_from_node,
    )
except ImportError:
    # Fallback if old module not available
    FILE_ID_PROPERTY_NAMES = ["id", "file"]

    def extract_file_id_from_node(node, property_names=None):
        return None

    def extract_uploaded_time_from_node(cognite_file_node):
        return None


def get_file_info_with_page_count(
    client: CogniteClient,
    file_obj,
    cognite_file_node=None,
    skip_page_count: bool = False,
) -> Optional[Dict[str, Any]]:
    """Get file information dictionary including page count from documents API.

    Args:
        client: CogniteClient instance
        file_obj: File object from CDF (should already contain uploaded_time)
        cognite_file_node: Optional CogniteFile data model node instance (deprecated, not used)
        skip_page_count: If True, skip page count retrieval (faster, defaults to 1)
    """
    # Get uploadedTime directly from file object (Files API already includes this)
    uploaded_time = None
    if hasattr(file_obj, "uploaded_time"):
        uploaded_time = file_obj.uploaded_time
    elif hasattr(file_obj, "uploadedTime"):
        uploaded_time = file_obj.uploadedTime

    # Convert to UTC ISO string if it's a datetime object
    if uploaded_time:
        from datetime import datetime, timezone

        if isinstance(uploaded_time, datetime):
            # Ensure datetime is timezone-aware and convert to UTC
            if uploaded_time.tzinfo is None:
                # Assume naive datetime is in UTC
                uploaded_time = uploaded_time.replace(tzinfo=timezone.utc)
            else:
                # Convert to UTC if it has timezone info
                uploaded_time = uploaded_time.astimezone(timezone.utc)
            uploaded_time = uploaded_time.isoformat()
        elif hasattr(uploaded_time, "isoformat"):
            # Handle other datetime-like objects
            uploaded_time = uploaded_time.isoformat()
        else:
            # Convert to string if not a datetime
            uploaded_time = str(uploaded_time)

    # Try to get page count - check file object first, then metadata, then Documents API
    page_count = None

    # First, check if page_count is directly on the file object
    if hasattr(file_obj, "page_count"):
        page_count = file_obj.page_count
    elif hasattr(file_obj, "pages"):
        page_count = len(file_obj.pages) if file_obj.pages else None

    # If not found, check metadata
    if page_count is None and hasattr(file_obj, "metadata") and file_obj.metadata:
        page_count = file_obj.metadata.get("page_count") or file_obj.metadata.get(
            "pages"
        )

    # If still not found and not skipping, try Documents API
    if page_count is None and not skip_page_count:
        try:
            # Use documents API to get document details including page count
            if hasattr(client, "documents"):
                document = client.documents.retrieve(id=file_obj.id)
                if hasattr(document, "page_count"):
                    page_count = document.page_count
                elif hasattr(document, "pages"):
                    page_count = len(document.pages) if document.pages else None
        except Exception:
            # Documents API might not be available or file might not be a document
            pass

    # If page count cannot be retrieved, default to 1
    if page_count is None:
        page_count = 1

    # Get MIME type from file object
    mime_type = file_obj.mime_type if hasattr(file_obj, "mime_type") else None

    return {
        "id": file_obj.id,
        "external_id": file_obj.external_id,
        "name": file_obj.name,
        "mime_type": mime_type,
        "uploadedTime": uploaded_time,
        "page_count": page_count,
    }


def get_files_from_space(
    client: CogniteClient,
    space: str = "cdf_cdm",
    view_external_id: str = "CogniteFile",
    view_version: str = "v1",
    filter_space: Optional[str] = None,
) -> Tuple[List[int], Dict[int, str]]:
    """
    Get file IDs from CogniteFile instances in a specific space.

    Args:
        client: CogniteClient instance
        space: Data model space to query (default: cdf_cdm)
        view_external_id: View external ID to query (default: CogniteFile)
        view_version: View version to query (default: v1)
        filter_space: Optional space to filter results by (only used for filtering, not for query)

    Returns a tuple of (list of file IDs, mapping of file_id to CogniteFile external_id).
    """
    file_ids = []
    file_id_to_revision_external_id = {}
    try:
        # Query nodes filtered by space and view
        view_id = ViewId(space, view_external_id, view_version)
        target_space = filter_space or space

        # Try optimized query first, fallback to full query if needed
        nodes = []
        try:
            if filter_space:
                # Query all nodes matching the view, then filter by filter_space
                all_nodes = list(
                    client.data_modeling.instances.list(
                        instance_type="node", sources=[view_id], limit=None
                    )
                )
                nodes = [n for n in all_nodes if n.space == filter_space]
            else:
                # Query nodes directly in the specified space
                nodes = list(
                    client.data_modeling.instances.list(
                        instance_type="node",
                        space=[space],
                        sources=[view_id],
                        limit=None,
                    )
                )
        except Exception:
            # Fallback: query all nodes and filter client-side
            all_nodes = list(
                client.data_modeling.instances.list(instance_type="node", limit=None)
            )
            nodes = [n for n in all_nodes if n.space == target_space]

        # Extract file IDs from CogniteFile instances using common utility
        for node in nodes:
            file_id = extract_file_id_from_node(node, FILE_ID_PROPERTY_NAMES)

            # Method 4: Query files API using instance_id (for files uploaded with upload_content)
            if file_id is None:
                try:
                    instance_node_id = NodeId(node.space, node.external_id)
                    files_list = client.files.retrieve_multiple(
                        instance_ids=[instance_node_id]
                    )
                    if files_list:
                        file_id = files_list[0].id
                except Exception:
                    pass

            # Method 5: Try to find file by name or alias from node properties
            if file_id is None:
                try:
                    # Get name from node properties
                    file_name = None
                    if hasattr(node, "data") and isinstance(node.data, dict):
                        for view_data in node.data.values():
                            if isinstance(view_data, dict) and "name" in view_data:
                                file_name = view_data["name"]
                                break
                    if (
                        not file_name
                        and hasattr(node, "properties")
                        and node.properties
                    ):
                        view_id_obj = ViewId(space, view_external_id, view_version)
                        view_props = node.properties.get(view_id_obj)
                        if isinstance(view_props, dict) and "name" in view_props:
                            file_name = view_props["name"]

                    if file_name:
                        # Try to find file by name
                        files_list = list(client.files.list(name=file_name, limit=1))
                        if files_list:
                            file_id = files_list[0].id
                except Exception:
                    pass

            if file_id:
                file_ids.append(file_id)
                if node.external_id:
                    file_id_to_revision_external_id[file_id] = node.external_id

        return file_ids, file_id_to_revision_external_id

    except Exception as e:
        raise Exception(f"Error querying CogniteFile instances: {e}")


def get_cognite_files(
    client: CogniteClient,
    limit: Optional[int] = None,
    mime_type: Optional[str] = None,
    instance_space: Optional[str] = None,
    skip_page_count: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get CogniteFiles from CDF using documents API to retrieve file details including page count.

    If instance_space is provided, only files referenced by CogniteFile instances in that space will be returned.
    The instance_space is only used to filter files, not to determine which data model to query.

    Args:
        client: CogniteClient instance
        limit: Optional limit on number of files to retrieve
        mime_type: Optional MIME type filter
        instance_space: Optional data model space to filter files by (only used for filtering, not for data model query)
    """
    try:
        # If instance_space is specified, get file IDs from CogniteFile instances first
        file_ids = None
        file_id_to_revision_external_id = {}
        if instance_space:
            file_ids, file_id_to_revision_external_id = get_files_from_space(
                client, filter_space=instance_space
            )
            if not file_ids:
                return []

        # Build query parameters
        query_params = {}
        if mime_type:
            query_params["mime_type"] = mime_type

        # List files
        if file_ids:
            # Retrieve files by ID using retrieve_multiple for efficiency
            try:
                files = list(client.files.retrieve_multiple(ids=file_ids))
            except Exception:
                # Fallback to individual retrieval if batch fails
                files = []
                for file_id in file_ids:
                    try:
                        file_obj = client.files.retrieve(id=file_id)
                        if file_obj:
                            files.append(file_obj)
                    except Exception:
                        pass
        else:
            # List all files
            if limit:
                files = list(client.files.list(limit=limit, **query_params))
            else:
                files = list(client.files.list(**query_params))

        # Convert to list of dicts for easier handling
        # Note: file objects from Files API already contain uploaded_time, no need for separate data modeling query
        file_list = []
        for file in files:
            file_info = get_file_info_with_page_count(
                client, file, None, skip_page_count=skip_page_count
            )
            if file_info:
                # Set external_id to CogniteFile external_id if we have the mapping
                if (
                    file_id_to_revision_external_id
                    and file_info["id"] in file_id_to_revision_external_id
                ):
                    file_info["external_id"] = file_id_to_revision_external_id[
                        file_info["id"]
                    ]
                file_list.append(file_info)

        return file_list

    except CogniteAPIError as e:
        raise Exception(f"Error retrieving files: {e}")


def chunk_file_into_page_blocks(
    file_info: Dict[str, Any], max_pages_per_chunk: int = 50
) -> List[FileReference]:
    """
    Chunk a file into page blocks of at most max_pages_per_chunk pages.

    Returns a list of FileReference objects for each page chunk.
    """
    file_id = file_info["id"]
    page_count = file_info.get("page_count", 1)

    file_refs = []
    current_page = 1

    while current_page <= page_count:
        last_page = min(current_page + max_pages_per_chunk - 1, page_count)
        file_ref = FileReference(
            file_id=file_id, first_page=current_page, last_page=last_page
        )
        file_refs.append(file_ref)
        current_page = last_page + 1

    return file_refs
