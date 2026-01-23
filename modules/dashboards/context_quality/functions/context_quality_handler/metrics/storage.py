"""
File storage utilities for metrics persistence.
"""

import json
import logging

from cognite.client import CogniteClient


logger = logging.getLogger(__name__)


def save_metrics_to_file(
    client: CogniteClient,
    metrics: dict,
    file_external_id: str,
    file_name: str
):
    """Save metrics to Cognite Files as JSON, overwriting if exists."""
    import tempfile
    import os
    
    # Create temp file
    temp_path = os.path.join(tempfile.gettempdir(), file_name)
    
    with open(temp_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    
    # Upload with overwrite
    client.files.upload(
        path=temp_path,
        external_id=file_external_id,
        name=file_name,
        mime_type="application/json",
        overwrite=True
    )
    
    # Clean up temp file
    try:
        os.remove(temp_path)
    except Exception:
        pass
    
    logger.info(f"📁 Saved metrics to Cognite Files: {file_external_id}")
