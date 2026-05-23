"""
CDF Pipeline for Create Annotations

This module provides the main pipeline function that creates CogniteDiagramAnnotations
from diagram detection results stored in RAW.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from cognite.client import CogniteClient
    from cognite.client.data_classes import Row
    from cognite.client.data_classes.data_modeling import DirectRelationReference
    from cognite.client.data_classes.data_modeling.cdm.v1 import (
        CogniteDiagramAnnotationApply,
    )
    from cognite.client.data_classes.diagrams import (
        DiagramDetectItem,
        DiagramDetectResult,
    )

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    CogniteClient = None
    Row = None
    DirectRelationReference = None
    CogniteDiagramAnnotationApply = None
    DiagramDetectItem = None
    DiagramDetectResult = None

from .logger import CogniteFunctionLogger

logger = None  # Use CogniteFunctionLogger directly


class MockBoundingBox:
    """Mock bounding box class to mimic DiagramDetectItem.bounding_box structure."""

    def __init__(self, x_min: float, x_max: float, y_min: float, y_max: float):
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max


class MockDiagramDetectItem:
    """Mock DiagramDetectItem class to convert JSON data to DiagramDetectItem-like structure."""

    def __init__(self, ann_data: Dict[str, Any], page_number: int = 1):
        self.text = ann_data.get("text", "")
        self.confidence = ann_data.get("confidence", 0.0)
        self.page_number = page_number
        # Store the full original annotation data to preserve all entity information
        self.original_data = ann_data

        # Extract bounding box from region
        region = ann_data.get("region", {})
        if not isinstance(region, dict):
            region = {}

        # Extract coordinates from vertices
        vertices = region.get("vertices", [])
        if vertices and isinstance(vertices, list) and len(vertices) > 0:
            x_coords = []
            y_coords = []
            for vertex in vertices:
                if isinstance(vertex, dict):
                    if "x" in vertex:
                        x_coords.append(float(vertex["x"]))
                    if "y" in vertex:
                        y_coords.append(float(vertex["y"]))

            if x_coords and y_coords:
                x_min = min(x_coords)
                x_max = max(x_coords)
                y_min = min(y_coords)
                y_max = max(y_coords)
                self.bounding_box = MockBoundingBox(x_min, x_max, y_min, y_max)
            else:
                # Default bounding box
                self.bounding_box = MockBoundingBox(0.0, 0.1, 0.0, 0.1)
        else:
            # Fallback to x, y, width, height if available
            x = region.get("x", 0.0)
            y = region.get("y", 0.0)
            width = region.get("width", 0.1)
            height = region.get("height", 0.1)
            self.bounding_box = MockBoundingBox(
                float(x), float(x + width), float(y), float(y + height)
            )


def convert_to_annotations(
    detect_results: List[MockDiagramDetectItem],
    file_external_id: str,
    file_id: Optional[int] = None,
    asset_external_ids: Optional[List[str]] = None,
    space: str = "sp_enterprise_schema",
    logger: Optional[CogniteFunctionLogger] = None,
) -> List[CogniteDiagramAnnotationApply]:
    """
    Convert diagram detection results to CogniteDiagramAnnotationApply instances.

    Based on the example code pattern.

    Args:
        detect_results: List of DiagramDetectItem-like objects
        file_external_id: File external ID for file relation
        file_id: Numeric file ID (optional, for RAW storage)
        asset_external_ids: List of asset external IDs for asset relations (optional)
        space: Data model space
        logger: Logger instance for error reporting

    Returns:
        List of CogniteDiagramAnnotationApply instances
    """
    log = logger or CogniteFunctionLogger()

    # Try to import if module-level imports failed
    annotation_class = CogniteDiagramAnnotationApply
    relation_class = DirectRelationReference

    if annotation_class is None:
        try:
            from cognite.client.data_classes.data_modeling.cdm.v1 import (
                CogniteDiagramAnnotationApply as CDMAnnotation,
            )

            annotation_class = CDMAnnotation
        except ImportError as e:
            log.error(f"CogniteDiagramAnnotationApply import failed: {e}")
            return []

    if relation_class is None:
        try:
            from cognite.client.data_classes.data_modeling import (
                DirectRelationReference as DRR,
            )

            relation_class = DRR
        except ImportError as e:
            log.error(f"DirectRelationReference import failed: {e}")
            return []

    annotations = []

    for i, item in enumerate(detect_results):
        try:
            # Generate unique external ID
            ann_ext_id = f"ann-{file_external_id}-{i}"

            # Extract bounding box (normalized)
            bbox = item.bounding_box
            page = item.page_number or 1

            # Create file reference
            file_ref = relation_class(space=space, external_id=file_external_id)

            # Base annotation (start node)
            ann = annotation_class(
                space=space,
                external_id=ann_ext_id,
                type=(space, ann_ext_id),  # Required: type reference
                start_node=file_ref,  # Required: start_node
                end_node=file_ref,  # Required: end_node
                name=f"Annotation {i} on {file_external_id}",
                description=f"Detected text: '{item.text}' on page {page}",
                confidence=item.confidence or 0.0,
                status="Suggested",
                start_node_page_number=page,
                start_node_x_min=bbox.x_min,
                start_node_x_max=bbox.x_max,
                start_node_y_min=bbox.y_min,
                start_node_y_max=bbox.y_max,
                start_node_text=item.text or "",
            )

            # Optional: Link to the source file via startNode relation
            ann.start_node = file_ref  # Direct relation to CogniteFile node

            # Store file_id as metadata if provided (for RAW storage)
            if file_id is not None:
                # Store file_id in a way that can be retrieved later
                # We'll use a custom attribute that we can access when saving to RAW
                ann._file_id = file_id  # Store as private attribute for RAW saving

            # Store annotation external ID for relation creation
            ann._annotation_external_id = ann_ext_id

            # Store the full original annotation data (including all entity information)
            if hasattr(item, "original_data"):
                ann._original_annotation_data = item.original_data

            # Create relations to matched assets and files from the diagram detection results
            relations = []

            # Extract entities from original annotation data and create relations
            # Entities in the original data are already matched by diagram detection
            if hasattr(item, "original_data") and item.original_data:
                entities = item.original_data.get("entities", [])
                if entities and isinstance(entities, list):
                    for entity in entities:
                        if isinstance(entity, dict):
                            # Get asset external ID from entity
                            asset_ext_id = entity.get("external_id") or entity.get(
                                "externalId"
                            )
                            if asset_ext_id:
                                # Create relation to the asset
                                # The entity is already matched by diagram detection, so we create the relation
                                # Also check if annotation text matches asset (case-insensitive) as per example pattern
                                # This matches: if item.text and item.text.strip().upper() in asset_external_id.upper()
                                if (
                                    item.text
                                    and item.text.strip().upper()
                                    in asset_ext_id.upper()
                                ):
                                    asset_ref = relation_class(
                                        space=space, external_id=asset_ext_id
                                    )
                                    relations.append(asset_ref)
                                    log.debug(
                                        f"Created relation from annotation {ann_ext_id} to asset {asset_ext_id}"
                                    )

            # Also check the passed asset_external_ids list for any additional matches
            if asset_external_ids:
                for asset_ext_id in asset_external_ids:
                    # Check if annotation text matches asset (case-insensitive)
                    # This matches the pattern: if item.text and item.text.strip().upper() in asset_external_id.upper()
                    if item.text and item.text.strip().upper() in asset_ext_id.upper():
                        # Check if we already added this relation
                        existing_relation = any(
                            (
                                isinstance(rel, tuple)
                                and len(rel) == 2
                                and rel[1] == asset_ext_id
                            )
                            or (
                                hasattr(rel, "external_id")
                                and rel.external_id == asset_ext_id
                            )
                            for rel in relations
                        )
                        if not existing_relation:
                            asset_ref = relation_class(
                                space=space, external_id=asset_ext_id
                            )
                            relations.append(asset_ref)
                            log.debug(
                                f"Created relation from annotation {ann_ext_id} to asset {asset_ext_id}"
                            )

            # Set relations if any were created
            # This matches the pattern: ann.relations = [asset_ref]
            if relations:
                ann.relations = relations

            annotations.append(ann)
        except Exception as e:
            log.warning(f"Error creating annotation {i}: {e}")
            import traceback

            log.debug(traceback.format_exc())
            continue

    return annotations


def _save_annotations_to_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_annotations: str,
    annotations: List[CogniteDiagramAnnotationApply],
    logger: Optional[CogniteFunctionLogger] = None,
) -> None:
    """Save annotations to RAW table."""
    from cognite.client.exceptions import CogniteAPIError

    log = logger or CogniteFunctionLogger()

    # Ensure table exists
    try:
        tables = client.raw.tables.list(raw_db, limit=-1)
        table_names = [tbl.name for tbl in tables]
        if raw_table_annotations not in table_names:
            client.raw.tables.create(raw_db, raw_table_annotations)
            log.info(f"Created RAW table: {raw_db}.{raw_table_annotations}")
    except Exception as e:
        log.warning(f"Error ensuring table exists: {e}")

    # Get Row class
    RowClass = Row
    if RowClass is None:
        try:
            from cognite.client.data_classes import Row as RowClass
        except ImportError:
            log.error("Row class is not available (import failed)")
            return

    rows = []
    for annotation in annotations:
        try:
            external_id = annotation.external_id
            if not external_id:
                continue

            # Get the full annotation dump (complete JSON representation)
            try:
                annotation_dump = annotation.dump()
            except Exception as e:
                log.warning(
                    f"Could not dump annotation {external_id}: {e}, trying alternative method"
                )
                # Fallback: try to get dict representation
                if hasattr(annotation, "__dict__"):
                    annotation_dump = annotation.__dict__
                else:
                    log.error(f"Cannot serialize annotation {external_id}")
                    continue

            # Store the full annotation as JSON
            columns = {
                "external_id": external_id,  # Keep external_id as a separate column for easy querying
                "annotation": json.dumps(
                    annotation_dump, default=str
                ),  # Full annotation dump
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            # Also store private attributes if they exist (for convenience)
            if hasattr(annotation, "_file_id"):
                columns["_file_id"] = annotation._file_id
            if hasattr(annotation, "_annotation_external_id"):
                columns["_annotation_external_id"] = annotation._annotation_external_id

            # Store the original annotation data from results (includes full entity information)
            if hasattr(annotation, "_original_annotation_data"):
                columns["original_annotation_data"] = json.dumps(
                    annotation._original_annotation_data, default=str
                )

            row = RowClass(key=external_id, columns=columns)
            rows.append(row)
        except Exception as e:
            log.warning(
                f"Error preparing annotation {annotation.external_id if annotation else 'unknown'} for RAW: {e}"
            )

    # Insert in batches
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            client.raw.rows.insert(
                db_name=raw_db, table_name=raw_table_annotations, row=batch
            )
            log.debug(f"Saved batch of {len(batch)} annotation(s) to RAW")
        except CogniteAPIError as e:
            log.error(f"Error saving batch to RAW: {e}")
            raise

    log.info(
        f"Saved {len(rows)} annotation(s) to RAW table {raw_db}.{raw_table_annotations}"
    )


def create_annotations(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
) -> None:
    """
    Main pipeline function for create annotations.

    Reads results from diagram detection results RAW table, converts them to
    CogniteDiagramAnnotationApply instances, and saves to annotations RAW table.

    Args:
        client: CogniteClient instance (required)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing pipeline parameters
    """
    try:
        logger.info("Starting Create Annotations Pipeline")

        if client is None:
            raise ValueError("CogniteClient is required")

        # Extract parameters from data
        cdf_config = data.get("_cdf_config")
        if cdf_config is None:
            raise ValueError("CDF config is required")

        space = data.get("space", "sp_enterprise_schema")
        raw_db = cdf_config.parameters.raw_db
        raw_table_results = cdf_config.parameters.raw_table_results
        raw_table_annotations = cdf_config.parameters.raw_table_annotations

        logger.info(f"Loading results from RAW table {raw_db}.{raw_table_results}")

        # Load results from RAW table
        try:
            rows = client.raw.rows.list(raw_db, raw_table_results, limit=-1).to_pandas()
        except Exception as e:
            logger.error(f"Error loading results from RAW: {e}")
            raise

        if rows.empty:
            logger.warning(
                f"No results found in RAW table {raw_db}.{raw_table_results}"
            )
            data["annotations"] = []
            return

        logger.info(f"Found {len(rows)} file(s) with results")

        all_annotations = []

        # Process each file's results
        for key, row in rows.iterrows():
            try:
                # Parse results data
                results_json = row.get("results", "{}")
                if isinstance(results_json, str):
                    results_data = json.loads(results_json)
                else:
                    results_data = (
                        results_json if isinstance(results_json, dict) else {}
                    )

                # Extract items from results
                items_data = results_data.get("items", [])
                if not items_data:
                    logger.debug(f"No items found for row {key}")
                    continue

                logger.debug(f"Processing {len(items_data)} item(s) for row {key}")

                # Process each item (page)
                for item_data in items_data:
                    # Extract file_id (numeric) from fileId
                    file_id = item_data.get("fileId")
                    if not file_id:
                        logger.warning(
                            f"Could not determine fileId for item in row {key}, skipping"
                        )
                        continue

                    # Extract file_external_id from fileInstanceId
                    file_instance_id_obj = item_data.get("fileInstanceId")
                    if isinstance(file_instance_id_obj, dict):
                        # fileInstanceId is a dict with {'externalId': '...', 'space': '...'}
                        file_external_id = file_instance_id_obj.get("externalId")
                    elif isinstance(file_instance_id_obj, str):
                        # fileInstanceId is a string
                        file_external_id = file_instance_id_obj
                    else:
                        # Fallback to fileExternalId if fileInstanceId not available
                        file_external_id = item_data.get("fileExternalId")
                        if not file_external_id:
                            # Last resort: construct from file_id
                            file_external_id = f"cognitefile_{file_id}"
                            logger.warning(
                                f"Using fallback file_external_id for fileId {file_id}"
                            )

                    if not file_external_id:
                        logger.warning(
                            f"Could not determine fileInstanceId for item in row {key}, skipping"
                        )
                        continue

                    page_number = item_data.get("pageNumber", item_data.get("page", 1))
                    item_annotations = item_data.get("annotations", [])

                    if not item_annotations:
                        logger.debug(
                            f"No annotations in item for file {file_external_id} (fileId: {file_id}), page {page_number}"
                        )
                        continue

                    logger.debug(
                        f"Processing {len(item_annotations)} annotation(s) for file {file_external_id} (fileId: {file_id}), page {page_number}"
                    )

                    # Convert annotation data to MockDiagramDetectItem objects
                    detect_results = []
                    all_asset_external_ids = (
                        set()
                    )  # Collect all unique asset external IDs

                    for ann_data in item_annotations:
                        try:
                            # Create mock DiagramDetectItem
                            mock_item = MockDiagramDetectItem(ann_data, page_number)
                            detect_results.append(mock_item)

                            # Extract all asset external IDs from entities
                            entities = ann_data.get("entities", [])
                            if entities and isinstance(entities, list):
                                for entity in entities:
                                    if isinstance(entity, dict):
                                        asset_ext_id = entity.get(
                                            "external_id"
                                        ) or entity.get("externalId")
                                        if asset_ext_id:
                                            all_asset_external_ids.add(asset_ext_id)
                        except Exception as e:
                            logger.warning(f"Error creating MockDiagramDetectItem: {e}")
                            continue

                    # Convert to annotations using fileInstanceId and fileId
                    if detect_results:
                        try:
                            file_annotations = convert_to_annotations(
                                detect_results=detect_results,
                                file_external_id=file_external_id,  # Use fileInstanceId.externalId from item
                                file_id=file_id,  # Use fileId (numeric) from item
                                asset_external_ids=list(all_asset_external_ids)
                                if all_asset_external_ids
                                else None,
                                space=space,
                                logger=logger,
                            )
                            all_annotations.extend(file_annotations)
                            if len(file_annotations) > 0:
                                logger.debug(
                                    f"Created {len(file_annotations)} annotation(s) for file {file_external_id} (fileId: {file_id})"
                                )
                            else:
                                logger.debug(
                                    f"No annotations created for file {file_external_id} (check convert_to_annotations)"
                                )
                        except Exception as e:
                            logger.error(
                                f"Error converting to annotations for file {file_external_id}: {e}"
                            )
                            import traceback

                            logger.debug(traceback.format_exc())

            except Exception as e:
                logger.warning(f"Error processing row {key}: {e}")
                continue

        logger.info(
            f"Created {len(all_annotations)} CogniteDiagramAnnotation instance(s)"
        )

        # Log full annotation instances before saving to RAW
        if all_annotations:
            logger.info("=" * 80)
            logger.info(
                "FULL CogniteDiagramAnnotation INSTANCES (before saving to RAW):"
            )
            logger.info("=" * 80)
            for i, ann in enumerate(all_annotations):
                logger.info(f"\n--- Annotation {i+1}/{len(all_annotations)} ---")
                logger.info(f"External ID: {ann.external_id}")
                logger.info(f"Space: {ann.space}")
                logger.info(f"Type: {ann.type}")
                logger.info(f"Name: {getattr(ann, 'name', 'N/A')}")
                logger.info(f"Description: {getattr(ann, 'description', 'N/A')}")
                logger.info(f"Confidence: {getattr(ann, 'confidence', 'N/A')}")
                logger.info(f"Status: {getattr(ann, 'status', 'N/A')}")
                logger.info(f"Start Node: {ann.start_node}")
                logger.info(f"End Node: {ann.end_node}")
                logger.info(
                    f"Start Node Page Number: {getattr(ann, 'start_node_page_number', 'N/A')}"
                )
                logger.info(
                    f"Start Node X Min: {getattr(ann, 'start_node_x_min', 'N/A')}"
                )
                logger.info(
                    f"Start Node X Max: {getattr(ann, 'start_node_x_max', 'N/A')}"
                )
                logger.info(
                    f"Start Node Y Min: {getattr(ann, 'start_node_y_min', 'N/A')}"
                )
                logger.info(
                    f"Start Node Y Max: {getattr(ann, 'start_node_y_max', 'N/A')}"
                )
                logger.info(
                    f"Start Node Text: {getattr(ann, 'start_node_text', 'N/A')}"
                )
                logger.info(f"Relations: {getattr(ann, 'relations', [])}")
                logger.info(f"File ID (stored): {getattr(ann, '_file_id', 'N/A')}")
                logger.info(
                    f"Annotation External ID (stored): {getattr(ann, '_annotation_external_id', 'N/A')}"
                )
                # Print all attributes
                logger.info(f"All attributes: {dir(ann)}")
                # Try to get dict representation if available
                try:
                    if hasattr(ann, "dump"):
                        ann_dict = ann.dump()
                        logger.info(
                            f"Dump (dict): {json.dumps(ann_dict, indent=2, default=str)}"
                        )
                    elif hasattr(ann, "__dict__"):
                        logger.info(
                            f"__dict__: {json.dumps(ann.__dict__, indent=2, default=str)}"
                        )
                except Exception as e:
                    logger.debug(f"Could not serialize annotation to dict: {e}")
            logger.info("=" * 80)

        # Save to RAW table
        if all_annotations:
            logger.info(
                f"Saving {len(all_annotations)} annotation(s) to RAW table {raw_db}.{raw_table_annotations}"
            )
            _save_annotations_to_raw(
                client=client,
                raw_db=raw_db,
                raw_table_annotations=raw_table_annotations,
                annotations=all_annotations,
                logger=logger,
            )

        data["annotations"] = all_annotations
        logger.info("Create Annotations Pipeline completed successfully")

    except Exception as e:
        error_msg = f"Create annotations pipeline failed: {str(e)}"
        logger.error(error_msg)
        raise
