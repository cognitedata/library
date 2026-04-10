from typing import Any

from cognite.client import CogniteClient
from config import ContextConfig


def get_treed_asset_mappings(
    client: CogniteClient, model_id: int, revision_id: int
) -> dict[str, Any]:
    mappings = client.three_d.asset_mappings.list(model_id=model_id, revision_id=revision_id, limit=-1)
    existing_matches = {}
    for mapping in mappings.data:
        if mapping.asset_id in existing_matches:
            existing_matches[mapping.asset_id].append(mapping.node_id)
        else:
            existing_matches[mapping.asset_id] = [mapping.node_id]
    return existing_matches


def get_3d_model_id_and_revision_id(
    client: CogniteClient, config: ContextConfig, three_d_model_name: str
) -> tuple[int, int]:
    try:
        model_id_list = [
            model.id
            for model in client.three_d.models.list(published=True, limit=1)
            if model.name == three_d_model_name
        ]
        if not model_id_list:
            raise ValueError(f"3D model with name {three_d_model_name} not found")
        model_id = model_id_list[0]

        revision_list = client.three_d.revisions.list(model_id=model_id, published=True)
        if not revision_list:
            raise ValueError(f"3D model with name {three_d_model_name} has no published revisions")
        revision = revision_list[0]  # get latest revision

        print(f"INFO: For Model name: {three_d_model_name} using 3D model ID: {model_id} - revision ID: {revision.id}")
        print("If wrong model ID/revision remove other published versions of the model and try again")

        return model_id, revision.id

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get entities for 3D nodes in data set: {config.three_d_data_set_ext_id}- error: {e}"
        )