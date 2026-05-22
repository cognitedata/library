from cognite.client import CogniteClient
from cognite.extractorutils.uploader import RawUploadQueue

from config import ContextConfig

from get_resources import get_treed_asset_mappings
from write_resources import write_mapping_to_raw


def _get_asset_name(client: CogniteClient, config: ContextConfig, asset_ext_id: str) -> str:
    """Return the asset display name from DM, falling back to the external ID."""
    asset_space = config.asset_dm_space
    if not asset_space:
        return asset_ext_id
    try:
        from cognite.client.data_classes.data_modeling import NodeId
        result = client.data_modeling.instances.retrieve(
            nodes=[NodeId(asset_space, asset_ext_id)]
        )
        if result.nodes:
            node = result.nodes[0]
            # Try to read 'name' from any available view property
            for space_props in node.properties.values():
                for view_props in space_props.values():
                    name = view_props.get("name")
                    if name:
                        return str(name)
    except Exception:
        pass
    return asset_ext_id


def run_quality_check(client: CogniteClient, config: ContextConfig):
    raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500_000, trigger_log_level="INFO")

    mappings = get_treed_asset_mappings(client, config)
    raw_table_good = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_good,
                                                        limit=-1)
    raw_table_bad = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_bad, limit=-1)
    raw_table_manual = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_manual,
                                                          limit=-1)

    new_mappings, old_mappings = compare_raw_to_real(mappings, raw_table_good, raw_table_bad)
    update_raw_tables(client, config, raw_uploader, new_mappings, old_mappings, raw_table_good, raw_table_bad,
                      raw_table_manual)


def compare_raw_to_real(
    mappings: dict[str, list[int]],
    raw_table_good,
    raw_table_bad,
) -> tuple[set[tuple[str, int]], set[tuple[str, int]]]:
    """Compare the current model mappings to what's been persisted in RAW.

    Returns:
        new_mappings: (assetId, nodeId) pairs present in the model but not yet in the good table
        old_mappings: (assetId, nodeId) pairs present in the good table but no longer in the model
    """
    good_matches = raw_table_good.to_dict(orient="records")
    bad_matches = raw_table_bad.to_dict(orient="records")

    # Normalize types: RAW pandas reads may give ints/floats/strings inconsistently.
    def _norm(asset_id, node_id) -> tuple[str, int]:
        return str(asset_id), int(node_id)

    good_mappings = {_norm(e["assetId"], e["nodeId"]) for e in good_matches}
    _bad_mappings = {_norm(e["assetId"], e["nodeId"]) for e in bad_matches}

    all_mappings = {_norm(asset_id, node_id) for asset_id, node_ids in mappings.items() for node_id in node_ids}

    new_mappings = all_mappings - good_mappings
    old_mappings = good_mappings - all_mappings
    return new_mappings, old_mappings


def update_raw_tables(
    client,
    config,
    raw_uploader,
    new_mappings: set[tuple[str, int]],
    old_mappings: set[tuple[str, int]],
    raw_table_good,
    raw_table_bad,
    raw_table_manual,
) -> None:
    good_matches = raw_table_good.to_dict(orient="records")
    bad_matches = raw_table_bad.to_dict(orient="records")
    manual_entries = raw_table_manual.to_dict(orient="records")

    # New mappings: append to manual with manualAction=created, remove from bad if present
    for asset_id, node_id in new_mappings:
        new_entry = {
            "assetName": _get_asset_name(client, config, asset_id),
            "assetId": asset_id,
            "nodeId": node_id,
            "manualAction": "created",
        }
        manual_entries.append(new_entry)
        bad_matches = [
            e for e in bad_matches
            if not (str(e["assetId"]) == asset_id and int(e["nodeId"]) == node_id)
        ]

    # Old mappings: move good row to manual with manualAction=deleted
    for asset_id, node_id in old_mappings:
        old_entry = next(
            (e for e in good_matches
             if str(e["assetId"]) == asset_id and int(e["nodeId"]) == node_id),
            None,
        )
        if old_entry:
            manual_entries.append({**old_entry, "manualAction": "deleted"})
            good_matches = [
                e for e in good_matches
                if not (str(e["assetId"]) == asset_id and int(e["nodeId"]) == node_id)
            ]

    write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, manual_entries)

