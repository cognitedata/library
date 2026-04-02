from cognite.client import CogniteClient
from cognite.extractorutils.uploader import RawUploadQueue

from config import ContextConfig

from get_resources import get_treed_asset_mappings, get_3d_model_id_and_revision_id
from write_resources import write_mapping_to_raw


def run_quality_check(client: CogniteClient, config: ContextConfig):
    raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500_000, trigger_log_level="INFO")

    model_id, revision_id = get_3d_model_id_and_revision_id(client, config, config.three_d_model_name)
    mappings = get_treed_asset_mappings(client, model_id=model_id, revision_id=revision_id)
    raw_table_good = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_good,
                                                        limit=-1)
    raw_table_bad = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_bad, limit=-1)
    raw_table_manual = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_manual,
                                                          limit=-1)

    new_mappings, old_mappings = compare_raw_to_real(client, mappings, raw_table_good, raw_table_bad)
    update_raw_tables(client, config, raw_uploader, new_mappings, old_mappings, raw_table_good, raw_table_bad,
                      raw_table_manual)


def compare_raw_to_real(client, mappings, raw_table_good, raw_table_bad):
    # Convert raw tables to lists of dictionaries for easier manipulation
    good_matches = raw_table_good.to_dict(orient='records')
    bad_matches = raw_table_bad.to_dict(orient='records')

    # Create a set of tuples for good and bad mappings
    good_mappings = {(entry['assetId'], entry['3DId']) for entry in good_matches}
    bad_mappings = {(entry['assetId'], entry['3DId']) for entry in bad_matches}

    # Flatten the mappings dictionary into a set of (assetId, nodeId) tuples
    all_mappings = {(asset_id, node_id) for asset_id, node_ids in mappings.items() for node_id in node_ids}

    # Find new mappings that are in the model but not in the good raw table
    new_mappings = {asset_id: node_id for asset_id, node_id in all_mappings if (asset_id, node_id) not in good_mappings}

    # Find old mappings that are in the good raw table but no longer in the model
    old_mappings = {asset_id: node_id for asset_id, node_id in good_mappings if (asset_id, node_id) not in all_mappings}

    return new_mappings, old_mappings



def update_raw_tables(client, config, raw_uploader, new_mappings, old_mappings, raw_table_good, raw_table_bad,
                      raw_table_manual):
    # Convert raw tables to lists of dictionaries
    good_matches = raw_table_good.to_dict(orient='records')
    bad_matches = raw_table_bad.to_dict(orient='records')
    manual_entries = raw_table_manual.to_dict(orient='records')

    # Add new mappings to the good matches and write to manual entries with manual_action: created
    for asset_id, node_id in new_mappings.items():
        new_entry = {'asset name': client.assets.retrieve(id=asset_id).name, 'assetId': asset_id, '3DId': node_id}
        manual_entries.append({**new_entry, 'manualAction': 'created'})
        # Remove the mapping from bad matches if it exists
        bad_matches = [entry for entry in bad_matches if not (entry['assetId'] == asset_id and entry['3DId'] == node_id)]

    # Remove old mappings from the good matches and write to manual entries with manual_action: deleted
    for asset_id, node_id in old_mappings.items():
        old_entry = next((entry for entry in good_matches if entry['assetId'] == asset_id and entry['3DId'] == node_id), None)
        if old_entry:
            manual_entries.append({**old_entry, 'manualAction': 'deleted'})
            good_matches = [entry for entry in good_matches if not (entry['assetId'] == asset_id and entry['3DId'] == node_id)]

    write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, manual_entries)

