import random
import traceback
from datetime import timedelta
from typing import List

import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ExtractionPipelineConfig, TimeSeriesUpdate
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeId, ViewId
from cognite.client.exceptions import CogniteAPIError
from services.LoggerService import CompactLogger
from utils.DataStructures import OIDConfig

# OID publishes data with ~1 week delay (168 hours)
OID_PUBLICATION_DELAY_HOURS = 168


class OIDPublicClient:
    """Manages connection to Open Industrial Data project"""
    
    def __init__(self, config: OIDConfig, logger: CompactLogger):
        self.config = config
        self.logger = logger
        self.client = self._create_client()
    
    def _create_client(self) -> CogniteClient:
        """Create authenticated client for Open Industrial Data"""
        scopes = [f"https://{self.config.oid_cluster}.cognitedata.com/.default"]
        token_url = f"https://login.microsoftonline.com/{self.config.oid_tenant_id}/oauth2/v2.0/token"
        base_url = f"https://{self.config.oid_cluster}.cognitedata.com"
        
        creds = OAuthClientCredentials(
            token_url=token_url,
            client_id=self.config.oid_client_id,
            scopes=scopes,
            client_secret=self.config.oid_client_secret
        )
        
        client_config = ClientConfig(
            client_name="OID-Reader",
            project=self.config.oid_project,
            credentials=creds,
            base_url=base_url,
            timeout=60
        )
        
        return CogniteClient(client_config)


class DataSyncService:
    """Service for synchronizing data from Open Industrial Data"""
    
    def __init__(
        self, target_client: CogniteClient, oid_client: OIDPublicClient, config: OIDConfig, logger: CompactLogger):
        self.target_client = target_client
        self.oid_client = oid_client
        self.config = config
        self.logger = logger
        # View ID for TimeSeries (configurable via extraction pipeline config)
        self.timeseries_view_id = ViewId(
            space=config.timeseries_view_space, 
            external_id=config.timeseries_view_external_id, 
            version=config.timeseries_view_version
        )
        # Cache for string time series to avoid repeated lookups
        self._string_ts_cache = {}
    
    def _get_time_offset_ms(self) -> float:
        """Calculate time offset in milliseconds"""
        return (
            timedelta(weeks=self.config.time_offset_weeks).total_seconds() * 1000 +
            self.config.time_offset_minutes * 60 * 1000
        )
    
    def _prepare_datapoints_for_insert(self, dps_list, offset_ms: float) -> list:
        """
        Prepare datapoints with time offset, handling string/numeric types.
        
        Returns list of dicts ready for insert_multiple()
        """
        datapoints_to_insert = []
        
        for dps in dps_list:
            if len(dps) == 0:
                continue
            
            # Check if any values are non-numeric
            has_string_values = any(
                not isinstance(dp.value, (int, float)) or dp.value is None
                for dp in dps
            )
            
            if has_string_values:
                is_string_ts = self.check_and_update_string_timeseries(dps.external_id)
                
                if is_string_ts:
                    prepared_dps = [
                        {"timestamp": dp.timestamp + offset_ms, "value": dp.value}
                        for dp in dps
                    ]
                else:
                    # Filter to only numeric values
                    prepared_dps = [
                        {"timestamp": dp.timestamp + offset_ms, "value": dp.value}
                        for dp in dps
                        if isinstance(dp.value, (int, float)) and dp.value is not None
                    ]
            else:
                prepared_dps = [
                    {"timestamp": dp.timestamp + offset_ms, "value": dp.value}
                    for dp in dps
                ]
            
            if prepared_dps:
                datapoints_to_insert.append({
                    "instance_id": NodeId(self.config.instance_space, external_id=dps.external_id),
                    "datapoints": prepared_dps
                })
        
        return datapoints_to_insert
    
    def get_pi_time_series(self) -> List[str]:
        """Get all time series with 'pi:' prefix from OID"""
        self.logger.info("Fetching PI time series list from OID...")
        
        try:
            ts_list = self.oid_client.client.time_series.list(
                limit=None,
                advanced_filter={
                    "prefix": {
                        "property": ["externalId"],
                        "value": "pi:"
                    }
                }
            )
            ext_ids = ts_list.as_external_ids()
            self.logger.info(f"✓ Found {len(ext_ids)} PI time series")
            return ext_ids
        except CogniteAPIError as e:
            self.logger.error(f"Failed to fetch time series: {str(e)}")
            return []
    
    def check_and_update_string_timeseries(self, external_id: str) -> bool:
        """
        Check if a time series in OID is string-based and update target if needed
        
        Args:
            external_id: Time series external ID
            
        Returns:
            True if time series is/should be string-based, False otherwise
        """
        # Check cache first
        if external_id in self._string_ts_cache:
            return self._string_ts_cache[external_id]
        
        try:
            # Check source time series in OID
            oid_ts = self.oid_client.client.time_series.retrieve(external_id=external_id)
            
            if oid_ts and oid_ts.is_string:
                self.logger.info(f"Detected string time series: {external_id}")
                
                # Update target time series to support strings
                try:
                    target_ts = self.target_client.time_series.retrieve(external_id=external_id)
                    
                    if target_ts and not target_ts.is_string:
                        # Update to string-based time series
                        self.target_client.time_series.update(
                            TimeSeriesUpdate(external_id=external_id).is_string.set(True)
                        )
                        self.logger.info(f"✓ Updated {external_id} to support string values")
                    
                except Exception as e:
                    self.logger.warning(f"Could not update {external_id} to string: {str(e)}")
                
                self._string_ts_cache[external_id] = True
                return True
            else:
                self._string_ts_cache[external_id] = False
                return False
                
        except Exception as e:
            self.logger.warning(f"Could not check string status for {external_id}: {str(e)}")
            self._string_ts_cache[external_id] = False
            return False
    
    def get_unbackfilled_time_series(self, all_ts_ids: List[str]) -> List[str]:
        """Get time series that haven't been fully backfilled yet (no 'oid_backfilled' tag in Data Model)"""
        try:
            # Retrieve time series nodes from Data Model to check tags
            node_ids = [NodeId(space=self.config.instance_space, external_id=ts_id) 
                       for ts_id in all_ts_ids]
            
            ts_nodes = self.target_client.data_modeling.instances.retrieve_nodes(
                nodes=node_ids, 
                sources=self.timeseries_view_id
            )
            
            # Build a set of time series that ARE backfilled (have 'oid_backfilled' tag)
            backfilled_ids = set()
            for node in ts_nodes:
                if node and node.properties and self.timeseries_view_id in node.properties:
                    tags = node.properties[self.timeseries_view_id].get("tags", [])
                    if tags and "oid_backfilled" in tags:
                        backfilled_ids.add(node.external_id)
            
            # Return all time series that are NOT in the backfilled set
            # This includes time series that don't exist yet in target project
            unbackfilled = [ts_id for ts_id in all_ts_ids if ts_id not in backfilled_ids]
            
            return unbackfilled
            
        except Exception as e:
            self.logger.warning(f"Error checking backfill status, using all: {str(e)}")
            return all_ts_ids
    
    def mark_as_backfilled(self, external_id: str):
        """Mark a time series as fully backfilled using Data Model tags (similar to file annotation pattern)"""
        try:
            # Retrieve the time series node from Data Model
            node_id = NodeId(space=self.config.instance_space, external_id=external_id)
            result = self.target_client.data_modeling.instances.retrieve_nodes(
                nodes=node_id, sources=self.timeseries_view_id
            )
            
            # Handle the result - could be a single Node or a NodeList
            if not result:
                self.logger.warning(f"No time series node found for {external_id}, cannot mark as backfilled")
                return
            
            # If result is a list/NodeList, get the first item
            ts_node = result if isinstance(result, Node) else (result[0] if len(result) > 0 else None)
            
            if not ts_node:
                self.logger.warning(f"Time series node {external_id} is empty, cannot mark as backfilled")
                return
            
            # Convert to writable node and update tags
            node_apply: NodeApply = ts_node.as_write()
            node_apply.existing_version = None
            
            # Get current tags from the node properties
            if not node_apply.sources or len(node_apply.sources) == 0:
                self.logger.warning(f"No sources found for {external_id}, cannot mark as backfilled")
                return
                
            tags_property: list[str] = node_apply.sources[0].properties.get("tags", [])
            
            # Ensure tags_property is a list
            if tags_property is None:
                tags_property = []
            
            # Add 'oid_backfilled' tag if not already present
            if "oid_backfilled" not in tags_property:
                tags_property.append("oid_backfilled")
                node_apply.sources[0].properties["tags"] = tags_property
                
                # Apply the updated node to Data Model
                self.target_client.data_modeling.instances.apply(
                    nodes=node_apply,
                    replace=False
                )
                self.logger.info(f"✓ Marked {external_id} as backfilled")
            else:
                self.logger.info(f"{external_id} already marked as backfilled")
                
        except Exception as e:
            self.logger.warning(f"Could not mark {external_id} as backfilled: {str(e)}")
            self.logger.warning(f"Traceback: {traceback.format_exc()}")
    
    def reset_backfill_tags(self, all_ts_ids: List[str]):
        """Remove 'oid_backfilled' tag from all time series to restart backfill process"""
        try:
            self.logger.info("=" * 50)
            self.logger.info("RESET BACKFILL: Removing all 'oid_backfilled' tags")
            self.logger.info("=" * 50)
            
            # Retrieve all time series nodes
            node_ids = [NodeId(space=self.config.instance_space, external_id=ts_id) 
                       for ts_id in all_ts_ids]
            
            ts_nodes = self.target_client.data_modeling.instances.retrieve_nodes(
                nodes=node_ids, 
                sources=self.timeseries_view_id
            )
            
            # Find nodes with oid_backfilled tag
            nodes_to_update = []
            for node in ts_nodes:
                if node and node.properties and self.timeseries_view_id in node.properties:
                    tags = node.properties[self.timeseries_view_id].get("tags", [])
                    if tags and "oid_backfilled" in tags:
                        # Convert to writable node and remove the tag
                        node_apply: NodeApply = node.as_write()
                        node_apply.existing_version = None
                        
                        # Remove oid_backfilled tag but keep all others
                        updated_tags = [tag for tag in tags if tag != "oid_backfilled"]
                        node_apply.sources[0].properties["tags"] = updated_tags
                        nodes_to_update.append(node_apply)
            
            if nodes_to_update:
                self.logger.info(f"Removing 'oid_backfilled' tag from {len(nodes_to_update)} time series")
                # Apply updates in batches of 100 to avoid overwhelming the API
                batch_size = 100
                for i in range(0, len(nodes_to_update), batch_size):
                    batch = nodes_to_update[i:i+batch_size]
                    self.target_client.data_modeling.instances.apply(
                        nodes=batch,
                        replace=False
                    )
                self.logger.info(f"✓ Reset complete - {len(nodes_to_update)} time series ready for backfill")
            else:
                self.logger.info("No time series had 'oid_backfilled' tag - nothing to reset")
            
        except Exception as e:
            self.logger.warning(f"Error resetting backfill tags: {str(e)}")
            self.logger.warning(f"Traceback: {traceback.format_exc()}")
    
    def update_config_reset_backfill(self):
        """Update extraction pipeline config to set reset_backfill back to false"""
        try:
            # Retrieve current config
            pipeline_config = self.target_client.extraction_pipelines.config.retrieve("ep_oid_sync")
            
            if pipeline_config and pipeline_config.config:
                # Parse the YAML config
                config_data = yaml.safe_load(pipeline_config.config)
                
                # Update reset_backfill to false
                if "sync_configuration" in config_data:
                    config_data["sync_configuration"]["reset_backfill"] = False
                    
                    # Convert back to YAML string
                    updated_config_str = yaml.dump(config_data, default_flow_style=False, sort_keys=False)
                    
                    # Create new config object and update
                    new_config = ExtractionPipelineConfig(
                        external_id="ep_oid_sync",
                        config=updated_config_str
                    )
                    self.target_client.extraction_pipelines.config.create(new_config)
                    
                    self.logger.info("✓ Updated config: reset_backfill set to false")
                else:
                    self.logger.warning("Could not find sync_configuration in config")
            else:
                self.logger.warning("Could not retrieve extraction pipeline config")
                
        except Exception as e:
            self.logger.warning(f"Error updating config: {str(e)}")
    
    def sync_time_series_data(
        self, external_ids: List[str], start_time: str, description: str = "", end_time: str | None = None
    ) -> int:
        """
        Sync data from OID to target project
        
        Args:
            external_ids: List of time series external IDs
            start_time: Start time for data retrieval (e.g., "15m-ago")
            description: Description for logging
            end_time: Optional end time (if None, uses "now" for OID boundary)
            
        Returns:
            Total datapoints inserted
        """
        if not external_ids:
            self.logger.warning("No time series to sync")
            return 0
        
        # For real-time sync: OID has publication delay (see OID_PUBLICATION_DELAY_HOURS)
        # So "15m-ago" in real-time means we fetch from "168h15m-ago" to "168h-ago" in OID
        # This gets the most recent 15 minutes of available data
        if end_time is None:
            # Compute the OID-adjusted time range
            if "m-ago" in start_time:
                minutes = int(start_time.split("m-ago")[0])
                oid_start = f"{OID_PUBLICATION_DELAY_HOURS * 60 + minutes}m-ago"
                oid_end = self.config.sync_end
            elif "h-ago" in start_time:
                hours = int(start_time.split("h-ago")[0])
                oid_start = f"{OID_PUBLICATION_DELAY_HOURS + hours}h-ago"
                oid_end = self.config.sync_end
            else:
                oid_start = start_time
                oid_end = self.config.sync_end
        else:
            # Backfill mode: use provided times directly
            oid_start = start_time
            oid_end = end_time
        
        # Log message (clearer for real-time sync)
        if description == "real-time":
            self.logger.info(f"Fetching {start_time} of data for {len(external_ids)} time series (OID range: {oid_start} to {oid_end})")
        else:
            desc = f" ({description})" if description else ""
            ts_desc = f"{len(external_ids)} time series" if len(external_ids) > 1 else f"{external_ids[0]}"
            self.logger.info(f"Syncing {ts_desc} from {oid_start} to {oid_end}{desc}")
        
        try:
            # Get data from OID (publication delayed one week for business reasons)
            dps_list = self.oid_client.client.time_series.data.retrieve(
                external_id=external_ids,
                start=oid_start,
                end=oid_end
            )
            
            total_dps = sum(len(dps) for dps in dps_list)            
            if total_dps == 0:
                self.logger.warning(f"No datapoints for period {start_time}")
                return 0
            
            desc_short = external_ids[0] if len(external_ids) == 1 else "multiple IDs"
            self.logger.info(f"Inserting {total_dps:,} data points from {start_time} for {desc_short}")
            
            # Prepare and insert datapoints
            offset_ms = self._get_time_offset_ms()
            datapoints_to_insert = self._prepare_datapoints_for_insert(dps_list, offset_ms)
            
            self.target_client.time_series.data.insert_multiple(datapoints=datapoints_to_insert)
            
            self.logger.info(f"✓ Inserted {total_dps:,} datapoints")
            self.logger.update_stats(ts_synced=len(external_ids), dps_inserted=total_dps)
            
            return total_dps
            
        except CogniteAPIError as e:
            self.logger.error(f"API error during sync: {str(e)}")
            return 0
        except Exception as e:
            self.logger.error(f"Unexpected error during sync: {str(e)}")
            return 0
    
    def sync_time_series_data_batched(
        self,
        external_ids: List[str],
        start_time: str,
        end_time: str | None = None,
        batch_weeks: int = 1,
        description: str = "",
        mark_complete: bool = False,
    ) -> int:
        """
        Sync data in weekly batches to avoid memory issues with large backfills
        
        Args:
            external_ids: List of time series external IDs
            start_time: Start time (e.g., "12w-ago")
            end_time: End time (e.g., "169h-ago"), defaults to config.sync_end
            batch_weeks: Number of weeks to fetch per batch (default: 1)
            description: Description for logging
            mark_complete: If True, mark time series as backfilled when done
            
        Returns:
            Total number of datapoints inserted
        """
        if end_time is None:
            end_time = self.config.sync_end
        
        # Parse start time to get total weeks
        if "w-ago" in start_time:
            total_weeks = int(start_time.split("w")[0])
        else:
            # If not weeks, just use regular sync
            return self.sync_time_series_data(external_ids, start_time, description)
        
        # Parse end time to get target end point (in weeks)
        if "w-ago" in end_time:
            end_weeks = int(end_time.split("w")[0])
        elif "h-ago" in end_time:
            end_weeks = int(end_time.split("h")[0]) // (7 * 24)
        else:
            end_weeks = 0
        
        total_inserted = 0
        
        # Fetch data in weekly batches from oldest to newest
        for week_start in range(total_weeks, end_weeks, -batch_weeks):
            week_end = max(week_start - batch_weeks, end_weeks)
            
            batch_start = f"{week_start}w-ago"
            batch_end = f"{week_end}w-ago" if week_end > 0 else end_time
            
            self.logger.info(f"Fetching batch: {batch_start} to {batch_end}")
            
            try:
                dps_list = self.oid_client.client.time_series.data.retrieve(
                    external_id=external_ids,
                    start=batch_start,
                    end=batch_end
                )
                
                total_dps = sum(len(dps) for dps in dps_list)
                
                if total_dps == 0:
                    continue
                
                self.logger.info(f"Processing {total_dps:,} data points from batch")
                
                offset_ms = self._get_time_offset_ms()
                datapoints_to_insert = self._prepare_datapoints_for_insert(dps_list, offset_ms)
                
                if datapoints_to_insert:
                    self.target_client.time_series.data.insert_multiple(datapoints=datapoints_to_insert)
                    total_inserted += total_dps
                    self.logger.update_stats(ts_synced=len(external_ids), dps_inserted=total_dps)
                
            except Exception as e:
                self.logger.warning(f"Error fetching batch {batch_start} to {batch_end}: {str(e)}")
                continue
        
        # Always mark as backfilled if requested (even if no data found)
        # This prevents repeatedly attempting to backfill empty time series
        if mark_complete and len(external_ids) == 1:
            if total_inserted > 0:
                self.logger.info(f"✓ Inserted {total_inserted:,} datapoints in batches")
            else:
                self.logger.info(f"No historical data found for {external_ids[0]}, marking as complete")
            self.mark_as_backfilled(external_ids[0])
        
        return total_inserted
    
    def run_full_sync(self):
        """Run complete data synchronization with smart backfill to avoid duplicates"""
        self.logger.info("=" * 50)
        self.logger.info("Starting Open Industrial Data Sync")
        self.logger.info("=" * 50)
        
        # Get all PI time series from OID
        ts_ext_ids = self.get_pi_time_series()
        
        if not ts_ext_ids:
            self.logger.error("No time series found, aborting")
            return
        
        # Check if backfill reset is requested
        if self.config.reset_backfill:
            self.reset_backfill_tags(ts_ext_ids)
            self.update_config_reset_backfill()
            self.logger.info("=" * 50)
        
        # Sync all time series with recent data (last 15 minutes)
        # This provides "real-time" data updates matching the schedule frequency
        self.logger.info(f"Syncing recent data ({self.config.sync_realtime_start}) for all {len(ts_ext_ids)} time series")
        self.sync_time_series_data(
            ts_ext_ids,
            self.config.sync_realtime_start,  # e.g., "15m-ago" for 10-min schedule
            description="real-time"
        )
        
        # Smart backfill: Only backfill time series that haven't been fully backfilled yet
        # This avoids re-syncing the same data repeatedly
        unbackfilled = self.get_unbackfilled_time_series(ts_ext_ids)
        
        if not unbackfilled:
            # All time series are backfilled - just maintain recent data
            self.logger.info("✓ All time series fully backfilled, maintaining recent data only")
        else:
            # Pick one random time series from those that need backfill
            random_ts = random.choice(unbackfilled)
            
            # Check if the time series Data Model node exists before backfilling
            # Datapoints can be written using instance_id if the node exists
            try:
                node_id = NodeId(space=self.config.instance_space, external_id=random_ts)
                result = self.target_client.data_modeling.instances.retrieve_nodes(
                    nodes=node_id, 
                    sources=self.timeseries_view_id
                )
                
                ts_node = result if isinstance(result, Node) else (result[0] if result and len(result) > 0 else None)
                
                if not ts_node:
                    self.logger.info(f"Skipping {random_ts}: no Data Model node yet ({len(unbackfilled)} remaining)")
                    self.mark_as_backfilled(random_ts)
                    return
                    
            except Exception as e:  # noqa: F841
                self.logger.info(f"Skipping {random_ts}: error checking node ({len(unbackfilled)} remaining)")
                self.mark_as_backfilled(random_ts)
                return
            
            self.logger.info(f"Random backfill: {random_ts} ({len(unbackfilled)} remaining)")
            
            # Backfill from 12w-ago to sync_realtime_start (EXCLUDES the recent window already synced)
            # This prevents duplicate data ingestion
            self.sync_time_series_data_batched(
                [random_ts],
                self.config.sync_random_start,  # "12w-ago"
                end_time=self.config.sync_realtime_start,  # e.g., "15m-ago" - stops before recent sync window
                batch_weeks=1,  # Fetch 1 week at a time to avoid memory issues
                description="backfill (no overlap)",
                mark_complete=True  # Mark as backfilled when done
            )
        
        self.logger.summary()
