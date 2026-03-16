
import abc
from typing import Literal
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeId,
    NodeOrEdgeData
)

from ..config import Config
from .LoggerService import CogniteFunctionLogger

class IApplyService(abc.ABC):
    """
    Interface for services that apply data to instances in CDF
    """

    @abc.abstractmethod
    def run(self, data) -> Literal["Done"] | None:
        """Main execution method for applying extracted keys
        
        Returns:
            Literal["Done"] | None: Returns "Done" if the operation was successful, otherwise None.
        """
        pass

class GeneralApplyService(IApplyService):
    """
    General implementation of the IApplyService interface.
    This service applies extracted keys to instances in CDF.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger
    ):
        """
        Initializes the apply service with required dependencies
        
        Args:
            client: CogniteClient for API interactions
            config: Configuration object containing data model views and settings
            logger: Logger instance for tracking execution
        """

        self.client = client
        self.config = config
        self.logger = logger
        self.entity_keys = {}

    def run(self) -> Literal["Done"] | None:
        """
        Main execution method for applying extracted keys
        
        Returns:
            Literal["Done"] | None: Returns "Done" if the operation was successful, otherwise None.
        """
        target_view_config = self.config.data.source_view

        # If there's no source view configured, we don't apply
        if not target_view_config:
            self.logger.warning(f"No target view configured. Skipping apply service.")
            return None
        
        try:
            self.logger.info("Applying extracted keys to instances in CDF")
            
            # Get configuration parameters
            raw_db = self.config.parameters.raw_db
            raw_table_key = self.config.parameters.raw_table_key
            processed_node_ids = []
            
            # Process each rule's table
            for rule in self.config.data.extraction_rules:
                rule_id = rule.rule_id
                rule_table_name = f"{raw_table_key}_{rule_id}"
                
                self.logger.info(f"Processing rule table: {rule_table_name}")
                
                # Load rows from rule table in batches
                cursor = None
                total_processed = 0
                
                while True:
                    # Fetch batch of rows
                    rows_response = self.client.raw.rows.list(
                        db_name=raw_db,
                        table_name=rule_table_name,
                        limit=None, #Just load the whole raw table why not
                    )
                    
                    if not rows_response:
                        break
                    
                    rows = rows_response.data if hasattr(rows_response, 'data') else rows_response
                    
                    if not rows:
                        break
                    
                    for row in rows:
                        ext_id = row.key
                        row_data = row.columns
                        
                        self._process_row(ext_id, row_data, rule_id)

                        # Add this node to the list of processed nodes that we need to preserve the original target_property
                        if not self.config.parameters.overwrite:
                            processed_node_ids.append(NodeId(target_view_config.instance_space, ext_id))
                    
                    total_processed += len(rows)
                    self.logger.info(f"Processed {total_processed} rows from {rule_table_name}")      

                    # If we are not overwriting, then we must include the existing aliases
                    if not processed_node_ids == []:
                        try:
                            #TODO: Query JUST the target property
                            nodes = self.client.data_modeling.instances.retrieve(nodes=processed_node_ids, sources=[target_view_config.as_view_id()])
                            for node in nodes:
                                # Get the target propery, usually 'aliases'
                                target_prop_value = node.get(target_view_config.view_space, {}).get(f"{target_view_config.view_external_id}/{target_view_config.view_version}", {}).get(target_view_config.target_prop, {})
                                
                                if isinstance(target_prop_value, list):
                                    self.entity_keys[ext_id].extend(target_prop_value)
                                
                        except:
                            self.logger.error(f"Failed to retrieve existing nodes, skipping failed keys to perserve existing aliases")
                            processed_node_ids= []
                            

                    # Check if there are more rows to fetch
                    cursor = rows_response.cursor if hasattr(rows_response, 'cursor') else None
                    if not cursor:
                        break
                
            # Upload the updated keys to the target view
            updates = []
            for ext_id, keys in self.entity_keys.items():
                updates.append(
                    NodeApply(
                        space=target_view_config.instance_space,
                        external_id=ext_id,
                        sources=[NodeOrEdgeData(target_view_config.as_view_id(), {target_view_config.target_prop: keys})]
                    )
                )
            try:
                self.client.data_modeling.instances.apply(
                    nodes=updates
                )
            except Exception as e:
                self.logger.error(f"Failed to update nodes with new keys: {e}")

            self.logger.info(f"Completed processing {rule_table_name}: {total_processed} total rows")
            

            return "Done"
        except Exception as e:
            self.logger.error(f"Failed to apply extracted keys: {e}")
            return None
    
    def _process_row(self, ext_id: str, row_data: dict, rule_id: str) -> None:
        """
        Add the key to the entity. Kept as a separate function for clarity.
        
        Args:
            ext_id: The entity external ID (row key)
            row_data: Dictionary containing extraction results for this entity
            rule_id: The rule ID for this extraction
        """

        keys = row_data.get("value", [])

        for key in keys:
            if key == '' or len(key) < self.config.parameters.min_key_length:
                self.logger.warning(f"Skipping row {ext_id} for rule {rule_id} due to insufficient key length.")
                return None

            if self.entity_keys.get(ext_id) is not None:
                self.entity_keys[ext_id].append(key)
            else:
                self.entity_keys[ext_id] = [key]