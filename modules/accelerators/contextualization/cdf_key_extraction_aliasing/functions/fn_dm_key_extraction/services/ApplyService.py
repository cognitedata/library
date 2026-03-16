"""Apply extracted keys to instances in CDF."""

import abc
from typing import Literal

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeId,
    NodeOrEdgeData,
)

from ..config import Config
from ..common.logger import CogniteFunctionLogger


class IApplyService(abc.ABC):
    """Interface for services that apply data to instances in CDF."""

    @abc.abstractmethod
    def run(self, data=None) -> Literal["Done"] | None:
        """Main execution method for applying extracted keys."""
        pass


class GeneralApplyService(IApplyService):
    """General implementation that applies extracted keys to instances in CDF."""

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
    ):
        self.client = client
        self.config = config
        self.logger = logger
        self.entity_keys = {}

    def run(self) -> Literal["Done"] | None:
        target_view_config = self.config.data.source_view
        if not target_view_config:
            self.logger.warning("No target view configured. Skipping apply service.")
            return None
        try:
            self.logger.info("Applying extracted keys to instances in CDF")
            raw_db = self.config.parameters.raw_db
            raw_table_key = self.config.parameters.raw_table_key
            processed_node_ids = []
            for rule in self.config.data.extraction_rules:
                rule_id = rule.rule_id
                rule_table_name = f"{raw_table_key}_{rule_id}"
                self.logger.info(f"Processing rule table: {rule_table_name}")
                total_processed = 0
                while True:
                    rows_response = self.client.raw.rows.list(
                        db_name=raw_db,
                        table_name=rule_table_name,
                        limit=None,
                    )
                    if not rows_response:
                        break
                    rows = (
                        rows_response.data
                        if hasattr(rows_response, "data")
                        else rows_response
                    )
                    if not rows:
                        break
                    for row in rows:
                        ext_id = row.key
                        row_data = row.columns
                        self._process_row(ext_id, row_data, rule_id)
                        if not self.config.parameters.overwrite:
                            processed_node_ids.append(
                                NodeId(target_view_config.instance_space, ext_id)
                            )
                    total_processed += len(rows)
                    self.logger.info(
                        f"Processed {total_processed} rows from {rule_table_name}"
                    )
                    if processed_node_ids:
                        try:
                            nodes = self.client.data_modeling.instances.retrieve(
                                nodes=processed_node_ids,
                                sources=[target_view_config.as_view_id()],
                            )
                            for node in nodes:
                                nspace = getattr(
                                    node, "space", target_view_config.view_space
                                )
                                props = (
                                    getattr(node, "properties", {}) or {}
                                ).get(nspace, {}).get(
                                    f"{target_view_config.view_external_id}/{target_view_config.view_version}",
                                    {},
                                )
                                target_prop_value = props.get(
                                    target_view_config.target_prop
                                )
                                ext_id = getattr(node, "external_id", None)
                                if ext_id and isinstance(target_prop_value, list):
                                    if self.entity_keys.get(ext_id) is not None:
                                        self.entity_keys[ext_id].extend(
                                            target_prop_value
                                        )
                        except Exception as e:
                            self.logger.error(
                                f"Failed to retrieve existing nodes: {e}"
                            )
                            processed_node_ids = []
                    cursor = (
                        getattr(rows_response, "cursor", None)
                        if hasattr(rows_response, "cursor")
                        else None
                    )
                    if not cursor:
                        break
            updates = []
            for ext_id, keys in self.entity_keys.items():
                updates.append(
                    NodeApply(
                        space=target_view_config.instance_space,
                        external_id=ext_id,
                        sources=[
                            NodeOrEdgeData(
                                target_view_config.as_view_id(),
                                {target_view_config.target_prop: keys},
                            )
                        ],
                    )
                )
            if updates:
                try:
                    self.client.data_modeling.instances.apply(nodes=updates)
                except Exception as e:
                    self.logger.error(f"Failed to update nodes with new keys: {e}")
            self.logger.info("Completed apply service")
            return "Done"
        except Exception as e:
            self.logger.error(f"Failed to apply extracted keys: {e}")
            return None

    def _process_row(
        self, ext_id: str, row_data: dict, rule_id: str
    ) -> None:
        keys = row_data.get("value", [])
        for key in keys:
            if key == "" or len(key) < self.config.parameters.min_key_length:
                self.logger.warning(
                    f"Skipping row {ext_id} for rule {rule_id} due to insufficient key length."
                )
                continue
            if self.entity_keys.get(ext_id) is not None:
                self.entity_keys[ext_id].append(key)
            else:
                self.entity_keys[ext_id] = [key]
