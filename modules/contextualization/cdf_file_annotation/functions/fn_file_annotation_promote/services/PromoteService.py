import abc
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    EdgeList,
    EdgeApply,
    NodeOrEdgeData,
    DirectRelationReference,
)
from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import DiagramAnnotationStatus


class IPromoteService(abc.ABC):
    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralPromoteService(IPromoteService):
    def __init__(self, client: CogniteClient, config: Config, logger: CogniteFunctionLogger):
        self.client = client
        self.config = config
        self.logger = logger
        self.core_annotation_view = self.config.data_model_views.core_annotation_view
        self.file_view = self.config.data_model_views.file_view
        self.target_entities_view = self.config.data_model_views.target_entities_view
        self.sink_node_ref = DirectRelationReference(
            space=self.config.finalize_function.apply_service.sink_node.space,
            external_id=self.config.finalize_function.apply_service.sink_node.external_id,
        )

    def run(self) -> str | None:
        """Main entrypoint for the Promote service."""
        candidates = self._get_Promote_candidates()
        if not candidates:
            self.logger.info("No Promote candidates found.")
            return "Done"

        self.logger.info(f"Found {len(candidates)} Promote candidates. Starting processing.")
        edges_to_update = []
        for edge in candidates:
            properties = edge.properties[self.core_annotation_view.as_view_id()]
            text_to_find = properties.get("startNodeText")
            if not text_to_find:
                continue
            if properties.get("type") == "diagrams.FileLink":
                search_space: str | None = self.file_view.instance_space
            else:
                search_space: str | None = self.target_entities_view.instance_space
            found_nodes = self._find_global_entity(text_to_find, search_space)
            edge_apply = self._prepare_edge_update(edge, found_nodes)
            if edge_apply:
                edges_to_update.append(edge_apply)

        if edges_to_update:
            self.client.data_modeling.instances.apply(edges=edges_to_update)
            self.client.raw.rows.insert
            self.logger.info(f"Successfully processed {len(edges_to_update)} edges.")
        else:
            self.logger.info("No edges were updated in this run.")

        return None  # Continue running if more candidates might exist

    def _get_Promote_candidates(self) -> EdgeList | None:
        """Queries for suggested edges pointing to the sink node that haven't been Promote-attempted."""
        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view.as_view_id()],
            filter={
                "and": [
                    {
                        "equals": {
                            "property": ["edge", "endNode"],
                            "value": {"space": self.sink_node_ref.space, "externalId": self.sink_node_ref.external_id},
                        }
                    },
                    {"equals": {"property": self.core_annotation_view.as_property_ref("status"), "value": "Suggested"}},
                    {
                        "not": {
                            "containsAny": {
                                "property": self.core_annotation_view.as_property_ref("tags"),
                                "values": ["Promote-attempted"],
                            }
                        }
                    },
                ]
            },
            limit=500,  # Batch size
        )

    def _find_global_entity(self, text: str, space: str | None):
        """Performs a global, un-scoped search for an entity matching the given text."""
        # NOTE: This approach is likely the slowest since we have to query against all instances, in a given space.
        # Pros: The most accurate and guaranteed approach
        # Cons: Will likely timeout as the amount of instances in a given space increase
        return self.client.data_modeling.instances.list(
            instance_type="node",
            sources=[self.target_entities_view.as_view_id()],
            filter={"in": {"property": self.target_entities_view.as_property_ref("aliases"), "values": [text]}},
            space=space,
            limit=2,  # Limit to 2 to detect ambiguity
        )

    def _prepare_edge_update(self, edge: EdgeApply, found_nodes) -> EdgeApply | None:
        """Prepares the EdgeApply object for the update based on the number of matches found."""
        edge_apply = edge.as_write()
        properties = edge_apply.sources[0].properties
        tags = properties.get("tags", [])

        if len(found_nodes) == 1:  # Success
            self.logger.info(f"Found single match for '{properties.get('startNodeText')}'. Promoting edge.")
            edge_apply.end_node = DirectRelationReference(found_nodes[0].space, found_nodes[0].external_id)
            properties["status"] = DiagramAnnotationStatus.APPROVED.value
            tags.append("Promoteed-auto")
        elif len(found_nodes) == 0:  # Failure
            self.logger.info(f"Found no match for '{properties.get('startNodeText')}'. Rejecting edge.")
            properties["status"] = DiagramAnnotationStatus.REJECTED.value
            tags.append("Promote-attempted")
        else:  # Ambiguous
            self.logger.info(f"Found multiple matches for '{properties.get('startNodeText')}'. Marking as ambiguous.")
            tags.extend(["Promote-attempted", "ambiguous-match"])

        properties["tags"] = tags
        return edge_apply
