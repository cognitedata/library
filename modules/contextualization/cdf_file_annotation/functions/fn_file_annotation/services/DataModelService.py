import abc
from datetime import UTC, datetime, timedelta

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    InstancesApplyResult,
    Node,
    NodeApply,
    NodeApplyResultList,
    NodeId,
    NodeList,
    ViewId,
)
from cognite.client.data_classes.filters import (
    Equals,
    Filter,
    In,
    Not,
    Range,
)
from fa_constants import TAG_ANNOTATION_IN_PROCESS, TAG_SCOPE_WIDE_DETECT
from services.ConfigService import (
    Config,
    ViewPropertyConfig,
    build_filter_from_query,
    get_limit_from_query,
)
from services.EntitySyncService import EntityInstance, EntitySyncService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import AnnotationStatus


class IDataModelService(abc.ABC):
    """
    Interface for interacting with data model instances in CDF
    """

    @abc.abstractmethod
    def get_files_for_annotation_reset(self) -> NodeList | None:
        pass

    @abc.abstractmethod
    def get_files_to_annotate(self) -> NodeList | None:
        pass

    @abc.abstractmethod
    def get_files_to_process(
        self,
    ) -> tuple[NodeList, dict[NodeId, Node]] | tuple[None, None]:
        pass

    @abc.abstractmethod
    def update_annotation_state(
        self,
        list_node_apply: list[NodeApply],
    ) -> NodeApplyResultList:
        pass

    @abc.abstractmethod
    def create_annotation_state(
        self,
        list_node_apply: list[NodeApply],
    ) -> NodeApplyResultList:
        pass

    @abc.abstractmethod
    def get_instances_entities(
        self, primary_scope_value: str, secondary_scope_value: str | None, file_space: str | None
    ) -> tuple[list[EntityInstance], list[EntityInstance]]:
        pass


class GeneralDataModelService(IDataModelService):
    """
    Implementation used for real runs
    """

    def __init__(
        self,
        config: Config,
        client: CogniteClient,
        logger: CogniteFunctionLogger,
        data_set_id: int | None = None,
        entity_read_deadline: float | None = None,
    ):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger
        self.entity_sync = EntitySyncService(client, config, logger, data_set_id, entity_read_deadline)
        # Latest read per view, so the scopes of one space share a single read.
        self._synced_entities: dict[ViewId, tuple[str | None, list[EntityInstance]]] = {}

        self.annotation_state_view: ViewPropertyConfig = config.data_model_views.annotation_state_view
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.target_entities_view: ViewPropertyConfig = config.data_model_views.target_entities_view

        self.get_files_to_annotate_retrieve_limit: int | None = get_limit_from_query(
            config.prepare_function.get_files_to_annotate_query
        )
        self.get_files_to_process_retrieve_limit: int | None = get_limit_from_query(
            config.launch_function.data_model_service.get_files_to_process_query
        )

        self.filter_files_to_annotate: Filter = build_filter_from_query(
            config.prepare_function.get_files_to_annotate_query
        )
        self.filter_files_to_process: Filter = build_filter_from_query(
            config.launch_function.data_model_service.get_files_to_process_query
        )

    def get_files_for_annotation_reset(self) -> NodeList | None:
        """
        Retrieves files that need their annotation status reset based on configuration.

        Args:
            None

        Returns:
            NodeList of file instances to reset, or None if no reset query is configured.

        NOTE: Not building the filter in the object instantiation because the filter will only ever be used once throughout all runs of prepare
              Furthermore, there is an implicit guarantee that a filter will be returned b/c launch checks if the query exists.
        """
        if not self.config.prepare_function.get_files_for_annotation_reset_query:
            return None

        filter_files_for_annotation_reset: Filter = build_filter_from_query(
            self.config.prepare_function.get_files_for_annotation_reset_query
        )
        result: NodeList | None = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.file_view.as_view_id(),
            space=self.file_view.instance_space,
            limit=-1,  # NOTE: this should always be kept at -1 so that all files defined in the query will get reset
            filter=filter_files_for_annotation_reset,
        )
        return result

    def get_files_to_annotate(self) -> NodeList | None:
        """
        Retrieves files ready for annotation processing based on their tag status.

        Queries for files marked "ToAnnotate" that don't have 'AnnotationInProcess' or 'Annotated' tags.
        The specific query filters are defined in the getFilesToAnnotate config parameter.

        Args:
            None

        Returns:
            NodeList of file instances ready for annotation, or None if no files found.

        NOTE: With debugFileExternalId set, only that file is returned, regardless of its ToAnnotate/Annotated tags.
              AnnotationInProcess is still excluded so the file is not prepared again while it is being processed.
        """
        filter_files_to_annotate = self.filter_files_to_annotate
        debug_file = self.config.debug_file
        if debug_file:
            filter_files_to_annotate = Equals(["node", "externalId"], debug_file.external_id) & Not(
                In(self.file_view.as_property_ref("tags"), [TAG_ANNOTATION_IN_PROCESS])
            )
        result: NodeList | None = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.file_view.as_view_id(),
            space=self.file_view.instance_space,
            # NOTE: the amount of instances that are returned may or may not matter
            # depending on how the memory constraints of azure/aws functions
            limit=self.get_files_to_annotate_retrieve_limit,
            filter=filter_files_to_annotate,
        )

        return result

    def get_files_to_process(
        self,
    ) -> tuple[NodeList, dict[NodeId, Node]] | tuple[None, None]:
        """
        Retrieves files with annotation state instances that are ready for diagram detection.

        Queries for FileAnnotationStateInstances based on the getFilesToProcess config parameter,
        extracts the linked file NodeIds, and retrieves the corresponding file nodes.

        Args:
            None

        Returns:
            A tuple containing:
                - NodeList of file instances to process
                - Dictionary mapping file NodeIds to their annotation state Node instances
            Returns (None, None) if no files are found.
        """
        annotation_state_filter = self._get_annotation_state_filter()
        annotation_state_instances: NodeList = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.annotation_state_view.as_view_id(),
            space=self.annotation_state_view.instance_space,
            limit=self.get_files_to_process_retrieve_limit,
            filter=annotation_state_filter,
        )

        if not annotation_state_instances:
            return None, None

        file_to_state_map: dict[NodeId, Node] = {}
        list_file_node_ids: list[NodeId] = []

        for node in annotation_state_instances:
            file_reference = node.properties.get(self.annotation_state_view.as_view_id()).get("linkedFile")
            if self.file_view.instance_space is None or self.file_view.instance_space == file_reference["space"]:
                file_node_id = NodeId(
                    space=file_reference["space"],
                    external_id=file_reference["externalId"],
                )

                file_to_state_map[file_node_id] = node
                list_file_node_ids.append(file_node_id)

        file_instances: NodeList = self.client.data_modeling.instances.retrieve_nodes(
            nodes=list_file_node_ids,
            sources=self.file_view.as_view_id(),
        )

        return file_instances, file_to_state_map

    def _get_annotation_state_filter(self) -> Filter:
        """
        Builds a filter for annotation state instances, including automatic retry logic for stuck jobs.

        Combines the configured filter with a fallback filter that catches annotation state instances
        stuck in Processing/Finalizing status for more than 12 hours.

        Args:
            None

        Returns:
            Combined Filter for querying annotation state instances.

        NOTE: filter = (getFilesToProcess filter || (annotationStatus == Processing && now() - lastUpdatedTime) > 1440 minutes)
        - getFilesToProcess filter comes from extraction pipeline
        - (annotationStatus == Processing | Finalizing && now() - lastUpdatedTime) > 720 minutes/12 hours -> hardcoded -> reprocesses any file that's stuck
            - Edge case that occurs very rarely but can happen.
        NOTE: Implementation of a more complex query that can't be handled in config should come from an implementation of the interface.
        NOTE: With debugFileExternalId set, only the debug file's state is returned and the stuck-job retry is skipped.
        """
        debug_file = self.config.debug_file
        if debug_file:
            return self.filter_files_to_process & Equals(
                self.annotation_state_view.as_property_ref("linkedFile"),
                {"space": debug_file.space, "externalId": debug_file.external_id},
            )

        annotation_status_property = self.annotation_state_view.as_property_ref("annotationStatus")
        annotation_last_updated_property = self.annotation_state_view.as_property_ref("sourceUpdatedTime")
        # NOTE: While this number is hard coded, I believe it doesn't need to be configured. Number comes from my experience with the pipeline. Feel free to change if your experience leads to a different number
        latest_permissible_time_utc = datetime.now(UTC) - timedelta(minutes=720)
        latest_permissible_time_utc = latest_permissible_time_utc.isoformat(timespec="milliseconds")
        filter_stuck = In(
            annotation_status_property,
            [AnnotationStatus.PROCESSING, AnnotationStatus.FINALIZING],
        ) & Range(annotation_last_updated_property, lt=latest_permissible_time_utc)

        filter = self.filter_files_to_process | filter_stuck  # | == OR
        return filter

    def update_annotation_state(self, list_node_apply: list[NodeApply]) -> NodeApplyResultList:
        """
        Updates existing annotation state nodes with new property values.

        Args:
            list_node_apply: List of NodeApply objects containing updated properties.

        Returns:
            NodeApplyResultList containing the results of the update operation.
        """
        update_results: InstancesApplyResult = self.client.data_modeling.instances.apply(
            nodes=list_node_apply,
            replace=False,  # ensures we don't delete other properties in the view
        )
        return update_results.nodes

    def create_annotation_state(self, list_node_apply: list[NodeApply]) -> NodeApplyResultList:
        """
        Creates new annotation state nodes, replacing any existing nodes with the same IDs.

        Args:
            list_node_apply: List of NodeApply objects to create as new annotation state instances.

        Returns:
            NodeApplyResultList containing the results of the creation operation.
        """
        update_results: InstancesApplyResult = self.client.data_modeling.instances.apply(
            nodes=list_node_apply,
            auto_create_direct_relations=True,
            replace=True,  # ensures we reset the properties of the node
        )
        return update_results.nodes

    def get_instances_entities(
        self, primary_scope_value: str, secondary_scope_value: str | None, file_space: str | None
    ) -> tuple[list[EntityInstance], list[EntityInstance]]:
        """
        Retrieves target entities and file entities for use in diagram detection.

        Each view is read whole through the entity sync cache, from its configured instanceSpace
        or from file_space when it has none, and then narrowed to the scope in memory:
            - entities in the primary and secondary scope carrying one of the configured tags
              (files must also have their search property set)
            - or entities in the primary scope tagged ScopeWideDetect, whatever their secondary scope

        Args:
            primary_scope_value: Primary scope identifier (e.g., site, facility).
            secondary_scope_value: Optional secondary scope identifier (e.g., unit, area).
            file_space: Instance space of the files being annotated, used for views without an instanceSpace.

        Returns:
            A tuple containing:
                - Target entity instances (typically assets)
                - File entity instances

        Raises:
            EntitySyncIncompleteError: The first read of a view did not finish within the time budget.
        """
        launch = self.config.launch_function
        parameters = self.config.parameters
        target_entities = self._entities_in_scope(
            self.target_entities_view,
            self.target_entities_view.instance_space or file_space,
            parameters.target_entities_tags,
            [launch.target_entities_search_property, launch.target_entities_resource_property],
            None,
            primary_scope_value,
            secondary_scope_value,
        )
        file_entities = self._entities_in_scope(
            self.file_view,
            self.file_view.instance_space or file_space,
            parameters.file_entities_tags,
            [launch.file_search_property, launch.file_resource_property],
            launch.file_search_property,
            primary_scope_value,
            secondary_scope_value,
        )
        return target_entities, file_entities

    def _entities_in_scope(
        self,
        view: ViewPropertyConfig,
        space: str | None,
        entity_tags: list[str],
        extra_properties: list[str | None],
        required_property: str | None,
        primary_scope_value: str,
        secondary_scope_value: str | None,
    ) -> list[EntityInstance]:
        """The entities of the view that the scope matches against; see get_instances_entities."""
        launch = self.config.launch_function
        primary_property = launch.primary_scope_property if primary_scope_value else None
        secondary_property = launch.secondary_scope_property if secondary_scope_value else None
        view_id = view.as_view_id()

        # The read does not depend on the scope, so it is shared by every scope in the space.
        synced = self._synced_entities.get(view_id)
        if synced is None or synced[0] != space:
            selected = ["name", "tags", launch.primary_scope_property, launch.secondary_scope_property]
            selected = list(dict.fromkeys(p for p in [*selected, *extra_properties] if p))
            kept_tags = list(dict.fromkeys([*entity_tags, TAG_SCOPE_WIDE_DETECT]))
            synced = (space, self.entity_sync.load(view, space, selected, kept_tags))
            self._synced_entities[view_id] = synced

        in_scope: list[EntityInstance] = []
        wanted_tags = set(entity_tags)
        for entity in synced[1]:
            properties = entity.properties[view_id]
            if primary_property and properties.get(primary_property) != primary_scope_value:
                continue
            raw_tags = properties.get("tags")
            tags = set(raw_tags) if isinstance(raw_tags, list) else set()
            scope_wide = TAG_SCOPE_WIDE_DETECT in tags
            in_secondary_scope = not secondary_property or properties.get(secondary_property) == secondary_scope_value
            searchable = not required_property or properties.get(required_property) is not None
            if scope_wide or (not wanted_tags.isdisjoint(tags) and in_secondary_scope and searchable):
                in_scope.append(entity)
        return in_scope
