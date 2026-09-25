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
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpression,
    Query,
    ResultSetExpression,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import (
    Equals,
    Filter,
    HasData,
    In,
    Not,
    Range,
)
from fa_constants import LAUNCH_STATE_LIMIT, QUERY_PAGE_SIZE, TAG_ANNOTATION_IN_PROCESS, TAG_SCOPE_WIDE_DETECT
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
        return self._query_nodes(
            "files",
            self.file_view,
            filter_files_to_annotate,
            ["tags"],
            self.get_files_to_annotate_retrieve_limit or -1,
        )

    def _query_nodes(
        self, name: str, view: ViewPropertyConfig, node_filter: Filter, properties: list[str], limit: int
    ) -> NodeList:
        """
        Reads the nodes of a view matching the filter, a page at a time, carrying only the given properties.

        Args:
            name: Name of the result set, as it shows up in the query.
            view: View the nodes are read from, in its instanceSpace when it has one.
            node_filter: Filter selecting the nodes.
            properties: View properties to return.
            limit: Maximum number of nodes; -1 reads them all.

        Returns:
            The matching nodes.
        """
        view_id = view.as_view_id()
        node_filter = self._in_view(view, node_filter)
        nodes: list[Node] = []
        cursor: str | None = None
        while True:
            page_size = QUERY_PAGE_SIZE if limit < 0 else min(QUERY_PAGE_SIZE, limit - len(nodes))
            query = Query(
                with_={name: NodeResultSetExpression(filter=node_filter, limit=page_size)},
                select={name: Select([SourceSelector(view_id, properties)])},
                cursors={name: cursor},
            )
            result = self.client.data_modeling.instances.query(query)
            page = list(result[name])
            nodes.extend(page)
            cursor = result.cursors.get(name)
            if not cursor or len(page) < page_size or (limit >= 0 and len(nodes) >= limit):
                return NodeList(nodes)

    @staticmethod
    def _in_view(view: ViewPropertyConfig, node_filter: Filter) -> Filter:
        """The filter narrowed to nodes with data in the view, in its instanceSpace when it has one."""
        scoped: Filter = HasData(views=[view.as_view_id()]) & node_filter
        if view.instance_space:
            scoped = Equals(["node", "space"], view.instance_space) & scoped
        return scoped

    def get_files_to_process(
        self,
    ) -> tuple[NodeList, dict[NodeId, Node]] | tuple[None, None]:
        """
        Retrieves files with annotation state instances that are ready for diagram detection.

        The states and their linked files are read in one query: the states that are New/Retry
        (the getFilesToProcess filter) and those stuck in Processing/Finalizing for more than
        12 hours are separate result sets, since one OR across both keeps DMS from paging it with
        an index. The files are reached through the linkedFile relation and carry only the
        properties Launch uses. Stuck states come first, then new ones, up to the retrieve limit.

        Args:
            None

        Returns:
            A tuple containing:
                - NodeList of file instances to process
                - Dictionary mapping file NodeIds to their annotation state Node instances
            Returns (None, None) if no files are found.

        NOTE: With debugFileExternalId set, only the debug file's state is read and the stuck-job retry is skipped.
        """
        state_view_id = self.annotation_state_view.as_view_id()
        file_view_id = self.file_view.as_view_id()
        launch = self.config.launch_function
        file_properties = list(
            dict.fromkeys(p for p in ["tags", launch.primary_scope_property, launch.secondary_scope_property] if p)
        )
        limit = self.get_files_to_process_retrieve_limit
        page_size = LAUNCH_STATE_LIMIT if not limit or limit < 0 else limit

        state_sets = {"new_states": self.filter_files_to_process}
        debug_file = self.config.debug_file
        if debug_file:
            state_sets["new_states"] = self.filter_files_to_process & Equals(
                self.annotation_state_view.as_property_ref("linkedFile"),
                {"space": debug_file.space, "externalId": debug_file.external_id},
            )
        else:
            state_sets = {"stuck_states": self._get_stuck_state_filter(), **state_sets}

        with_: dict[str, ResultSetExpression] = {}
        select: dict[str, Select] = {}
        for states in state_sets:
            files = states.replace("_states", "_files")
            with_[states] = NodeResultSetExpression(
                filter=self._in_view(self.annotation_state_view, state_sets[states]), limit=page_size
            )
            with_[files] = NodeResultSetExpression(
                from_=states,
                through=state_view_id.as_property_ref("linkedFile"),
                direction="outwards",
                limit=page_size,
            )
            select[states] = Select([SourceSelector(state_view_id, ["*"])])
            select[files] = Select([SourceSelector(file_view_id, file_properties)])
        result = self.client.data_modeling.instances.query(Query(with_=with_, select=select))

        files_by_id: dict[NodeId, Node] = {}
        file_to_state_map: dict[NodeId, Node] = {}
        for states in state_sets:
            files_by_id.update({file.as_id(): file for file in result[states.replace("_states", "_files")]})
            for state in result[states]:
                if len(file_to_state_map) >= page_size:
                    break
                file_reference = ((state.properties or {}).get(state_view_id) or {}).get("linkedFile")
                if not isinstance(file_reference, dict):
                    continue
                file_node_id = NodeId(file_reference["space"], file_reference["externalId"])
                in_file_space = self.file_view.instance_space in (None, file_node_id.space)
                if in_file_space and file_node_id in files_by_id:
                    file_to_state_map.setdefault(file_node_id, state)

        if not file_to_state_map:
            return None, None
        return NodeList([files_by_id[file_id] for file_id in file_to_state_map]), file_to_state_map

    def _get_stuck_state_filter(self) -> Filter:
        """
        The annotation states stuck in Processing/Finalizing for more than 12 hours, which Launch runs again.

        NOTE: While this number is hard coded, I believe it doesn't need to be configured. Number comes from my
        experience with the pipeline. Feel free to change if your experience leads to a different number.
        """
        latest_permissible_time_utc = datetime.now(UTC) - timedelta(minutes=720)
        return In(
            self.annotation_state_view.as_property_ref("annotationStatus"),
            [AnnotationStatus.PROCESSING, AnnotationStatus.FINALIZING],
        ) & Range(
            self.annotation_state_view.as_property_ref("sourceUpdatedTime"),
            lt=latest_permissible_time_utc.isoformat(timespec="milliseconds"),
        )

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
