import abc
from typing import Literal

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeApply, NodeList
from cognite.client.exceptions import CogniteAPIError
from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import (
    AnnotationState,
    AnnotationStatus,
    PerformanceTracker,
    node_tags,
    tags_apply,
)
from utils.QueryTimeout import QueryTimeoutRetry, is_query_timeout


class AbstractPrepareService(abc.ABC):
    """
    Orchestrates the file annotation prepare process. This service prepares files for annotation
    by creating annotation state instances for files marked ToAnnotate.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        data_model_service: IDataModelService,
    ):
        self.client = client
        self.config = config
        self.logger = logger
        self.tracker = tracker
        self.data_model_service = data_model_service

    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralPrepareService(AbstractPrepareService):
    """
    Orchestrates the file annotation prepare process. This service prepares files for annotation
    by creating annotation state instances for files marked ToAnnotate.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        data_model_service: IDataModelService,
        function_call_info: dict,
    ):
        super().__init__(
            client,
            config,
            logger,
            tracker,
            data_model_service,
        )

        self.annotation_state_view: ViewPropertyConfig = config.data_model_views.annotation_state_view
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view

        self.function_id: int | None = function_call_info.get("function_id")
        self.call_id: int | None = function_call_info.get("call_id")

        self.query_timeout = QueryTimeoutRetry(logger)
        self.reset_files: bool = False
        if self.config.prepare_function.get_files_for_annotation_reset_query:
            self.reset_files = True

    def run(self) -> Literal["Done"] | None:
        """
        Prepares files for annotation by creating annotation state instances.

        Retrieves files marked "ToAnnotate", creates corresponding FileAnnotationState instances,
        and updates file tags to indicate processing has started. Can also reset files if configured.

        Args:
            None

        Returns:
            "Done" if no more files need preparation, None if processing should continue.

        Raises:
            CogniteAPIError: On API errors, and on a query that keeps timing out (408) after a few retries.
            ValueError: If annotation state view instance space is not configured.
        """
        self.logger.info(
            message="Starting Prepare Function",
            section="START",
        )
        try:
            if self.reset_files:
                file_nodes_to_reset: NodeList | None = self.data_model_service.get_files_for_annotation_reset()
                if not file_nodes_to_reset:
                    self.logger.info(
                        "No files found with the getFilesForAnnotationReset query provided in the config file"
                    )
                else:
                    self.logger.info(f"Resetting {len(file_nodes_to_reset)} files")
                    reset_node_apply: list[NodeApply] = []
                    tags_to_remove = {"AnnotationInProcess", "Annotated", "AnnotationFailed"}
                    file_view_id = self.file_view.as_view_id()
                    for file_node in file_nodes_to_reset:
                        tags_property = node_tags(file_node, file_view_id)
                        reset_node_apply.append(
                            tags_apply(
                                file_node,
                                file_view_id,
                                [t for t in tags_property if t not in tags_to_remove],
                            )
                        )
                    update_results = self.data_model_service.update_annotation_state(reset_node_apply)
                    self.logger.info(
                        f"Removed the AnnotationInProcess/Annotated/AnnotationFailed tag of {len(update_results)} files"
                    )
                self.reset_files = False
        except CogniteAPIError as e:
            if not is_query_timeout(e):
                raise
            self.query_timeout.wait(e)
            return None

        try:
            file_nodes: NodeList | None = self.data_model_service.get_files_to_annotate()
            if not file_nodes:
                self.logger.info(
                    message="No files found to prepare",
                    section="END",
                )
                return "Done"
            self.logger.info(f"Preparing {len(file_nodes)} files")
        except CogniteAPIError as e:
            if not is_query_timeout(e):
                raise
            self.query_timeout.wait(e)
            return None
        self.query_timeout.reset()

        annotation_state_instances: list[NodeApply] = []
        file_apply_instances: list[NodeApply] = []
        for file_node in file_nodes:
            node_id = {"space": file_node.space, "externalId": file_node.external_id}
            annotation_instance = AnnotationState(
                annotationStatus=AnnotationStatus.NEW,
                linkedFile=node_id,
            )
            annotation_instance_space: str = self.annotation_state_view.instance_space or file_node.space
            annotation_node_apply: NodeApply = annotation_instance.to_node_apply(
                node_space=annotation_instance_space,
                annotation_state_view=self.annotation_state_view.as_view_id(),
            )
            annotation_state_instances.append(annotation_node_apply)

            tags_property = node_tags(file_node, self.file_view.as_view_id())
            if "AnnotationInProcess" not in tags_property:
                tags_property.append("AnnotationInProcess")
                file_apply_instances.append(tags_apply(file_node, self.file_view.as_view_id(), tags_property))

        try:
            create_results = self.data_model_service.create_annotation_state(annotation_state_instances)
            self.logger.info(message=f"Created {len(create_results)} annotation state instances")
            update_results = self.data_model_service.update_annotation_state(file_apply_instances)
            self.logger.info(
                message=f"Added 'AnnotationInProcess' to the tag property for {len(update_results)} files",
                section="END",
            )
        except CogniteAPIError as e:
            self.logger.error(message="Ran into the following error", error=e, section="END")
            raise

        self.tracker.add_files(success=len(file_nodes))
        return None
