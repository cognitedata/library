import abc
from typing import cast, Literal
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.data_modeling import (
    NodeList,
    NodeApply,
)

from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import (
    AnnotationStatus,
    AnnotationState,
    PerformanceTracker,
)


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
            CogniteAPIError: If query timeout or other API errors occur (408 errors are handled gracefully).
            ValueError: If annotation state view instance space is not configured.
        """
        self.logger.info(
            message=f"Starting Prepare Function",
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
                    for file_node in file_nodes_to_reset:
                        file_node_apply: NodeApply = file_node.as_write()
                        tags_property: list[str] = cast(list[str], file_node_apply.sources[0].properties["tags"])
                        if "AnnotationInProcess" in tags_property:
                            tags_property.remove("AnnotationInProcess")
                        if "Annotated" in tags_property:
                            tags_property.remove("Annotated")
                        if "AnnotationFailed" in tags_property:
                            tags_property.remove("AnnotationFailed")

                        reset_node_apply.append(file_node_apply)
                    update_results = self.data_model_service.update_annotation_state(reset_node_apply)
                    self.logger.info(
                        f"Removed the AnnotationInProcess/Annotated/AnnotationFailed tag of {len(update_results)} files"
                    )
                self.reset_files = False
        except CogniteAPIError as e:
            # NOTE: Reliant on the CogniteAPI message to stay the same across new releases. If unexpected changes were to occur please refer to this section of the code and check if error message is now different.
            if (
                e.code == 408
                and e.message == "Graph query timed out. Reduce load or contention, or optimise your query."
            ):
                # NOTE: 408 indicates a timeout error. Keep retrying the query if a timeout occurs.
                self.logger.error(message=f"Ran into the following error:\n{str(e)}")
                return
            else:
                raise e

        try:
            file_nodes: NodeList | None = self.data_model_service.get_files_to_annotate()
            if not file_nodes:
                self.logger.info(
                    message=f"No files found to prepare",
                    section="END",
                )
                return "Done"
            self.logger.info(f"Preparing {len(file_nodes)} files")
        except CogniteAPIError as e:
            # NOTE: Reliant on the CogniteAPI message to stay the same across new releases. If unexpected changes were to occur please refer to this section of the code and check if error message is now different.
            if (
                e.code == 408
                and e.message == "Graph query timed out. Reduce load or contention, or optimise your query."
            ):
                # NOTE: 408 indicates a timeout error. Keep retrying the query if a timeout occurs.
                self.logger.error(message=f"Ran into the following error:\n{str(e)}")
                return
            else:
                raise e

        annotation_state_instances: list[NodeApply] = []
        file_apply_instances: list[NodeApply] = []
        for file_node in file_nodes:
            node_id = {"space": file_node.space, "externalId": file_node.external_id}
            annotation_instance = AnnotationState(
                annotationStatus=AnnotationStatus.NEW,
                linkedFile=node_id,
            )
            if not self.annotation_state_view.instance_space:
                msg = (
                    "Need an instance space in DataModelViews/AnnotationStateView config to store the annotation state"
                )
                self.logger.error(msg)
                raise ValueError(msg)
            annotation_instance_space: str = self.annotation_state_view.instance_space

            annotation_node_apply: NodeApply = annotation_instance.to_node_apply(
                node_space=annotation_instance_space,
                annotation_state_view=self.annotation_state_view.as_view_id(),
            )
            annotation_state_instances.append(annotation_node_apply)

            file_node_apply: NodeApply = file_node.as_write()
            tags_property: list[str] = cast(list[str], file_node_apply.sources[0].properties["tags"])
            if "AnnotationInProcess" not in tags_property:
                tags_property.append("AnnotationInProcess")
                file_apply_instances.append(file_node_apply)

        try:
            create_results = self.data_model_service.create_annotation_state(annotation_state_instances)
            self.logger.info(message=f"Created {len(create_results)} annotation state instances")
            update_results = self.data_model_service.update_annotation_state(file_apply_instances)
            self.logger.info(
                message=f"Added 'AnnotationInProcess' to the tag property for {len(update_results)} files",
                section="END",
            )
        except Exception as e:
            self.logger.error(message=f"Ran into the following error:\n{str(e)}", section="END")
            raise

        self.tracker.add_files(success=len(file_nodes))
        return


class LocalPrepareService(GeneralPrepareService):
    """
    Prepare service variant for local development and debugging.

    Extends GeneralPrepareService with any local-specific behavior if needed.
    """

    pass
