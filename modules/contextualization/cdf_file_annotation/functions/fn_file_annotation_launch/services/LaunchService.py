import time
import abc
from typing import Literal
from datetime import datetime, timezone
from collections import defaultdict
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.contextualization import FileReference
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeList,
    NodeApply,
    NodeOrEdgeData,
)

from services.ConfigService import Config, ViewPropertyConfig
from services.CacheService import ICacheService
from services.AnnotationService import IAnnotationService
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import (
    BatchOfNodes,
    AnnotationStatus,
    PerformanceTracker,
    FileProcessingBatch,
)


class AbstractLaunchService(abc.ABC):
    """
    Orchestrates the file annotation launch process. This service prepares files for annotation,
    manages batching and caching, and initiates diagram detection jobs.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        data_model_service: IDataModelService,
        cache_service: ICacheService,
        annotation_service: IAnnotationService,
        function_id: int | None = None,
        function_call_id: int | None = None,
    ):
        self.client = client
        self.config = config
        self.logger = logger
        self.tracker = tracker

        self.data_model_service = data_model_service
        self.cache_service = cache_service
        self.annotation_service = annotation_service

        self.function_id = function_id
        self.function_call_id = function_call_id

    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralLaunchService(AbstractLaunchService):
    """
    Orchestrates the file annotation launch process. This service prepares files for annotation,
    manages batching and caching, and initiates diagram detection jobs.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        data_model_service: IDataModelService,
        cache_service: ICacheService,
        annotation_service: IAnnotationService,
        function_id: int | None = None,
        function_call_id: int | None = None,
    ):
        super().__init__(
            client,
            config,
            logger,
            tracker,
            data_model_service,
            cache_service,
            annotation_service,
            function_id,
            function_call_id,
        )

        self.max_batch_size: int = config.launch_function.batch_size
        self.page_range: int = config.launch_function.annotation_service.page_range
        self.contextualization_file_view: ViewPropertyConfig = config.data_model_views.contextualization_file_view

        self.in_memory_cache: list[dict] = []
        self._cached_primary_scope: str | None = None
        self._cached_secondary_scope: str | None = None

        self.primary_scope_property: str = self.config.launch_function.primary_scope_property
        self.secondary_scope_property: str | None = self.config.launch_function.secondary_scope_property

        self.reset_files: bool = False
        if self.config.launch_function.data_model_service.get_files_for_annotation_reset_query:
            self.reset_files = True

    def run(self) -> Literal["Done"] | None:
        """
        The main entry point for the launch service. It prepares the files and then
        processes them in organized, context-aware batches.
        """
        if self.reset_files:
            self.logger.info("Checking for files to reset based on 'getFilesForAnnotationResetQuery'.")
            try:
                # Assumes get_files_for_annotation_reset is added back to DataModelService
                file_nodes_to_reset: NodeList | None = self.data_model_service.get_files_for_annotation_reset()

                if file_nodes_to_reset:
                    self.logger.info(f"Found {len(file_nodes_to_reset)} files to reset.")
                    reset_node_applies: list[NodeApply] = []

                    # Define the complete set of properties to reset
                    properties_to_reset = {
                        "annotationStatus": AnnotationStatus.NEW,
                        "annotationMessage": None,
                        "annotationAttemptCount": 0,
                        "annotatedPageCount": None,
                        "diagramDetectJobId": None,
                        "diagramDetectAnnotationResults": None,
                        "diagramDetectConfiguration": None,
                        "diagramDetectCreatedTime": None,
                        "diagramDetectStatusTime": None,
                        "totalPageCount": None,
                        "launchFunctionId": None,
                        "launchFunctionCallId": None,
                        "finalizeFunctionId": None,
                        "finalizeFunctionCallId": None,
                    }

                    for file_node in file_nodes_to_reset:
                        if file_node.properties[self.contextualization_file_view.as_view_id()].get("annotationStatus"):
                            node_apply = NodeApply(
                                space=file_node.space,
                                external_id=file_node.external_id,
                                existing_version=file_node.version,
                                sources=[
                                    NodeOrEdgeData(
                                        source=self.contextualization_file_view.as_view_id(),
                                        properties=properties_to_reset,
                                    )
                                ],
                            )
                            reset_node_applies.append(node_apply)

                    # Apply the updates
                    update_results = self.data_model_service.update_annotation_state(reset_node_applies)
                    self.logger.info(f"Successfully reset the annotation state for {len(update_results)} files.")

                else:
                    self.logger.info("No files matched the reset query.")

                # Ensure this block only runs once per function execution
                self.reset_files = False

            except CogniteAPIError as e:
                self.logger.error(f"Error during file reset process: {e}")
                # Decide if you should raise or continue
                raise

        self.logger.info(
            message=f"Starting Launch Function",
            section="START",
        )
        try:
            file_nodes: NodeList[Node] | None = self.data_model_service.get_files_to_process()
            if not file_nodes:
                self.logger.info(message=f"No files found to launch")
                return "Done"
            self.logger.info(message=f"Launching {len(file_nodes)} files", section="END")
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

        processing_batches: list[FileProcessingBatch] = self._organize_files_for_processing(file_nodes)

        total_files_processed = 0
        try:
            for batch in processing_batches:
                primary_scope_value = batch.primary_scope_value
                secondary_scope_value = batch.secondary_scope_value
                msg = f"{self.primary_scope_property}: {primary_scope_value}"
                if secondary_scope_value:
                    msg += f", {self.secondary_scope_property}: {secondary_scope_value}"
                self.logger.info(message=f"Processing {len(batch.files)} files in {msg}")
                self._ensure_cache_for_batch(primary_scope_value, secondary_scope_value)

                current_batch = BatchOfNodes()
                for file_node in batch.files:
                    file_reference: FileReference = current_batch.create_file_reference(
                        file_node=file_node,
                        page_range=self.page_range,
                        contextualizaton_file_view=self.contextualization_file_view.as_view_id(),
                    )
                    current_batch.add(file_node, file_reference)
                    total_files_processed += 1
                    if current_batch.size() == self.max_batch_size:
                        self.logger.info(message=f"Processing batch - Max batch size ({self.max_batch_size}) reached")
                        self._process_batch(current_batch)
                if not current_batch.is_empty():
                    self.logger.info(message=f"Processing remaining {current_batch.size()} files in batch")
                    self._process_batch(current_batch)
                self.logger.info(message=f"Finished processing for {msg}", section="END")
        except CogniteAPIError as e:
            if e.code == 429:
                self.logger.debug(f"{str(e)}")
                self.logger.info(
                    "Reached the max amount of jobs that can be processed by the server at once.",
                    "END",
                )
                return "Done"
            else:
                raise e
        finally:
            self.tracker.add_files(success=total_files_processed)

        return

    def _organize_files_for_processing(self, list_files: NodeList) -> list[FileProcessingBatch]:
        """
        Groups files based on the 'primary_scope_property' and 'secondary_scope_property'
        defined in the configuration. This strategy allows us to load a relevant entity cache
        once for a group of files that share the same operational context, significantly
        reducing redundant CDF queries.
        """
        organized_data: dict[str, dict[str, list[Node]]] = defaultdict(lambda: defaultdict(list))

        for file_node in list_files:
            node_props = file_node.properties[self.contextualization_file_view.as_view_id()]
            primary_value = node_props.get(self.primary_scope_property)
            secondary_value = "__NONE__"
            if self.secondary_scope_property:
                secondary_value = node_props.get(self.secondary_scope_property)
            organized_data[primary_value][secondary_value].append(file_node)

        final_processing_batches: list[FileProcessingBatch] = []
        for primary_property in sorted(organized_data.keys()):
            groups = organized_data[primary_property]
            for secondary_property in sorted(groups.keys()):
                files_in_batch = groups[secondary_property]
                if secondary_property == "__NONE__":
                    actual_secondary_property = None
                else:
                    actual_secondary_property = secondary_property
                final_processing_batches.append(
                    FileProcessingBatch(
                        primary_scope_value=primary_property,
                        secondary_scope_value=actual_secondary_property,
                        files=files_in_batch,
                    )
                )
                self.logger.info(
                    message=f"Created batch of {len(files_in_batch)} files for {self.primary_scope_property}: {primary_property}, {self.secondary_scope_property}: {secondary_property}",
                    section="END",
                )
        return final_processing_batches

    def _ensure_cache_for_batch(self, primary_scope_value: str, secondary_scope_value: str | None):
        """
        Ensure self.in_memory_cache is populated for the given site and unit.
        Checks if there's a mismatch in site, unit, or if the in_memory_cache is empty
        """
        if (
            self._cached_primary_scope != primary_scope_value
            or self._cached_secondary_scope != secondary_scope_value
            or not self.in_memory_cache
        ):
            self.logger.info(f"Refreshing in memory cache")
            try:
                self.in_memory_cache = self.cache_service.get_entities(
                    self.data_model_service, primary_scope_value, secondary_scope_value
                )
                self._cached_primary_scope = primary_scope_value
                self._cached_secondary_scope = secondary_scope_value
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

    def _process_batch(self, batch: BatchOfNodes):
        """
        Processes a single batch of files. For each file, it starts a diagram
        detection job and then updates the file with the cooresponding information.
        """
        if batch.is_empty():
            return

        self.logger.info(f"Running diagram detect on {batch.size()} files with {len(self.in_memory_cache)} entities")

        try:
            job_id: int = self.annotation_service.run_diagram_detect(
                files=batch.file_references, entities=self.in_memory_cache
            )
            update_properties = {
                "annotationStatus": AnnotationStatus.PROCESSING,
                "diagramDetectCreatedTime": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "diagramDetectJobId": job_id,
                "launchFunctionId": self.function_id,
                "launchFunctionCallId": self.function_call_id,
            }
            batch.update_node_properties(
                new_properties=update_properties,
                view_id=self.contextualization_file_view.as_view_id(),
            )
            update_results = self.data_model_service.update_annotation_state(batch.apply)
            self.logger.info(
                message=f" Updated the annotation state:\n- annotation status set to 'Processing'\n- job id set to {job_id}",
                section="END",
            )
        finally:
            batch.clear


class LocalLaunchService(GeneralLaunchService):
    """
    A Launch service that uses a custom, local process for handling batches,
    while inheriting all other functionality from GeneralLaunchService.
    """

    def _process_batch(self, batch: BatchOfNodes):
        """
        This method overrides the original _process_batch.
        Instead of calling the annotation service, it could, for example,
        process the files locally.
        """
        if batch.is_empty():
            return

        self.logger.info(f"Running diagram detect on {batch.size()} files with {len(self.in_memory_cache)} entities")

        try:
            job_id: int = self.annotation_service.run_diagram_detect(
                files=batch.file_references, entities=self.in_memory_cache
            )
            update_properties = {
                "annotationStatus": AnnotationStatus.PROCESSING,
                "diagramDetectCreatedTime": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "diagramDetectJobId": job_id,
                "launchFunctionId": self.function_id,
                "launchFunctionCallId": self.function_call_id,
            }
            batch.update_node_properties(
                new_properties=update_properties,
                view_id=self.contextualization_file_view.as_view_id(),
            )
            update_results = self.data_model_service.update_annotation_state(batch.apply)
            self.logger.info(
                message=f" Updated the annotation state instances:\n- annotation status set to 'Processing'\n- job id set to {job_id}",
                section="END",
            )
        except CogniteAPIError as e:
            if e.code == 429:
                self.logger.debug(f"{str(e)}")
                self.logger.info(
                    "Reached the max amount of jobs that can be processed by the server at once.\nSleeping for 15 minutes",
                    "END",
                )
                time.sleep(
                    900
                )  # in a local run the ideal behavior is to not terminate the program because of this error, since it's expected
                return
            else:
                self.logger.error(f"Ran into the following error:\n{str(e)}")
                raise Exception(e)
        finally:
            batch.clear()
