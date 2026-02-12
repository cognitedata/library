import abc
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Literal

from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import FileReference
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeList,
)
from cognite.client.exceptions import CogniteAPIError
from services.AnnotationService import IAnnotationService
from services.CacheService import ICacheService
from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import (
    AnnotationStatus,
    BatchOfPairedNodes,
    FileProcessingBatch,
    PerformanceTracker,
)


class AbstractLaunchService(abc.ABC):
    """
    Orchestrates the file annotation launch process. This service manages batching and caching,
    and initiates diagram detection jobs for files ready to be annotated.
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
    ):
        self.client = client
        self.config = config
        self.logger = logger
        self.tracker = tracker
        self.data_model_service = data_model_service
        self.cache_service = cache_service
        self.annotation_service = annotation_service

    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralLaunchService(AbstractLaunchService):
    """
    Orchestrates the file annotation launch process. This service manages batching and caching,
    and initiates diagram detection jobs for files ready to be annotated.
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
        function_call_info: dict,
    ):
        super().__init__(
            client,
            config,
            logger,
            tracker,
            data_model_service,
            cache_service,
            annotation_service,
        )

        self.max_batch_size: int = config.launch_function.batch_size
        self.page_range: int = config.launch_function.annotation_service.page_range
        self.annotation_state_view: ViewPropertyConfig = config.data_model_views.annotation_state_view
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view

        self.in_memory_cache: list[dict] = []
        self.in_memory_patterns: list[dict] = []
        self._cached_primary_scope: str | None = None
        self._cached_secondary_scope: str | None = None

        self.primary_scope_property: str = self.config.launch_function.primary_scope_property
        self.secondary_scope_property: str | None = self.config.launch_function.secondary_scope_property

        self.function_id: int | None = function_call_info.get("function_id")
        self.call_id: int | None = function_call_info.get("call_id")

    def run(self) -> Literal["Done"] | None:
        """
        Main execution loop for launching diagram detection jobs.

        Retrieves files ready for processing, organizes them into context-aware batches based on scope,
        ensures appropriate entity caches are loaded, and initiates diagram detection jobs for each batch.

        Args:
            None

        Returns:
            "Done" if no more files to process or max jobs reached, None if processing should continue.

        Raises:
            CogniteAPIError: If query timeout (408) or max jobs reached (429), handled gracefully.
        """
        self.logger.info(
            message="Starting Launch Function",
            section="START",
        )
        try:
            file_nodes, file_to_state_map = self.data_model_service.get_files_to_process()
            if not file_nodes or not file_to_state_map:
                self.logger.info(message="No files found to launch")
                return "Done"
            self.logger.info(message=f"Launching {len(file_nodes)} files", section="END")
        except CogniteAPIError as e:
            # NOTE: Reliant on the CogniteAPI message to stay the same across new releases. If unexpected changes were to occur please refer to this section of the code and check if error message is now different.
            if (
                e.code == 408
                and e.message == "Graph query timed out. Reduce load or contention, or optimise your query."
            ):
                # NOTE: 408 indicates a timeout error. Keep retrying the query if a timeout occurs.
                self.logger.error(message="Ran into the following error", error=e)
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

                current_batch = BatchOfPairedNodes(file_to_state_map=file_to_state_map)
                for file_node in batch.files:
                    file_reference: FileReference = current_batch.create_file_reference(
                        file_node_id=file_node.as_id(),
                        page_range=self.page_range,
                        annotation_state_view_id=self.annotation_state_view.as_view_id(),
                    )
                    current_batch.add_pair(file_node, file_reference)
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
        Organizes files into batches grouped by scope for efficient processing.

        Groups files based on primary and secondary scope properties defined in configuration.
        This strategy enables loading a relevant entity cache once per group, significantly
        reducing redundant CDF queries for files sharing the same operational context.

        Args:
            list_files: NodeList of file instances to organize into batches.

        Returns:
            List of FileProcessingBatch objects, each containing files from the same scope.
        """
        organized_data: dict[str, dict[str, list[Node]]] = defaultdict(lambda: defaultdict(list))

        for file_node in list_files:
            node_props = file_node.properties[self.file_view.as_view_id()]
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
        Ensures the in-memory entity cache is loaded and current for the given scope.

        Checks if cache needs refreshing (scope mismatch or empty cache) and fetches fresh
        entities and patterns from the cache service if needed.

        Args:
            primary_scope_value: Primary scope identifier for the batch being processed.
            secondary_scope_value: Optional secondary scope identifier for the batch.

        Returns:
            None

        Raises:
            CogniteAPIError: If query timeout (408) occurs, handled gracefully by returning early.
        """
        if (
            self._cached_primary_scope != primary_scope_value
            or self._cached_secondary_scope != secondary_scope_value
            or not self.in_memory_cache
        ):
            self.logger.info("Refreshing in memory cache")
            try:
                self.in_memory_cache, self.in_memory_patterns = self.cache_service.get_entities(
                    self.data_model_service,
                    primary_scope_value,
                    secondary_scope_value,
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
                    self.logger.error(message="Ran into the following error", error=e)
                    return
                else:
                    raise e

    def _process_batch(self, batch: BatchOfPairedNodes):
        """
        Processes a batch of files by initiating diagram detection jobs and updating state.

        Runs both regular and pattern mode diagram detection (if enabled) for all files in the batch,
        then updates annotation state instances with job IDs and processing status.

        Args:
            batch: BatchOfPairedNodes containing file references and their annotation state nodes.

        Returns:
            None

        Raises:
            CogniteAPIError: If max concurrent jobs reached (429), handled gracefully.
        """
        if batch.is_empty():
            return
        try:
            self._run_diagram_detect_and_update_state(batch)
        finally:
            batch.clear_pair()

    def _run_diagram_detect_and_update_state(self, batch: BatchOfPairedNodes):
        """
        Core logic for running diagram detection and updating annotation state.

        Args:
            batch: BatchOfPairedNodes containing file references and their annotation state nodes.
        """
        self.logger.info(
            f"Running diagram detect on {batch.size()} files with {len(self.in_memory_cache)} entities"
        )
        job_id, job_token = self.annotation_service.run_diagram_detect(
            files=batch.file_references, entities=self.in_memory_cache
        )
        update_properties = {
            "annotationStatus": AnnotationStatus.PROCESSING,
            "sourceUpdatedTime": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "diagramDetectJobId": job_id,
            "diagramDetectJobToken": job_token,
            "launchFunctionId": self.function_id,
            "launchFunctionCallId": self.call_id,
        }

        pattern_job_id, pattern_job_token = self._run_pattern_mode_if_enabled(batch)
        if pattern_job_id:
            update_properties["patternModeJobId"] = pattern_job_id
            update_properties["patternModeJobToken"] = pattern_job_token

        batch.batch_states.update_node_properties(
            new_properties=update_properties,
            view_id=self.annotation_state_view.as_view_id(),
        )
        self.data_model_service.update_annotation_state(batch.batch_states.apply)
        self.logger.info(
            message=f"Updated the annotation state instances:\n- annotation status set to 'Processing'\n- job set to (id: {job_id}, token: {job_token})\n- pattern mode job set to (id: {pattern_job_id}, token: {pattern_job_token})",
            section="END",
        )

    def _run_pattern_mode_if_enabled(self, batch: BatchOfPairedNodes) -> tuple[int | None, str | None]:
        """
        Runs pattern mode diagram detection if enabled in configuration.

        Args:
            batch: BatchOfPairedNodes containing file references.

        Returns:
            Tuple of (pattern_job_id, pattern_job_token) or (None, None) if pattern mode is disabled.
        """
        if not self.config.launch_function.pattern_mode:
            return None, None

        total_patterns = sum(
            len(p.get("sample", [])) for p in self.in_memory_patterns[:2]
        ) if self.in_memory_patterns else 0

        self.logger.info(
            f"Running pattern mode diagram detect on {batch.size()} files with {total_patterns} sample patterns"
        )
        return self.annotation_service.run_pattern_mode_detect(
            files=batch.file_references, pattern_samples=self.in_memory_patterns
        )


class LocalLaunchService(GeneralLaunchService):
    """
    Launch service variant for local development and debugging.

    Extends GeneralLaunchService with custom error handling for local runs, including
    sleep/retry logic for API rate limiting rather than immediate termination.
    """

    def _process_batch(self, batch: BatchOfPairedNodes):
        """
        Processes a batch with local-specific error handling.

        Extends the base _process_batch with additional error handling suitable for local runs,
        including automatic retry with sleep on rate limit errors (429) rather than terminating.

        Args:
            batch: BatchOfPairedNodes containing file references and their annotation state nodes.

        Returns:
            None

        Raises:
            Exception: If non-rate-limit errors occur.
        """
        if batch.is_empty():
            return

        try:
            self._run_diagram_detect_and_update_state(batch)
        except CogniteAPIError as e:
            if e.code == 429:
                self.logger.debug(str(e))
                self.logger.info(
                    "Reached the max amount of jobs that can be processed by the server at once.\nSleeping for 15 minutes",
                    "END",
                )
                time.sleep(900)  # Local run: wait and retry instead of terminating
                return
            self.logger.error(message="Ran into the following error", error=e)
            raise
        finally:
            batch.clear_pair()
