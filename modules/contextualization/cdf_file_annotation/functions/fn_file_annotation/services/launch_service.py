import abc
import json
import time
from collections import defaultdict
from typing import Literal

from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import FileReference
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeApply,
    NodeId,
    NodeList,
    NodeOrEdgeData,
)
from cognite.client.exceptions import CogniteAPIError
from fa_constants import LOCAL_RATE_LIMIT_SLEEP_SECONDS, TAG_ANNOTATION_FAILED, TAG_ANNOTATION_IN_PROCESS
from services.annotation_service import IAnnotationService
from services.config_service import Config, ViewPropertyConfig
from services.data_model_service import IDataModelService
from services.entity_cache_service import ICacheService, count_pattern_sample_strings, split_entities_by_kind
from services.entity_sync_service import EntitySyncIncompleteError
from services.logger_service import CogniteFunctionLogger
from utils.data_structures import (
    AnnotationStatus,
    BatchOfPairedNodes,
    FileProcessingBatch,
    PerformanceTracker,
    annotation_clock,
    node_tags,
    replace_tag,
    tags_apply,
    unique_tags,
)
from utils.query_timeout import QueryTimeoutRetry, is_query_timeout


def _scope_group_value(node_props: object, property_name: str | None) -> str:
    """Text of a scope property. A missing or non-text value is '' so batch sorting never sees None."""
    if not property_name or not isinstance(node_props, dict):
        return ""
    raw_value = node_props.get(property_name)
    if isinstance(raw_value, str):
        return raw_value
    return ""


def launch_state_update_message(job_id: int | None, pattern_job_id: int | None) -> str:
    """Summary of the jobs stored on the annotation state. Job tokens are not included."""
    return (
        "Updated the annotation state instances:\n"
        "- annotation status set to 'Processing'\n"
        f"- diagram detect job id: {job_id}\n"
        f"- pattern mode job id: {pattern_job_id}"
    )


class RateLimitPolicy(abc.ABC):
    """Define how a runtime reacts when Diagram Detect returns HTTP 429."""

    @abc.abstractmethod
    def handle(self, logger: CogniteFunctionLogger) -> Literal["Done"] | None:
        """Handle rate limiting and return the launch-loop result."""


class DeployedRateLimitPolicy(RateLimitPolicy):
    def handle(self, logger: CogniteFunctionLogger) -> Literal["Done"]:
        logger.info("Reached the maximum number of concurrent Diagram Detect jobs.", "END")
        return "Done"


class LocalRateLimitPolicy(RateLimitPolicy):
    def handle(self, logger: CogniteFunctionLogger) -> None:
        logger.info(
            f"Reached the maximum number of concurrent Diagram Detect jobs. "
            f"Sleeping for {LOCAL_RATE_LIMIT_SLEEP_SECONDS} seconds.",
            "END",
        )
        time.sleep(LOCAL_RATE_LIMIT_SLEEP_SECONDS)


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
        rate_limit_policy: RateLimitPolicy,
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
        self._cached_scope: tuple[str | None, str, str | None] | None = None

        self.primary_scope_property: str | None = self.config.launch_function.primary_scope_property
        self.secondary_scope_property: str | None = self.config.launch_function.secondary_scope_property
        target_view = config.data_model_views.target_entities_view
        self.group_by_file_space: bool = not self.file_view.instance_space or not target_view.instance_space

        self.function_id: int | None = function_call_info.get("function_id")
        self.call_id: int | None = function_call_info.get("call_id")
        self.rate_limit_policy = rate_limit_policy
        self.query_timeout = QueryTimeoutRetry(logger)

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
            if not (self.primary_scope_property or "").strip():
                self.logger.warning(
                    message=(
                        "primaryScopeProperty is not set: all targets / assets will be used to match against "
                        "all files, with the possibility of false positive matching"
                    )
                )
        except CogniteAPIError as e:
            if not is_query_timeout(e):
                raise
            self.query_timeout.wait(e)
            return None

        processing_batches: list[FileProcessingBatch] = self._organize_files_for_processing(file_nodes)

        counts = {"launched": 0, "failed": 0}
        # Files whose state this run wrote: launched, or marked failed. The rest are released on error.
        launched_file_ids: set[NodeId] = set()

        def process(current_batch: BatchOfPairedNodes) -> None:
            batch_file_ids = list(current_batch.batch_files.ids)
            outcome = "launched" if self._process_batch(current_batch) else "failed"
            counts[outcome] += len(batch_file_ids)
            launched_file_ids.update(batch_file_ids)

        try:
            for batch in processing_batches:
                primary_scope_value = batch.primary_scope_value
                secondary_scope_value = batch.secondary_scope_value
                scoped = bool(self.primary_scope_property) or batch.file_space is not None
                if scoped:
                    msg = self._describe_scope(batch)
                    self.logger.info(message=f"Processing {len(batch.files)} files in {msg}")
                self._ensure_cache_for_batch(primary_scope_value, secondary_scope_value, batch.file_space)

                current_batch = BatchOfPairedNodes(file_to_state_map=file_to_state_map)
                for file_node in batch.files:
                    file_reference: FileReference = current_batch.create_file_reference(
                        file_node_id=file_node.as_id(),
                        page_range=self.page_range,
                        annotation_state_view_id=self.annotation_state_view.as_view_id(),
                    )
                    current_batch.add_pair(file_node, file_reference)
                    if current_batch.size() == self.max_batch_size:
                        self.logger.info(message=f"Processing batch - Max batch size ({self.max_batch_size}) reached")
                        process(current_batch)
                if not current_batch.is_empty():
                    self.logger.info(message=f"Processing remaining {current_batch.size()} files in batch")
                    process(current_batch)
                if scoped:
                    self.logger.info(message=f"Finished processing for {msg}", section="END")
        except CogniteAPIError as e:
            if e.code == 429:
                self.logger.debug(f"{e!s}")
                return self.rate_limit_policy.handle(self.logger)
            elif is_query_timeout(e):
                try:
                    self.query_timeout.wait(e)
                except CogniteAPIError:
                    self._release_unlaunched_files(file_nodes, launched_file_ids)
                    raise
                return None
            else:
                self._release_unlaunched_files(file_nodes, launched_file_ids)
                raise e
        except EntitySyncIncompleteError as e:
            # The files keep their claim; the next run continues the read from the stored cursor.
            self.logger.info(f"{e}. Unlaunched files are launched once the read has finished.")
            return None
        except Exception:
            # Re-raised after releasing, so any failure frees the files this run claimed.
            self._release_unlaunched_files(file_nodes, launched_file_ids)
            raise
        finally:
            self.tracker.add_files(success=counts["launched"], failed=counts["failed"])

        self.query_timeout.reset()
        return None

    def _release_unlaunched_files(self, file_nodes: NodeList, launched_file_ids: set[NodeId]) -> None:
        """
        Removes the 'AnnotationInProcess' tag from the files this run claimed but never launched.

        Prepare skips files that carry the tag, so a failed launch would otherwise leave them
        out of the pipeline until someone removes the tag by hand.

        Args:
            file_nodes: The files retrieved for this run.
            launched_file_ids: NodeIds of the files that made it into a diagram detect job.

        Returns:
            None
        """
        file_view_id = self.file_view.as_view_id()
        release_applies: list[NodeApply] = []
        for file_node in file_nodes:
            if file_node.as_id() in launched_file_ids:
                continue
            tags = node_tags(file_node, file_view_id)
            if TAG_ANNOTATION_IN_PROCESS not in tags:
                continue
            remaining_tags = [tag for tag in unique_tags(tags) if tag != TAG_ANNOTATION_IN_PROCESS]
            release_applies.append(
                NodeApply(
                    space=file_node.space,
                    external_id=file_node.external_id,
                    sources=[NodeOrEdgeData(source=file_view_id, properties={"tags": remaining_tags})],
                )
            )

        if not release_applies:
            return
        try:
            self.data_model_service.update_annotation_state(release_applies)
            self.logger.info(
                message=(
                    f"Launch failed: removed '{TAG_ANNOTATION_IN_PROCESS}' from {len(release_applies)} files "
                    "so they can be picked up again"
                )
            )
        except CogniteAPIError as e:
            self.logger.error(
                message=f"Could not remove '{TAG_ANNOTATION_IN_PROCESS}' from the files of the failed launch",
                error=e,
            )

    def _organize_files_for_processing(self, list_files: NodeList) -> list[FileProcessingBatch]:
        """
        Organizes files into batches grouped by scope for efficient processing.

        Groups files by their instance space (when a view has no instanceSpace) and by the
        primary and secondary scope properties defined in configuration. This strategy enables
        loading a relevant entity cache once per group, significantly reducing redundant CDF
        queries for files sharing the same operational context.

        Args:
            list_files: NodeList of file instances to organize into batches.

        Returns:
            List of FileProcessingBatch objects, each containing files from the same scope.
        """
        organized_data: dict[tuple[str | None, str, str], list[Node]] = defaultdict(list)

        for file_node in list_files:
            node_props = (file_node.properties or {}).get(self.file_view.as_view_id()) or {}
            primary_value = _scope_group_value(node_props, self.primary_scope_property)
            # Secondary only applies inside a primary scope; without primary, treat as fully unscoped
            # so the file is matched against all tagged assets (scoped and unscoped).
            secondary_value = (
                _scope_group_value(node_props, self.secondary_scope_property)
                if self.secondary_scope_property and primary_value
                else "__NONE__"
            )
            file_space = file_node.space if self.group_by_file_space else None
            organized_data[(file_space, primary_value, secondary_value)].append(file_node)

        final_processing_batches: list[FileProcessingBatch] = []
        for file_space, primary_property, secondary_property in sorted(organized_data):
            batch = FileProcessingBatch(
                primary_scope_value=primary_property,
                secondary_scope_value=None if secondary_property in {"__NONE__", ""} else secondary_property,
                files=organized_data[(file_space, primary_property, secondary_property)],
                file_space=file_space,
            )
            final_processing_batches.append(batch)
            if self.primary_scope_property or file_space is not None:
                self.logger.info(
                    message=f"Created batch of {len(batch.files)} files for {self._describe_scope(batch)}",
                    section="END",
                )
        unscoped_files = sum(len(batch.files) for batch in final_processing_batches if not batch.primary_scope_value)
        if (self.primary_scope_property or "").strip() and unscoped_files:
            self.logger.warning(
                message=(
                    f"Files without scope (primary='', secondary='') - {unscoped_files} is tried matched against "
                    "all assets. Unscoped assets are tried matched against all documents"
                )
            )
        return final_processing_batches

    def _describe_scope(self, batch: FileProcessingBatch) -> str:
        """Human-readable scope of a batch for logs, e.g. 'space: plant_a, site: PlantA'."""
        parts: list[str] = []
        if batch.file_space is not None:
            parts.append(f"space: {batch.file_space}")
        if self.primary_scope_property:
            parts.append(f"{self.primary_scope_property}: {batch.primary_scope_value}")
        if batch.secondary_scope_value:
            parts.append(f"{self.secondary_scope_property}: {batch.secondary_scope_value}")
        return ", ".join(parts)

    def _ensure_cache_for_batch(
        self, primary_scope_value: str, secondary_scope_value: str | None, file_space: str | None
    ) -> None:
        """
        Ensures the in-memory entity cache is loaded and current for the given scope.

        Checks if cache needs refreshing (scope mismatch or empty cache) and fetches fresh
        entities and patterns from the cache service if needed.

        Args:
            primary_scope_value: Primary scope identifier for the batch being processed.
            secondary_scope_value: Optional secondary scope identifier for the batch.
            file_space: Instance space of the batch's files when entities are read per file space.

        Returns:
            None

        Raises:
            CogniteAPIError: If query timeout (408) occurs, handled gracefully by returning early.
        """
        scope = (file_space, primary_scope_value, secondary_scope_value)
        if self._cached_scope != scope or not self.in_memory_cache:
            self.logger.info("Refreshing in memory cache")
            self.in_memory_cache, self.in_memory_patterns = self.cache_service.get_entities(
                self.data_model_service,
                primary_scope_value,
                secondary_scope_value,
                file_space,
            )
            self._cached_scope = scope
            assets, files = split_entities_by_kind(self.in_memory_cache)
            pattern_count = count_pattern_sample_strings(self.in_memory_patterns)
            self.tracker.set_detect_input(
                entities=len(self.in_memory_cache),
                patterns=pattern_count if self.config.launch_function.pattern_mode else None,
            )
            self.logger.info(
                f"In-memory cache ready for scope space={file_space!r} primary={primary_scope_value!r} "
                f"secondary={secondary_scope_value!r}: "
                f"{len(assets)} assets, {len(files)} files, "
                f"{pattern_count} pattern sample string(s)"
            )

    def _process_batch(self, batch: BatchOfPairedNodes) -> bool:
        """
        Processes a batch of files by initiating diagram detection jobs and updating state.

        Runs both regular and pattern mode diagram detection (if enabled) for all files in the batch,
        then updates annotation state instances with job IDs and processing status. A batch whose scope
        has neither entities nor patterns is marked Failed, so Launch does not pick the same files up again.

        Args:
            batch: BatchOfPairedNodes containing file references and their annotation state nodes.

        Returns:
            True when a detect job was launched, False when the files were marked failed.

        Raises:
            CogniteAPIError: If max concurrent jobs reached (429) before any job for this batch started.
                When pattern-mode fails after a regular job has already started, the regular job is
                recorded and the batch is treated as launched (True) so files are not released.
        """
        if batch.is_empty():
            return False

        try:
            # Run regular diagram detect
            job_id: int | None = None
            job_token: str | None = None
            if self.in_memory_cache:
                assets, files = split_entities_by_kind(self.in_memory_cache)
                self.logger.info(
                    f"Running diagram detect on {batch.size()} files with "
                    f"{len(self.in_memory_cache)} entities "
                    f"({len(assets)} assets, {len(files)} files)"
                )
                if self.logger.log_level == "DEBUG":
                    self.logger.debug("Regular detect entities JSON: " + json.dumps(self.in_memory_cache, default=str))
                job_id, job_token = self.annotation_service.run_diagram_detect(
                    files=batch.file_references, entities=self.in_memory_cache
                )
            else:
                self.logger.info("Skipping standard diagram detect: no entities available.")

            update_properties = {
                "annotationStatus": AnnotationStatus.PROCESSING,
                **annotation_clock(),
                "launchFunctionId": self.function_id,
                "launchFunctionCallId": self.call_id,
            }
            if job_id is not None:
                update_properties["diagramDetectJobId"] = job_id
                update_properties["diagramDetectJobToken"] = job_token

            # Run diagram detect on pattern mode
            pattern_job_id: int | None = None
            pattern_job_token: str | None = None
            if self.config.launch_function.pattern_mode:
                total_patterns = count_pattern_sample_strings(self.in_memory_patterns)
                self.logger.info(
                    f"Running pattern mode diagram detect on {batch.size()} files with "
                    f"{total_patterns} sample patterns "
                    f"(structuralAutoPatterns={self.config.launch_function.structural_auto_patterns})"
                )
                if total_patterns:
                    for group in self.in_memory_patterns:
                        samples = group.get("sample") or []
                        self.logger.debug(
                            f"Pattern group {group.get('resource_type')}/{group.get('annotation_type')}: "
                            f"{len(samples)} samples → {samples[:20]}" + (" ..." if len(samples) > 20 else "")
                        )
                    try:
                        pattern_job_id, pattern_job_token = self.annotation_service.run_pattern_mode_detect(
                            files=batch.file_references, pattern_samples=self.in_memory_patterns
                        )
                    except CogniteAPIError as e:
                        if job_id is not None:
                            self.logger.warning(
                                f"Pattern-mode detect failed after regular job {job_id} started. "
                                f"Recording the regular job; these files get no pattern-mode results: {e}"
                            )
                            self._record_launched_jobs(batch, update_properties, job_id, None)
                            return True
                        raise
                    update_properties["patternModeJobId"] = pattern_job_id
                    update_properties["patternModeJobToken"] = pattern_job_token
                else:
                    self.logger.info("Skipping pattern-mode diagram detect: no sample patterns available.")

            if "diagramDetectJobId" not in update_properties and "patternModeJobId" not in update_properties:
                self.logger.warning(
                    f"No jobs launched for {batch.size()} files: no entities and no patterns in their scope. "
                    "Marking them Failed."
                )
                self._mark_batch_failed(batch, "No entities or pattern samples found for the file's scope")
                return False

            self._record_launched_jobs(batch, update_properties, job_id, pattern_job_id)
            return True
        finally:
            batch.clear_pair()

    def _record_launched_jobs(
        self,
        batch: BatchOfPairedNodes,
        update_properties: dict[str, object],
        job_id: int | None,
        pattern_job_id: int | None,
    ) -> None:
        """Store the launched job ids on the batch's annotation states."""
        batch.batch_states.update_node_properties(
            new_properties=update_properties,
            view_id=self.annotation_state_view.as_view_id(),
        )
        self.data_model_service.update_annotation_state(batch.batch_states.apply)
        self.logger.info(
            message=launch_state_update_message(job_id, pattern_job_id),
            section="END",
        )

    def _mark_batch_failed(self, batch: BatchOfPairedNodes, message: str) -> None:
        """Set the batch's states to Failed and swap the files' AnnotationInProcess tag for AnnotationFailed."""
        batch.batch_states.update_node_properties(
            new_properties={
                "annotationStatus": AnnotationStatus.FAILED,
                **annotation_clock(),
                "annotationMessage": message,
                "launchFunctionId": self.function_id,
                "launchFunctionCallId": self.call_id,
            },
            view_id=self.annotation_state_view.as_view_id(),
        )
        file_view_id = self.file_view.as_view_id()
        file_applies = [
            tags_apply(
                file_node,
                file_view_id,
                replace_tag(node_tags(file_node, file_view_id), TAG_ANNOTATION_IN_PROCESS, TAG_ANNOTATION_FAILED),
            )
            for file_node in batch.batch_files.nodes
        ]
        self.data_model_service.update_annotation_state(batch.batch_states.apply + file_applies)
