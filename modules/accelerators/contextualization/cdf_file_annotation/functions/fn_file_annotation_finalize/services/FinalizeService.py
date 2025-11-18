import time
import abc
from typing import cast, Literal
from datetime import datetime, timezone
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeId,
    NodeList,
    NodeApply,
    NodeOrEdgeData,
)

from services.ConfigService import Config, ViewPropertyConfig
from services.LoggerService import CogniteFunctionLogger
from services.RetrieveService import IRetrieveService
from services.ApplyService import IApplyService
from utils.DataStructures import (
    BatchOfNodes,
    PerformanceTracker,
    AnnotationStatus,
)


class AbstractFinalizeService(abc.ABC):
    """
    Orchestrates the file annotation finalize process.
    This service retrieves the results of the diagram detect jobs from the launch function and then applies annotations to the file.
    Additionally, it captures the file and asset annotations into separate RAW tables.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        retrieve_service: IRetrieveService,
        apply_service: IApplyService,
    ):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger
        self.tracker: PerformanceTracker = tracker
        self.retrieve_service: IRetrieveService = retrieve_service
        self.apply_service: IApplyService = apply_service

    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralFinalizeService(AbstractFinalizeService):
    """
    Implementation of the FinalizeService.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        retrieve_service: IRetrieveService,
        apply_service: IApplyService,
        function_call_info: dict,
    ):
        super().__init__(
            client,
            config,
            logger,
            tracker,
            retrieve_service,
            apply_service,
        )

        self.annotation_state_view: ViewPropertyConfig = config.data_model_views.annotation_state_view
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.page_range: int = config.launch_function.annotation_service.page_range
        self.max_retries: int = config.finalize_function.max_retry_attempts
        self.clean_old_annotations: bool = config.finalize_function.clean_old_annotations
        self.function_id: int | None = function_call_info.get("function_id")
        self.call_id: int | None = function_call_info.get("call_id")

    def run(self) -> Literal["Done"] | None:
        """
        Main execution loop for finalizing diagram detection jobs.

        Retrieves completed jobs, fetches their results, processes annotations for each file,
        and updates annotation state instances. Handles multi-page files by tracking progress
        and requeueing files with remaining pages.

        Args:
            None

        Returns:
            "Done" if no jobs available, None if processing should continue.

        Raises:
            CogniteAPIError: Various API errors are handled gracefully (version conflicts,
                           timeouts, etc.).
        """
        self.logger.info("Starting Finalize Function", section="START")
        try:
            job_id, pattern_mode_job_id, file_to_state_map = self.retrieve_service.get_job_id()
            if not job_id or not file_to_state_map:
                self.logger.info("No diagram detect jobs found", section="END")
                return "Done"
            self.logger.info(f"Retrieved job id ({job_id}) and claimed {len(file_to_state_map.values())} files")
        except CogniteAPIError as e:
            if e.code == 400 and e.message == "A version conflict caused the ingest to fail.":
                self.logger.info(
                    message=f"Retrieved job id that has already been claimed. Grabbing another job.",
                    section="END",
                )
                return
            elif (
                e.code == 408
                and e.message == "Graph query timed out. Reduce load or contention, or optimise your query."
            ):
                self.logger.error(message="Ran into the following error", error=e, section="END")
                return
            else:
                raise e

        job_results: dict | None = None
        pattern_mode_job_results: dict | None = None
        try:
            job_results = self.retrieve_service.get_diagram_detect_job_result(job_id)
            if pattern_mode_job_id:
                pattern_mode_job_results = self.retrieve_service.get_diagram_detect_job_result(pattern_mode_job_id)
        except Exception as e:
            self.logger.info(
                message=f"Unfinalizing {len(file_to_state_map.keys())} files - job id ({job_id}) is a bad gateway",
                section="END",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=list(file_to_state_map.values())),
                status=AnnotationStatus.RETRY,
                failed=True,
            )

        # A job is considered complete if:
        # 1. The main job is finished, AND
        # 2. EITHER pattern mode was not enabled (no pattern job ID)
        #    OR pattern mode was enabled AND its job is also finished.
        jobs_complete: bool = job_results is not None and (
            not pattern_mode_job_id or pattern_mode_job_results is not None
        )

        if not jobs_complete:
            self.logger.info(
                message=f"Unfinalizing {len(file_to_state_map.keys())} files - job id ({job_id}) and/or pattern id ({pattern_mode_job_id}) not complete",
                section="END",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=list(file_to_state_map.values())),
                status=AnnotationStatus.PROCESSING,
            )
            self.logger.info(message="Sleeping for 30 seconds")
            time.sleep(30)
            return

        self.logger.info(
            f"Both jobs ({job_id}, {pattern_mode_job_id}) complete. Applying all annotations.",
            section="END",
        )

        merged_results = {
            (item["fileInstanceId"]["space"], item["fileInstanceId"]["externalId"]): {"regular": item}
            for item in job_results["items"]
        }
        if pattern_mode_job_results:
            for item in pattern_mode_job_results["items"]:
                key = (
                    item["fileInstanceId"]["space"],
                    item["fileInstanceId"]["externalId"],
                )
                if key in merged_results:
                    merged_results[key]["pattern"] = item
                else:
                    merged_results[key] = {"pattern": item}

        count_retry, count_failed, count_success = 0, 0, 0
        annotation_state_node_applies = []

        for (space, external_id), results in merged_results.items():
            file_id = NodeId(space, external_id)
            file_node = self.client.data_modeling.instances.retrieve_nodes(
                nodes=file_id, sources=self.file_view.as_view_id()
            )
            if not file_node:
                continue

            annotation_state_node = file_to_state_map[file_id]
            current_attempt = cast(
                int,
                annotation_state_node.properties[self.annotation_state_view.as_view_id()]["attemptCount"],
            )
            next_attempt = current_attempt + 1

            try:
                self.logger.info(f"Processing file {file_id}:")
                annotation_msg, pattern_msg = self.apply_service.process_and_apply_annotations_for_file(
                    file_node,
                    results.get("regular"),
                    results.get("pattern"),
                    self.clean_old_annotations
                    and annotation_state_node.properties[self.annotation_state_view.as_view_id()].get(
                        "annotatedPageCount"
                    )
                    is None,
                )
                self.logger.info(f"\t- {annotation_msg}")
                self.logger.info(f"\t- {pattern_msg}")

                # Logic to handle multi-page files
                page_count = results.get("regular", {}).get("pageCount", 1)
                annotated_pages = self._check_all_pages_annotated(annotation_state_node, page_count)

                if annotated_pages == page_count:
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.ANNOTATED,
                        next_attempt,
                        annotated_pages,
                        page_count,
                        annotation_msg,
                        pattern_msg,
                    )
                    count_success += 1
                else:
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.NEW,
                        current_attempt,
                        annotated_pages,
                        page_count,
                        "Processed page batch, more pages remaining",
                        pattern_msg,
                    )
                    count_success += 1  # Still a success for this batch

            except Exception as e:
                self.logger.error(f"Failed to process annotations for file {file_id}", error=e)
                if next_attempt >= self.max_retries:
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.FAILED,
                        next_attempt,
                        annotation_message=str(e),
                        pattern_mode_message=str(e),
                    )
                    count_failed += 1
                else:
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.RETRY,
                        next_attempt,
                        annotation_message=str(e),
                        pattern_mode_message=str(e),
                    )
                    count_retry += 1

            annotation_state_node_applies.append(job_node_to_update)

        # Batch update the state nodes at the end
        if annotation_state_node_applies:
            self.logger.info(
                f"Updating {len(annotation_state_node_applies)} annotation state instances",
                section="START",
            )
            try:
                self.apply_service.update_instances(list_node_apply=annotation_state_node_applies)
                self.logger.info(
                    f"\t- {count_success} set to Annotated/New\n\t- {count_retry} set to Retry\n\t- {count_failed} set to Failed"
                )
            except Exception as e:
                self.logger.error(
                    "Error during batch update of annotation states",
                    error=e,
                    section="END",
                )

        self.tracker.add_files(success=count_success, failed=(count_failed + count_retry))
        return None

    def _process_annotation_state(
        self,
        node: Node,
        status: str,
        attempt_count: int,
        annotated_page_count: int | None = None,
        page_count: int | None = None,
        annotation_message: str | None = None,
        pattern_mode_message: str | None = None,
    ) -> NodeApply:
        """
        Creates a NodeApply to update an annotation state instance with processing results.

        Updates status, attempt count, timestamps, and page tracking for multi-page files.
        The annotatedPageCount and pageCount properties are updated based on progress through
        the file's pages.

        Args:
            node: The annotation state node to update.
            status: New annotation status (ANNOTATED, FAILED, NEW, RETRY).
            attempt_count: Current attempt count for this file.
            annotated_page_count: Number of pages successfully annotated so far.
            page_count: Total number of pages in the file.
            annotation_message: Message describing regular annotation results.
            pattern_mode_message: Message describing pattern mode results.

        Returns:
            NodeApply object ready to be applied to update the annotation state.

        NOTE: Create a node apply from the node passed into the function.
        The annotatedPageCount and pageCount properties won't be set if this is the first time the job has been run for the specific node.
        Thus, we set it here and include logic to handle the scneario where it is set.
        NOTE: Always want to use the latest page count from the diagram detect results
        e.g.) let page_range = 50
            - If the pdf has less than 50 pages, say 3 pages, then...
                - annotationStatus property will get set to 'complete'
                - annotatedPageCount and pageCount properties will be set to 3.
            - Elif the pdf has more than 50 pages, say 80, then...
                - annotationStatus property will get set to 'new'
                - annotatedPageCount set to 50
                - pageCount set to 80
                - attemptCount doesn't get incremented
            - If an error occurs, the annotated_page_count and page_count won't be passed
                - Don't want to touch the pageCount and annotatedPageCount properties in this scenario
        """
        update_properties = {
            "annotationStatus": status,
            "sourceUpdatedTime": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "annotationMessage": annotation_message,
            "patternModeMessage": pattern_mode_message,
            "attemptCount": attempt_count,
            "finalizeFunctionId": self.function_id,
            "finalizeFunctionCallId": self.call_id,
        }
        if annotated_page_count and page_count:
            update_properties["annotatedPageCount"] = annotated_page_count
            update_properties["pageCount"] = page_count

        node_apply = NodeApply(
            space=node.space,
            external_id=node.external_id,
            existing_version=None,
            sources=[
                NodeOrEdgeData(
                    source=self.annotation_state_view.as_view_id(),
                    properties=update_properties,
                )
            ],
        )

        return node_apply

    def _check_all_pages_annotated(self, node: Node, page_count: int) -> int:
        """
        Calculates how many pages have been annotated after this batch completes.

        Handles progressive annotation of multi-page files by tracking which pages have been
        processed based on the configured page_range batch size.

        Args:
            node: The annotation state node being processed.
            page_count: Total number of pages in the file from diagram detect results.

        Returns:
            Number of pages annotated after this batch (includes previous batches).

        NOTE: The annotatedPageCount and pageCount properties won't be set if this is the first time the job has been run for the specific node.
        - if annotated_page_count is not set (first run):
            - if page_range >= to the page count:
                - annotated_page_count = page_count b/c all of the pages were passed into the FileReference during LaunchService
            - else:
                - annotated_page_count = page_range b/c there are more pages to annotate
        - else the annotation_page_count property is set:
            - if (annotated_page_count + page_range) >= page_count:
                -  annotated_page_count = page_count b/c all of the pages were passed into the FileReference during LaunchService
            else:
                - annotated_page_count = self.page_range + annotated_page_count b/c there are more pages to annotate
        """
        annotated_page_count: int | None = cast(
            int,
            node.properties[self.annotation_state_view.as_view_id()].get("annotatedPageCount"),
        )

        if not annotated_page_count:
            if self.page_range >= page_count:
                annotated_page_count = page_count
            else:
                annotated_page_count = self.page_range
            self.logger.info(f"Annotated pages 1-to-{annotated_page_count} out of {page_count} total pages", "BOTH")
        else:
            start_page = annotated_page_count + 1
            if (annotated_page_count + self.page_range) >= page_count:
                annotated_page_count = page_count
            else:
                annotated_page_count += self.page_range
            self.logger.info(
                f"Annotated pages {start_page}-to-{annotated_page_count} out of {page_count} total pages", "BOTH"
            )

        return annotated_page_count

    def _update_batch_state(
        self,
        batch: BatchOfNodes,
        status: AnnotationStatus,
        failed: bool = False,
    ):
        """
        Updates annotation state instances in bulk, typically for error scenarios.

        Used when jobs are incomplete or failed to reset job IDs and update status for
        retry or re-queuing.

        Args:
            batch: BatchOfNodes containing annotation state nodes to update.
            status: New annotation status to set for all nodes.
            failed: Whether this is a failure scenario (clears job IDs if True).

        Returns:
            None
        """
        if len(batch.nodes) == 0:
            return

        self.logger.info(message=f"Updating {len(batch.nodes)} annotation state instances")
        if failed:
            update_properties = {
                "annotationStatus": status,
                "sourceUpdatedTime": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "diagramDetectJobId": None,
                "patternModeJobId": None,
            }
            batch.update_node_properties(
                new_properties=update_properties,
                view_id=self.annotation_state_view.as_view_id(),
            )
        else:
            if status == AnnotationStatus.PROCESSING:
                claimed_time = batch.nodes[0].properties[self.annotation_state_view.as_view_id()]["sourceUpdatedTime"]
                update_properties = {
                    "annotationStatus": status,
                    "sourceUpdatedTime": claimed_time,
                }
            else:
                update_properties = {
                    "annotationStatus": status,
                    "sourceUpdatedTime": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }
            batch.update_node_properties(
                new_properties=update_properties,
                view_id=self.annotation_state_view.as_view_id(),
            )
        try:
            update_results = self.apply_service.update_instances(list_node_apply=batch.apply)
            self.logger.info(f"- set annotation status to {status}")
        except Exception as e:
            self.logger.error(
                f"Ran into the following error. Trying again in 30 seconds",
                error=e,
                section="END",
            )
            time.sleep(30)
            update_results = self.apply_service.update_instances(list_node_apply=batch.apply)
            self.logger.info(f"- set annotation status to {status}")
