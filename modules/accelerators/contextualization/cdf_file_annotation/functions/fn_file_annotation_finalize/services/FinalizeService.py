import abc
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, cast

from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeApply,
    NodeId,
    NodeList,
    NodeOrEdgeData,
)
from cognite.client.exceptions import CogniteAPIError
from services.ApplyService import IApplyService
from services.ConfigService import Config, ViewPropertyConfig
from services.LoggerService import CogniteFunctionLogger
from services.RetrieveService import IRetrieveService
from utils.DataStructures import (
    AnnotationStatus,
    BatchOfNodes,
    PerformanceTracker,
    remove_protected_properties,
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
    FINALIZED_FILES_RAW_TABLE = "annotation_finalize_processed_files"

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
        self.raw_db: str = config.raw_tables.raw_db
        self.finalized_files_raw_table: str = self.FINALIZED_FILES_RAW_TABLE

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
            regular_job, pattern_mode_job, file_to_state_map = self.retrieve_service.get_job_id()
            if not regular_job or not file_to_state_map:
                self.logger.info("No diagram detect jobs found", section="END")
                return "Done"
            self.logger.info(f"Retrieved job {regular_job} and claimed {len(file_to_state_map.values())} files")
        except CogniteAPIError as e:
            if e.code == 400 and e.message == "A version conflict caused the ingest to fail.":
                self.logger.info(
                    message=f"Retrieved job that has already been claimed. Grabbing another job.",
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
            self.logger.info("(Regular) Retrieving diagram detect job results", "START")
            job_results = self.retrieve_service.get_diagram_detect_job_result(
                job_id=regular_job[0], job_token=regular_job[1]
            )
            if pattern_mode_job:
                self.logger.info("(Pattern) Retrieving diagram detect job results")
                pattern_mode_job_results = self.retrieve_service.get_diagram_detect_job_result(
                    job_id=pattern_mode_job[0], job_token=pattern_mode_job[1]
                )
        except Exception as e:
            self.logger.error(
                message=f"Unfinalizing {len(file_to_state_map.keys())} files. Encountered an error.",
                error=e,
                section="BOTH",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=list(file_to_state_map.values())),
                status=AnnotationStatus.RETRY,
                failed=True,
            )
            return

        if job_results is None:
            self.logger.info(
                message=f"Unfinalizing {len(file_to_state_map.keys())} files - unable to retrieve regular job payload for {regular_job}",
                section="BOTH",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=list(file_to_state_map.values())),
                status=AnnotationStatus.PROCESSING,
            )
            self.logger.info(message="Sleeping for 30 seconds")
            time.sleep(30)
            return

        regular_items_map = self._index_items_by_file(job_results)
        pattern_items_map = self._index_items_by_file(pattern_mode_job_results) if pattern_mode_job_results else {}

        merged_results: dict[tuple[str, str], dict[str, dict | None]] = {}
        pending_nodes: list[Node] = []
        pending_external_ids: list[str] = []
        ready_external_ids: list[str] = []

        for file_id, state_node in file_to_state_map.items():
            key = file_id.as_tuple()
            regular_item = regular_items_map.get(key)
            pattern_item = pattern_items_map.get(key) if pattern_mode_job else None
            regular_done = self._is_item_terminal(regular_item)
            pattern_done = True if not pattern_mode_job else self._is_item_terminal(pattern_item)

            if regular_done and pattern_done:
                merged_results[key] = {"regular": regular_item, "pattern": pattern_item}
                ready_external_ids.append(file_id.external_id)
            else:
                pending_nodes.append(state_node)
                pending_external_ids.append(file_id.external_id)

        if not merged_results:
            self.logger.info(
                message=(
                    f"No terminal file items yet for claimed batch. "
                    f"regular_job={regular_job}, pattern_job={pattern_mode_job}, pending_files={len(pending_external_ids)}"
                ),
                section="BOTH",
            )
            if pending_external_ids:
                self.logger.info(
                    message=f"Pending file externalIds: {', '.join(pending_external_ids)}",
                    section="END",
                )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=pending_nodes),
                status=AnnotationStatus.PROCESSING,
            )
            self.logger.info(message="Sleeping for 30 seconds")
            time.sleep(30)
            return

        self.logger.info(
            message=(
                f"Finalizing {len(ready_external_ids)} files with terminal detect results. "
                f"externalIds: {', '.join(ready_external_ids)}"
            ),
            section="END",
        )

        count_retry, count_failed, count_success = 0, 0, 0
        annotation_state_node_applies: list[NodeApply] = []
        file_node_applies: list[NodeApply] = []

        finalized_external_ids: list[str] = []
        finalized_records: list[dict[str, Any]] = []
        for (space, external_id), results in merged_results.items():
            file_id = NodeId(space, external_id)
            file_node = self.client.data_modeling.instances.retrieve_nodes(
                nodes=file_id, sources=self.file_view.as_view_id()
            )
            if not file_node:
                continue

            annotation_state_node = file_to_state_map.get(file_id)
            if not annotation_state_node:
                self.logger.warning(f"Missing annotation state for finalized file {file_id.external_id}. Skipping.")
                continue
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
                    file_node_apply: NodeApply = remove_protected_properties(file_node.as_apply())
                    file_node_apply.existing_version = None
                    tags = cast(list[str], file_node_apply.sources[0].properties["tags"])
                    if "AnnotationInProcess" in tags:
                        tags[tags.index("AnnotationInProcess")] = "Annotated"
                    elif "Annotated" not in tags:
                        self.logger.warning(
                            f"File {file_id.external_id} was processed, but 'AnnotationInProcess' tag was not found."
                        )
                    file_node_applies.append(file_node_apply)
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.ANNOTATED,
                        next_attempt,
                        annotated_pages,
                        page_count,
                        annotation_msg,
                        pattern_msg,
                    )
                    final_status: str | AnnotationStatus = AnnotationStatus.ANNOTATED
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
                    final_status = AnnotationStatus.NEW
                    count_success += 1  # Still a success for this batch

            except Exception as e:
                self.logger.error(f"Failed to process annotations for file {file_id}", error=e)
                if next_attempt >= self.max_retries:
                    file_node_apply: NodeApply = remove_protected_properties(file_node.as_apply())
                    file_node_apply.existing_version = None
                    tags = cast(list[str], file_node_apply.sources[0].properties["tags"])
                    if "AnnotationInProcess" in tags:
                        tags[tags.index("AnnotationInProcess")] = "AnnotationFailed"
                    elif "AnnotationFailed" not in tags:
                        self.logger.warning(
                            f"File {file_id.external_id} failed processing, but 'AnnotationInProcess' tag was not found."
                        )
                    file_node_applies.append(file_node_apply)
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.FAILED,
                        next_attempt,
                        annotation_message=str(e),
                        pattern_mode_message=str(e),
                    )
                    final_status = AnnotationStatus.FAILED
                    count_failed += 1
                else:
                    job_node_to_update = self._process_annotation_state(
                        annotation_state_node,
                        AnnotationStatus.RETRY,
                        next_attempt,
                        annotation_message=str(e),
                        pattern_mode_message=str(e),
                    )
                    final_status = AnnotationStatus.RETRY
                    count_retry += 1

            annotation_state_node_applies.append(job_node_to_update)
            finalized_external_ids.append(file_id.external_id)
            finalized_records.append(
                {
                    "fileSpace": file_id.space,
                    "fileExternalId": file_id.external_id,
                    "annotationStatus": self._enum_to_value(final_status),
                    "regularJobId": regular_job[0] if regular_job else None,
                    "patternModeJobId": pattern_mode_job[0] if pattern_mode_job else None,
                    "regularItemStatus": (results.get("regular") or {}).get("status"),
                    "patternItemStatus": (results.get("pattern") or {}).get("status"),
                    "regularItemError": (results.get("regular") or {}).get("errorMessage"),
                    "patternItemError": (results.get("pattern") or {}).get("errorMessage"),
                    "functionId": self.function_id,
                    "functionCallId": self.call_id,
                    "finalizedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }
            )

        # Batch update the state nodes at the end
        if annotation_state_node_applies or file_node_applies:
            self.logger.info(
                f"Updating {len(annotation_state_node_applies)} annotation state instances",
                section="START",
            )
            try:
                self.apply_service.update_instances(list_node_apply=(annotation_state_node_applies + file_node_applies))
                self.logger.info(
                    f"\t- {count_success} set to Annotated/New\n\t- {count_retry} set to Retry\n\t- {count_failed} set to Failed"
                )
            except Exception as e:
                self.logger.error(
                    "Error during batch update of annotation states",
                    error=e,
                    section="END",
                )

        if finalized_external_ids:
            self.logger.info(
                message=(
                    f"Finalized files in this cycle ({len(finalized_external_ids)}): "
                    f"{', '.join(finalized_external_ids)}"
                ),
                section="BOTH",
            )
            self._write_finalized_files_rows(finalized_records)

        if pending_nodes:
            self.logger.info(
                message=(
                    f"Keeping {len(pending_nodes)} files in Processing for next finalize poll. "
                    f"externalIds: {', '.join(pending_external_ids)}"
                ),
                section="END",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=pending_nodes),
                status=AnnotationStatus.PROCESSING,
            )
            # Keep the polling cadence moderate when there are still pending items.
            self.logger.info(message="Sleeping for 30 seconds")
            time.sleep(30)

        self.tracker.add_files(success=count_success, failed=(count_failed + count_retry))
        return None

    def _write_finalized_files_rows(self, finalized_records: list[dict[str, Any]]) -> None:
        if not finalized_records:
            return

        base_ts = int(time.time() * 1000)
        rows: list[RowWrite] = []
        for idx, record in enumerate(finalized_records):
            key = (
                f"{base_ts}_{idx}_{record['fileSpace']}_{record['fileExternalId']}"
                f"_{record.get('functionCallId') or 'na'}"
            )
            rows.append(RowWrite(key=key, columns=record))

        try:
            self.client.raw.rows.insert(
                db_name=self.raw_db,
                table_name=self.finalized_files_raw_table,
                row=rows,
                ensure_parent=True,
            )
            self.logger.info(
                message=(
                    f"Wrote {len(rows)} finalized file records to RAW "
                    f"{self.raw_db}.{self.finalized_files_raw_table}"
                ),
                section="BOTH",
            )
        except Exception as e:
            self.logger.error(
                message=(
                    f"Failed writing finalized file records to RAW "
                    f"{self.raw_db}.{self.finalized_files_raw_table}"
                ),
                error=e,
                section="END",
            )

    @staticmethod
    def _enum_to_value(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        return value

    def _index_items_by_file(self, job_results: dict | None) -> dict[tuple[str, str], dict]:
        if not job_results:
            return {}
        indexed: dict[tuple[str, str], dict] = {}
        for item in cast(list[dict], job_results.get("items", [])):
            file_instance = cast(dict, item.get("fileInstanceId", {}))
            space = file_instance.get("space")
            external_id = file_instance.get("externalId")
            if space and external_id:
                indexed[(space, external_id)] = item
        return indexed

    def _is_item_terminal(self, item: dict | None) -> bool:
        if item is None:
            return False
        status = str(item.get("status", "")).strip().lower()
        if status in {"completed", "failed", "cancelled", "canceled"}:
            return True
        if item.get("errorMessage"):
            return True
        # Defensive fallback for payloads where status may be omitted on completed items.
        if status == "" and ("annotations" in item or "pageCount" in item):
            return True
        return False

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
            self.logger.info(f"Annotated pages 1-to-{annotated_page_count} out of {page_count} total pages", "END")
        else:
            start_page = annotated_page_count + 1
            if (annotated_page_count + self.page_range) >= page_count:
                annotated_page_count = page_count
            else:
                annotated_page_count += self.page_range
            self.logger.info(
                f"Annotated pages {start_page}-to-{annotated_page_count} out of {page_count} total pages", "END"
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
