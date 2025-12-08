from dataclasses import dataclass, asdict, field
from typing import Literal, cast
from enum import Enum
from datetime import datetime, timezone, timedelta

from cognite.client.data_classes.data_modeling import (
    Node,
    NodeId,
    NodeApply,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.contextualization import (
    FileReference,
)


@dataclass
class EnvConfig:
    """
    Data structure holding the configs to connect to CDF client locally
    """

    cdf_project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: str


class DiagramAnnotationStatus(str, Enum):
    SUGGESTED = "Suggested"
    APPROVED = "Approved"
    REJECTED = "Rejected"


class AnnotationStatus(str, Enum):
    """
    Defines the types of values that the annotationStatus property can be for the Annotation State Instances.
    Inherits from 'str' so that the enum members are also string instances,
    making them directly usable where a string is expected (e.g., serialization).
    Holds the different values that the annotationStatus property can be for the Annotation State Instances.
    """

    NEW = "New"
    RETRY = "Retry"
    PROCESSING = "Processing"
    FINALIZING = "Finalizing"
    ANNOTATED = "Annotated"
    FAILED = "Failed"


class FilterOperator(str, Enum):
    """
    Defines the types of filter operations that can be specified in the configuration.
    Inherits from 'str' so that the enum members are also string instances,
    making them directly usable where a string is expected (e.g., serialization).
    """

    EQUALS = "Equals"  # Checks for equality against a single value.
    EXISTS = "Exists"  # Checks if a property exists (is not null).
    CONTAINSALL = "ContainsAll"  # Checks if an item contains all specified values for a given property
    IN = "In"  # Checks if a value is within a list of specified values. Not implementing CONTAINSANY b/c IN is usually more suitable
    SEARCH = "Search"  # Performs full text search on a specified property


@dataclass
class AnnotationState:
    """
    Data structure holding the mpcAnnotationState view properties. Time will convert to Timestamp when ingested into CDF.
    """

    annotationStatus: AnnotationStatus
    linkedFile: dict[str, str] = field(default_factory=dict)
    attemptCount: int = 0
    annotationMessage: str | None = None
    diagramDetectJobId: int | None = None
    sourceCreatedTime: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    sourceUpdatedTime: str = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    sourceCreatedUser: str = "fn_dm_context_annotation_prepare"
    sourceUpdatedUser: str = "fn_dm_context_annotation_prepare"

    def _create_external_id(self) -> str:
        """
        Create a deterministic external ID so that we can replace mpcAnnotationState of files that have been updated and aren't new
        """
        prefix = "an_state"
        linked_file_space = self.linkedFile["space"]
        linked_file_id = self.linkedFile["externalId"]
        return f"{prefix}_{linked_file_space}_{linked_file_id}"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_node_apply(self, node_space: str, annotation_state_view: ViewId) -> NodeApply:
        external_id: str = self._create_external_id()

        return NodeApply(
            space=node_space,
            external_id=external_id,
            sources=[
                NodeOrEdgeData(
                    source=annotation_state_view,
                    properties=self.to_dict(),
                )
            ],
        )


@dataclass
class FileProcessingBatch:
    primary_scope_value: str
    secondary_scope_value: str | None
    files: list[Node]


@dataclass
class entity:
    """
    data structure for the 'entities' fed into diagram detect,
    {
        "external_id": file.external_id,
        "name": file.properties[job_config.file_view.as_view_id()]["name"],
        "space": file.space,
        "annotation_type": job_config.file_view.type,
        "resource_type": file.properties[job_config.file_view.as_view_id()][{resource_type}],
        "search_property": file.properties[job_config.file_view.as_view_id()][{search_property}],
    }
    """

    external_id: str
    name: str
    space: str
    annotation_type: Literal["diagrams.FileLink", "diagrams.AssetLink"] | None
    resource_type: str
    search_property: list[str] = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


@dataclass
class BatchOfNodes:
    nodes: list[Node] = field(default_factory=list)
    ids: list[NodeId] = field(default_factory=list)
    apply: list[NodeApply] = field(default_factory=list)

    def add(self, node: Node):
        self.nodes.append(node)
        node_id = node.as_id()
        self.ids.append(node_id)
        return

    def clear(self):
        self.nodes.clear()
        self.ids.clear()
        self.apply.clear()
        return

    def update_node_properties(self, new_properties: dict, view_id: ViewId):
        for node in self.nodes:
            node_apply = NodeApply(
                space=node.space,
                external_id=node.external_id,
                existing_version=None,
                sources=[
                    NodeOrEdgeData(
                        source=view_id,
                        properties=new_properties,
                    )
                ],
            )
            self.apply.append(node_apply)
        return


@dataclass
class BatchOfPairedNodes:
    """
    Where nodeA is an instance of the file view and nodeB is an instance of the annotation state view
    """

    file_to_state_map: dict[NodeId, Node]
    batch_files: BatchOfNodes = field(default_factory=BatchOfNodes)
    batch_states: BatchOfNodes = field(default_factory=BatchOfNodes)
    file_references: list[FileReference] = field(default_factory=list)

    def add_pair(self, file_node: Node, file_reference: FileReference):
        self.file_references.append(file_reference)
        self.batch_files.add(file_node)
        file_node_id: NodeId = file_node.as_id()
        state_node: Node = self.file_to_state_map[file_node_id]
        self.batch_states.add(state_node)

    def create_file_reference(
        self,
        file_node_id: NodeId,
        page_range: int,
        annotation_state_view_id: ViewId,
    ) -> FileReference:
        """
        Create a file reference that has a page range for annotation.
        The current implementation of the detect api 20230101-beta only allows annotation of files up to 50 pages.
        Thus, this is my idea of how we can enables annotating files that are more than 50 pages long.

        The annotatedPageCount and pageCount properties won't be set in the initial creation of the annotation state nodes.
        That's because we don't know how many pages are in the pdf until we run the diagram detect job where the page count gets returned from the results of the job.
        Thus, annotatedPageCount and pageCount get set in the finalize function.
        The finalize function will set the page count properties based on the page count that returned from diagram detect job results.
            - If the pdf has less than 50 pages, say 3 pages, then...
                - annotationStatus property will get set to 'complete'
                - annotatedPageCount and pageCount properties will be set to 3.
            - Elif the pdf has more than 50 pages, say 80, then...
                - annotationStatus property will get set to 'new'
                - annotatedPageCount set to 50
                - pageCount set to 80
                - attemptCount doesn't get incremented

        NOTE: Chose to create the file_reference here b/c I already have access to the file node and state node.
        If I chose to have this logic in the launchService then we'd have to iterate on all of the nodes that have already been added.
        Thus -> O(N) + O(N) to create the BatchOfPairedNodes and then to create the file references
        Instead, this approach makes it just O(N)
        """
        annotation_state_node: Node = self.file_to_state_map[file_node_id]
        annotated_page_count: int | None = cast(
            int,
            annotation_state_node.properties[annotation_state_view_id].get("annotatedPageCount"),
        )
        page_count: int | None = cast(
            int,
            annotation_state_node.properties[annotation_state_view_id].get("pageCount"),
        )
        if not annotated_page_count or not page_count:
            file_reference: FileReference = FileReference(
                file_instance_id=file_node_id,
                first_page=1,
                last_page=page_range,
            )
        else:
            # NOTE: adding 1 here since that annotated_page_count variable holds the last page that was annotated. Thus we want to annotate the following page
            # e.g.) first run annotates pages 1-50 second run would annotate 51-100
            first_page = annotated_page_count + 1
            last_page = annotated_page_count + page_range
            if page_count <= last_page:
                last_page = page_count
            file_reference: FileReference = FileReference(
                file_instance_id=file_node_id,
                first_page=first_page,
                last_page=last_page,
            )

        return file_reference

    def clear_pair(self):
        self.batch_files.clear()
        self.batch_states.clear()
        self.file_references.clear()

    def size(self) -> int:
        return len(self.file_references)

    def is_empty(self) -> bool:
        if self.file_references:
            return False
        return True


@dataclass
class PerformanceTracker:
    """
    Keeps track of metrics
    """

    files_success: int = 0
    files_failed: int = 0
    total_runs: int = 0
    total_time_delta: timedelta = timedelta(0)
    latest_run_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def _run_time(self) -> timedelta:
        time_delta = datetime.now(timezone.utc) - self.latest_run_time
        return time_delta

    def _average_run_time(self) -> timedelta:
        if self.total_runs == 0:
            return timedelta(0)
        return self.total_time_delta / self.total_runs

    def add_files(self, success: int, failed: int = 0):
        self.files_success += success
        self.files_failed += failed

    def generate_local_report(self) -> str:
        self.total_runs += 1
        time_delta = self._run_time()
        self.total_time_delta += time_delta
        self.latest_run_time = datetime.now(timezone.utc)

        report = f"run time: {time_delta}"
        return report

    def generate_overall_report(self) -> str:
        report = f" Run started {datetime.now(timezone.utc)}\n- total runs: {self.total_runs}\n- total files processed: {self.files_success+self.files_failed}\n- successful files: {self.files_success}\n- failed files: {self.files_failed}\n- total run time: {self.total_time_delta}\n- average run time: {self._average_run_time()}"
        return report

    def generate_ep_run(
        self,
        caller: Literal["Prepare", "Launch", "Finalize"],
        function_id: str | None,
        call_id: str | None,
    ) -> str:
        """Generates the report string for the extraction pipeline run."""
        report = (
            f"(caller:{caller}, function_id:{function_id}, call_id:{call_id}) - "
            f"total files processed: {self.files_success + self.files_failed} - "
            f"successful files: {self.files_success} - "
            f"failed files: {self.files_failed}"
        )
        return report

    def reset(self) -> None:
        self.files_success = 0
        self.files_failed = 0
        self.total_runs: int = 0
        self.total_time_delta = timedelta(0)
        self.latest_run_time = datetime.now(timezone.utc)
        print("PerformanceTracker state has been reset")


@dataclass
class PromoteTracker:
    """
    Tracks metrics for the promote function.

    Metrics:
    - edges_promoted: Edges successfully promoted (single match found)
    - edges_rejected: Edges rejected (no match found)
    - edges_ambiguous: Edges with ambiguous matches (multiple entities found)
    - total_runs: Number of batches processed
    - total_time_delta: Cumulative runtime
    """

    edges_promoted: int = 0
    edges_rejected: int = 0
    edges_ambiguous: int = 0
    total_runs: int = 0
    total_time_delta: timedelta = field(default_factory=lambda: timedelta(0))
    latest_run_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def _run_time(self) -> timedelta:
        """Calculates time since last run started."""
        time_delta: timedelta = datetime.now(timezone.utc) - self.latest_run_time
        return time_delta

    def _average_run_time(self) -> timedelta:
        """Calculates average time per batch."""
        if self.total_runs == 0:
            return timedelta(0)
        return self.total_time_delta / self.total_runs

    def add_edges(self, promoted: int = 0, rejected: int = 0, ambiguous: int = 0) -> None:
        """
        Adds edge counts to the tracker.

        Args:
            promoted: Number of edges successfully promoted
            rejected: Number of edges rejected (no match)
            ambiguous: Number of edges with ambiguous matches
        """
        self.edges_promoted += promoted
        self.edges_rejected += rejected
        self.edges_ambiguous += ambiguous

    def generate_local_report(self) -> str:
        """
        Generates a report for the current batch.

        Returns:
            String report with run time
        """
        self.total_runs += 1
        time_delta: timedelta = self._run_time()
        self.total_time_delta += time_delta
        self.latest_run_time = datetime.now(timezone.utc)

        report: str = f"Batch run time: {time_delta}"
        return report

    def generate_overall_report(self) -> str:
        """
        Generates a comprehensive report for all runs.

        Returns:
            String report with all metrics
        """
        total_edges: int = self.edges_promoted + self.edges_rejected + self.edges_ambiguous
        report: str = (
            f"Promote Function Summary\n"
            f"- Total runs: {self.total_runs}\n"
            f"- Total edges processed: {total_edges}\n"
            f"  ├─ Promoted (auto): {self.edges_promoted}\n"
            f"  ├─ Rejected (no match): {self.edges_rejected}\n"
            f"  └─ Ambiguous (multiple matches): {self.edges_ambiguous}\n"
            f"- Total run time: {self.total_time_delta}\n"
            f"- Average run time: {self._average_run_time()}"
        )
        return report

    def generate_ep_run(self, function_id: str | None, call_id: str | None) -> str:
        """
        Generates a report string for extraction pipeline logging.

        Args:
            function_id: Cognite Function ID
            call_id: Cognite Function call ID

        Returns:
            String report for extraction pipeline
        """
        total_edges: int = self.edges_promoted + self.edges_rejected + self.edges_ambiguous
        report: str = (
            f"(caller:Promote, function_id:{function_id}, call_id:{call_id}) - "
            f"total edges processed: {total_edges} - "
            f"promoted: {self.edges_promoted} - "
            f"rejected: {self.edges_rejected} - "
            f"ambiguous: {self.edges_ambiguous}"
        )
        return report

    def reset(self) -> None:
        """Resets all tracker metrics to initial state."""
        self.edges_promoted = 0
        self.edges_rejected = 0
        self.edges_ambiguous = 0
        self.total_runs = 0
        self.total_time_delta = timedelta(0)
        self.latest_run_time = datetime.now(timezone.utc)
        print("PromoteTracker state has been reset")


def remove_protected_properties(node_apply: NodeApply) -> NodeApply:
    """
    In mid November the product team pushed a change that adds write-protection for 'isUploaded' and 'uploadedTime' to staging clusters.
    The rationale is that CogniteFile and CogniteAsset forced the team to implement a system managed field concept.
    This function effectively deletes the protected properties from the json object and adheres to the new standard set in place.
    We don't use typed nodes like CogniteFile in this deployment pack since we need the properties associated with the view that we extended.
    """
    protected_properties = [
        "isUploaded",
        "uploadedTime",
    ]  # NOTE: These are just the protected properties of CogniteFile. There are also protected properties for CogniteAsset, though we don't use it in this deployment pack.
    for source in node_apply.sources:
        # Safely remove the keys if they exist using .pop(key, None)
        for property in protected_properties:
            source.properties.pop(property, None)

    return node_apply
