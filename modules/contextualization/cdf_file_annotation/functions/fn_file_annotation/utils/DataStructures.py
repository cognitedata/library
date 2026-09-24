from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Literal, cast

from cognite.client.data_classes.contextualization import (
    FileReference,
)
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeApply,
    NodeId,
    NodeOrEdgeData,
    ViewId,
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


class DiagramAnnotationStatus(StrEnum):
    SUGGESTED = "Suggested"
    APPROVED = "Approved"
    REJECTED = "Rejected"


class AnnotationStatus(StrEnum):
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


class CacheMarker(StrEnum):
    """Transient promote-cache outcomes."""

    AMBIGUOUS = "AMBIGUOUS"
    NO_MATCH = "NO_MATCH"


class FilterOperator(StrEnum):
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
    sourceCreatedTime: str = field(default_factory=lambda: datetime.now(UTC).replace(microsecond=0).isoformat())
    sourceUpdatedTime: str = field(default_factory=lambda: datetime.now(UTC).replace(microsecond=0).isoformat())
    sourceCreatedUser: str = "fn_file_annotation"
    sourceUpdatedUser: str = "fn_file_annotation"

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
        state_properties = (annotation_state_node.properties or {}).get(annotation_state_view_id) or {}
        annotated_page_count: int | None = cast(
            int,
            state_properties.get("annotatedPageCount"),
        )
        page_count: int | None = cast(
            int,
            state_properties.get("pageCount"),
        )
        if not annotated_page_count or not page_count:
            file_reference = FileReference(
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
            if last_page < first_page:
                first_page = last_page
            file_reference = FileReference(
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
        return not self.file_references


@dataclass
class PerformanceTracker:
    """
    Keeps track of metrics
    """

    files_success: int = 0
    files_failed: int = 0
    total_runs: int = 0
    total_time_delta: timedelta = timedelta(0)
    latest_run_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    entities_found: int | None = None
    patterns_created: int | None = None

    def _run_time(self) -> timedelta:
        time_delta = datetime.now(UTC) - self.latest_run_time
        return time_delta

    def _average_run_time(self) -> timedelta:
        if self.total_runs == 0:
            return timedelta(0)
        return self.total_time_delta / self.total_runs

    def add_files(self, success: int, failed: int = 0):
        self.files_success += success
        self.files_failed += failed

    def set_detect_input(self, *, entities: int, patterns: int | None) -> None:
        """Record Launch detect inputs: entities, and pattern samples when pattern mode is on."""
        self.entities_found = entities
        self.patterns_created = patterns

    def generate_local_report(self) -> str:
        self.total_runs += 1
        time_delta = self._run_time()
        self.total_time_delta += time_delta
        self.latest_run_time = datetime.now(UTC)

        report = f"run time: {time_delta}"
        return report

    def generate_overall_report(self, stage: str | None = None) -> str:
        heading = f"{stage} run started" if stage else "Run started"
        lines = [
            f" {heading} {datetime.now(UTC)}",
            f"- total runs: {self.total_runs}",
            f"- total files processed: {self.files_success + self.files_failed}",
            f"- successful files: {self.files_success}",
            f"- failed files: {self.files_failed}",
        ]
        if self.entities_found is not None:
            lines.append(f"- entities found: {self.entities_found}")
        if self.patterns_created is not None:
            lines.append(f"- patterns created: {self.patterns_created}")
        lines.append(f"- total run time: {self.total_time_delta}")
        lines.append(f"- average run time: {self._average_run_time()}")
        return "\n".join(lines)

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
    latest_run_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    def _run_time(self) -> timedelta:
        """Calculates time since last run started."""
        time_delta: timedelta = datetime.now(UTC) - self.latest_run_time
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
        self.latest_run_time = datetime.now(UTC)

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


def node_tags(node: Node, view_id: ViewId) -> list[str]:
    """Return a copy of the node's tags in the given view, or [] when it has none."""
    return list(cast(list[str], ((node.properties or {}).get(view_id) or {}).get("tags") or []))


def tags_apply(node: Node, view_id: ViewId, tags: list[str]) -> NodeApply:
    """
    Build a NodeApply that changes only the node's tags.

    Sending the node's other properties back would make the request as large as the node and could
    overwrite changes made to them since the node was read. Use with replace=False.

    Args:
        node: The node to update.
        view_id: View that exposes the tags property.
        tags: The new tags; duplicates are removed.

    Returns:
        NodeApply carrying only the tags.
    """
    return NodeApply(
        space=node.space,
        external_id=node.external_id,
        sources=[NodeOrEdgeData(source=view_id, properties={"tags": unique_tags(tags)})],
    )


def unique_tags(tags: list[str]) -> list[str]:
    """Return tags with duplicates removed, preserving first-seen order."""
    seen: set[str] = set()
    result: list[str] = []
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            result.append(tag)
    return result


def add_unique_tags(tags: list[str], *new_tags: str) -> list[str]:
    """Append tags that are not already present, preserving order."""
    result = unique_tags(tags)
    existing = set(result)
    for tag in new_tags:
        if tag not in existing:
            result.append(tag)
            existing.add(tag)
    return result


def replace_tag(tags: list[str], old: str, new: str) -> list[str]:
    """Remove `old` and ensure `new` is present once."""
    result = [tag for tag in unique_tags(tags) if tag != old]
    if new not in result:
        result.append(new)
    return result
