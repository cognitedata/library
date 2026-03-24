from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


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


class FilterOperator(str, Enum):
    """
    Defines the types of filter operations that can be specified in the configuration.
    Inherits from 'str' so that the enum members are also string instances,
    making them directly usable where a string is expected (e.g., serialization).
    """

    EQUALS = "EQUALS"  # Checks for equality against a single value.
    EXISTS = "EXISTS"  # Checks if a property exists (is not null).
    CONTAINSALL = "CONTAINSALL"  # Checks if an item contains all specified values for a given property
    IN = "IN"  # Checks if a value is within a list of specified values. Not implementing CONTAINSANY b/c IN is usually more suitable
    SEARCH = "SEARCH"  # Performs full text search on a specified property


class FieldRole(Enum):
    """Defines the role of the field in the data extraction process."""

    TARGET = "target"
    CONTEXT = "context"
    VALIDATION = "validation"


class SourceFieldParameter(BaseModel):
    """
    A class to define the configuration parameters for a single source field
    during data extraction.
    """

    # 1. Required fields are defined first (using Field(..., description="..."))
    field_name: str = Field(
        ...,
        description="Name or path to the metadata field (e.g., 'description', 'metadata.tagIds').",
    )
    table_id: Optional[str] = Field(
        None,
        description="Identifier for the source of the field if contained in RAW table (e.g., 'cdf', 'external_api').",
    )
    join_fields: Optional[Dict[str, str]] = Field(
        None,
        description="Mapping of join fields between view and table (e.g., {'view_field': 'sourceId', 'table_field': 'sourceId'}).",
    )
    field_type: str = Field(
        "string", description="Data type of the field (e.g., 'string', 'array', 'object')."
    )
    required: bool = Field(
        ...,
        description="Whether the field must exist (skip entity if missing) (e.g., false).",
    )
    priority: int = Field(
        None, description="Order of precedence when multiple fields match (e.g., 1)."
    )
    role: FieldRole = Field(
        None, description="Role in extraction: 'target', 'context', 'validation'."
    )
    max_length: Optional[int] = Field(
        None,
        description="Maximum field length to process (performance) (optional, e.g., 1000).",
    )

    # 2. Optional fields are defined next, using None or default_factory
    separator: Optional[str] = Field(
        None,
        description="Delimiter for list-type fields (optional, e.g., ',', ';', '|').",
    )

    # FIX: Use default_factory for the mutable default (List)
    preprocessing: Union[List[str], str, None] = Field(
        None,
        description="Preprocessing steps before extraction (optional, e.g., ['trim', 'lowercase']).",
    )


class ExtractionMethod(Enum):
    """Enumeration of available extraction methods (passthrough is the default when omitted)."""

    PASSTHROUGH = "passthrough"  # Use entire field value as key (no regex/parsing)
    REGEX = "regex"
    FIXED_WIDTH = "fixed width"
    TOKEN_REASSEMBLY = "token reassembly"
    HEURISTIC = "heuristic"


class ExtractionType(Enum):
    """Enumeration of extraction types."""

    CANDIDATE_KEY = "candidate_key"
    FOREIGN_KEY_REFERENCE = "foreign_key_reference"
    DOCUMENT_REFERENCE = "document_reference"


@dataclass
class SourceField:
    """Configuration for a source field to extract from."""

    field_name: str
    field_type: str = "string"
    required: bool = False
    priority: int = 1
    separator: Optional[str] = None
    role: str = "target"
    max_length: int = 1000
    preprocessing: List[str] = field(default_factory=list)


@dataclass
class ExtractionRule:
    """Configuration for an individual extraction rule."""

    name: str
    description: str = ""
    extraction_type: ExtractionType = ExtractionType.CANDIDATE_KEY
    method: ExtractionMethod = ExtractionMethod.REGEX
    pattern: str = ""
    priority: int = 50
    enabled: bool = True
    scope_filters: Dict[str, Any] = field(default_factory=dict)
    min_confidence: float = 0.3
    case_sensitive: bool = False
    aliasing_rules: List[Dict[str, Any]] = field(default_factory=list)
    source_fields: List[SourceField] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    composite_strategy: Optional[
        str
    ] = None  # "concatenate", "token_reassembly", "context_aware"
    # How to handle multiple source fields for a single rule: first_match, merge_all
    field_selection_strategy: Optional[str] = None


class ExtractedKey:
    """Represents an extracted key with metadata."""

    def __init__(
        self,
        value: str,
        extraction_type: ExtractionType,
        source_field: str,
        confidence: float,
        method: ExtractionMethod,
        rule_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.value = value
        # Normalize to enum if passed as string (e.g. from handlers)
        if isinstance(extraction_type, str) and extraction_type in [e.value for e in ExtractionType]:
            extraction_type = ExtractionType(extraction_type)
        self.extraction_type = extraction_type
        self.source_field = source_field
        self._confidence = round(confidence, 2)  # Truncate to 2 decimal places
        if isinstance(method, str) and method in [e.value for e in ExtractionMethod]:
            method = ExtractionMethod(method)
        self.method = method
        self.rule_id = rule_id
        self.metadata = metadata if metadata is not None else {}

    @property
    def confidence(self) -> float:
        """Get the confidence value."""
        return self._confidence

    @confidence.setter
    def confidence(self, value: float) -> None:
        """Set the confidence value, truncated to 2 decimal places."""
        self._confidence = round(value, 2)

    def __repr__(self) -> str:
        return (
            f"ExtractedKey(value={self.value!r}, extraction_type={self.extraction_type!r}, "
            f"source_field={self.source_field!r}, confidence={self._confidence}, "
            f"method={self.method!r}, rule_id={self.rule_id!r})"
        )


@dataclass
class ExtractionResult:
    """Result of key extraction operation."""

    entity_id: str
    entity_type: str
    candidate_keys: List[ExtractedKey] = field(default_factory=list)
    foreign_key_references: List[ExtractedKey] = field(default_factory=list)
    document_references: List[ExtractedKey] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
