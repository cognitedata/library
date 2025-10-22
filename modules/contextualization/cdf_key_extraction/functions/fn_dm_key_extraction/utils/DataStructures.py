from enum import Enum
from typing import List, Optional, Union
from pydantic import BaseModel, Field
from dataclasses import dataclass

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
    field_name: str = Field(..., description="Name or path to the metadata field (e.g., 'description', 'metadata.tagIds').")
    field_type: str = Field(..., description="Data type of the field (e.g., 'string', 'array', 'object').")
    required: bool = Field(..., description="Whether the field must exist (skip entity if missing) (e.g., false).")
    priority: int = Field(None, description="Order of precedence when multiple fields match (e.g., 1).")
    role: FieldRole = Field(None, description="Role in extraction: 'target', 'context', 'validation'.")

    # 2. Optional fields are defined next, using None or default_factory
    separator: Optional[str] = Field(
        None, description="Delimiter for list-type fields (optional, e.g., ',', ';', '|')."
    )

    max_length: Optional[int] = Field(
        None, description="Maximum field length to process (performance) (optional, e.g., 1000)."
    )

    # FIX: Use default_factory for the mutable default (List)
    preprocessing: Union[List[str], str, None] = Field(
        None,
        description="Preprocessing steps before extraction (optional, e.g., ['trim', 'lowercase'])."
    )