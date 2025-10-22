from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

class TokenizationParameters(BaseModel):
    """
    Configuration for breaking down source fields into individual tokens.
    """
    token_patterns: List[str] = Field(..., description="Token extraction patterns.")

    # Use Field() for default value [] and description
    separator_patterns: List[str] = Field(
        default_factory=['-', '_', ' '], description="Characters/strings that separate tokens (e.g., ['-', '_', ' '])."
    )

    max_tokens: Optional[int] = Field(
        None, description="Maximum tokens to extract (e.g., 6)."
    )

    extract_from_multiple_fields: Optional[List[str]] = Field(
        None, description="Cross-field token extraction (e.g., ['name', 'description']).",
        alias="cross_field_extraction_paths"  # Example of using an alias if needed
    )

class AssemblyRule(BaseModel):
    """
    Defines a single rule for reassembling extracted tokens.
    """
    format: str = Field(..., description="Template with placeholders (e.g., '{site}-{unit}-{tag}').")
    priority: int = Field(..., description="Rule precedence (e.g., 10).")
    name: str = Field("TOKEN_ASSEMBLY", description="A name for the assembly rule.")

    # Use Field() for default value {} and description
    conditions: Dict[str, Any] = Field(
        default_factory={}, description="Conditions for applying this rule (e.g., {'token_count': 3})."
    )

class ValidationParameters(BaseModel):
    """
    Configuration for validating the extracted tokens and the final assembled result.
    """
    min_tokens: Optional[int] = Field(None, description="Minimum tokens required (e.g., 3).")
    max_tokens: Optional[int] = Field(None, description="Maximum tokens allowed (e.g., 5).")
    validate_assembled: bool = Field(False, description="Validate final assembled result (e.g., True).")
    validation_pattern: Optional[str] = Field(
        None, description="Regex for assembled result validation (e.g., '^[A-Z]+-\\d+-[A-Z]+\\d+$')."
    )

class TokenReassemblyMethodParameter(BaseModel):
    method: Literal['token reassembly'] = 'token reassembly'

    """
    A class defining parameters for token extraction, reassembly, and validation.
    """

    # Nested Pydantic models are defined directly as attributes
    tokenization: TokenizationParameters = Field(
        ..., description="Configuration for token extraction and separation."
    )

    assembly_rules: List[AssemblyRule] = Field(
        ..., description="Rules for reassembling tokens."
    )

    validation: ValidationParameters = Field(
        ..., description="Configuration for validating tokens and the final result."
    )