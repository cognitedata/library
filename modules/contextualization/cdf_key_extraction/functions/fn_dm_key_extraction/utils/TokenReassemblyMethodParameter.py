from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, model_validator
import re

class TokenPattern(BaseModel):
    name: str = Field(..., description="Name of the token pattern.")

    pattern: str = Field(..., description="Regex pattern for token extraction.")

    # TODO need to validate necessity
    position: int = Field(..., description="Position of the token in the input string.")

    required: bool = Field(False, description="Whether the token is required.")

    component_type: str = Field("unit", description="Type of the token component.")

class TokenizationParameters(BaseModel):
    """
    Configuration for breaking down source fields into individual tokens.
    """
    token_patterns: List[TokenPattern] = Field(..., description="Token extraction patterns.")

    # Use Field() for default value [] and description
    separator_pattern: str = Field(
        ..., description="Characters/strings that separate tokens (e.g., ['-', '_', ' '])."
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
    priority: int = Field(100, description="Rule precedence (e.g., 10).")
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

    def __init__(self, **data):
        super().__init__(**data)

        self.assembly_rules = sorted(self.assembly_rules, key=lambda x: x.priority)
        self.tokenization.token_patterns = sorted(self.tokenization.token_patterns, key=lambda x: x.position)

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

    @model_validator(mode='after')
    def validate_and_cleanup_tokens(self):
        """
        Validate and cleanup token patterns and assembly rules:
        - Remove duplicate token pattern names
        - Remove token patterns not used in any assembly rule
        - Remove assembly rules that reference non-existent tokens
        """
        # Get all token names from patterns
        token_names = [pattern.name for pattern in self.tokenization.token_patterns]
        
        # Find duplicate token names
        seen_names = set()
        duplicate_names = set()
        for name in token_names:
            if name in seen_names:
                duplicate_names.add(name)
            else:
                seen_names.add(name)
        
        # Remove token patterns with duplicate names
        filtered_patterns = [
            pattern for pattern in self.tokenization.token_patterns 
            if pattern.name not in duplicate_names
        ]
        
        # Get all unique token names from assembly rule formats
        assembly_token_names = set()
        for rule in self.assembly_rules:
            # Extract token names from format string using regex
            # Matches {token_name} patterns
            matches = re.findall(r'\{([^}]+)\}', rule.format)
            assembly_token_names.update(matches)
        
        # Remove token patterns that don't appear in any assembly rule
        filtered_patterns = [
            pattern for pattern in filtered_patterns
            if pattern.name in assembly_token_names
        ]
        
        # Get final set of valid token names
        valid_token_names = {pattern.name for pattern in filtered_patterns}
        
        # Remove assembly rules that reference non-existent token names
        filtered_assembly_rules = []
        for rule in self.assembly_rules:
            rule_token_names = set(re.findall(r'\{([^}]+)\}', rule.format))
            # Keep rule only if all referenced tokens exist
            if rule_token_names.issubset(valid_token_names):
                filtered_assembly_rules.append(rule)
        
        # Update the model with filtered data
        self.tokenization.token_patterns = filtered_patterns
        self.assembly_rules = filtered_assembly_rules
        
        return self