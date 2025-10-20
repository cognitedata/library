from typing import Optional, List, Literal, Dict, Any, Union
from pydantic import BaseModel, Field, model_validator, ValidationError

class RegexOptions(BaseModel):
    """
    Configuration options for the underlying regular expression engine.
    """
    # Define parameters as type-annotated class attributes with defaults
    multiline: bool = Field(False, description="Enable multiline mode (e.g., False).")
    dotall: bool = Field(False, description="Make '.' match newlines (e.g., False).")
    ignore_case: bool = Field(False, description="Case-insensitive matching (e.g., False).")
    unicode: bool = Field(True, description="Enable Unicode support (e.g., True).")

class RegexMethodParameter(BaseModel):
    method: Literal['regex'] = 'regex'


    """
    A class to define the parameters for a regex-based data extraction method.
    """
    # All fields are defined here, with explicit defaults or ellipses for required fields
    @model_validator(mode='after')
    def check_capture_groups_have_reassembly(self):
        """
        Ensures that if capture groups are provided, a reassembly method is provided as well
        """
        if not self.capture_groups is None and self.reassemble_format is None:
            raise ValidationError("Capture groups must have reassemble format configured")

        return self

    pattern: str = Field(
        ..., description="Regular expression pattern (e.g., '\\bP[-_]?\\d{2,4}[A-Z]?\\b')."
    )

    regex_options: RegexOptions = Field(
        RegexOptions(), description="Configuration options for the regex engine."
    )

    validation_pattern: Optional[str] = Field(
        None, description="Additional pattern for post-extraction validation (optional, e.g., '^P\\d{2,4}[A-Z]?$')."
    )

    capture_groups: Optional[List[Dict[str, Any]]] = Field(
        None, description="Named capture group definitions (optional)."
    )

    reassemble_format: Optional[str] = Field(
        None, description="Template for reassembling captured components (optional, e.g., '{prefix}-{number}{suffix}')."
    )

    max_matches_per_field: Optional[int] = Field(
        None, description="Limit number of matches (optional, e.g., 50)."
    )

    early_termination: bool = Field(
        False, description="Stop after first match (e.g., False)."
    )