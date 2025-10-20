from typing import Optional, List, Literal, Union
from pydantic import BaseModel, Field

class FieldDefinition(BaseModel):
    name: str = Field(..., description="Field identifier")
    start_position: int = Field(..., ge=0, description="Starting column (0-indexed)")
    end_position: int = Field(..., gt=0, description="Ending column (exclusive)")
    trim: bool = Field(True, description="Remove leading/trailing whitespace")
    required: bool = Field(False, description="Reject record if field is empty")

# Use Literal for the specific string options to enhance type checking
EncodingOption = Literal["utf-8", "latin-1", "ascii"] # Added 'ascii' as a common option

class FixedWidthMethodParameter(BaseModel):
    # Required discriminator field (Literal value must be the string used in the Union)
    method: Literal['fixed width'] = 'fixed width'

    # Class-level field annotations replace the __init__ arguments
    field_definitions: List[FieldDefinition] = Field(
        ..., description="Field extraction specifications."
    )

    line_pattern: Optional[str] = Field(
        None, description="Regex to identify processable lines (e.g., '^\s*[A-Z0-9]')."
    )

    skip_lines: int = Field(
        0, description="Number of initial lines to skip (e.g., 2)."
    )

    stop_on_empty: bool = Field(
        False, description="Stop processing on empty line (e.g., True)."
    )

    record_length: Optional[int] = Field(
        None, description="Lines per record for multi-line parsing (optional, e.g., 3)."
    )

    record_delimiter: Optional[str] = Field(
        None, description="Text delimiter between records (optional, e.g., '---')."
    )

    encoding: EncodingOption = Field(
        "utf-8", description="Text encoding (e.g., 'utf-8', 'latin-1')."
    )