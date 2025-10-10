from typing import Optional, List, Literal

class FieldDefinition:
    """
    Defines the extraction parameters for a single fixed-width field.
    """
    def __init__(
        self,
        name: str,
        start_position: int,
        end_position: int,
        trim: bool = True,
        required: bool = False
    ):
        """
        Initializes a FieldDefinition.

        :param name: Field identifier (e.g., "tag_id").
        :param start_position: Starting column (0-indexed) (e.g., 0).
        :param end_position: Ending column (exclusive) (e.g., 12).
        :param trim: Remove leading/trailing whitespace (e.g., True).
        :param required: Reject record if field is empty (e.g., False).
        """
        self.name = name
        self.start_position = start_position
        self.end_position = end_position
        self.trim = trim
        self.required = required

# Use Literal for the specific string options to enhance type checking
EncodingOption = Literal["utf-8", "latin-1", "ascii"] # Added 'ascii' as a common option


class FixedWidthMethodParameter:
    """
    A class to define the parameters for a fixed-width data extraction method.
    """
    def __init__(
        self,
        field_definitions: List[FieldDefinition],
        line_pattern: Optional[str] = None,
        skip_lines: int = 0,
        stop_on_empty: bool = False,
        record_length: Optional[int] = None,
        record_delimiter: Optional[str] = None,
        encoding: EncodingOption = "utf-8"
    ):
        """
        Initializes the FixedWidthMethodParameter configuration.

        :param field_definitions: Field extraction specifications.
        :param line_pattern: Regex to identify processable lines (optional, e.g., '^\s*[A-Z0-9]').
        :param skip_lines: Number of initial lines to skip (e.g., 2).
        :param stop_on_empty: Stop processing on empty line (e.g., True).
        :param record_length: Lines per record for multi-line parsing (optional, e.g., 3).
        :param record_delimiter: Text delimiter between records (optional, e.g., "---").
        :param encoding: Text encoding (e.g., "utf-8", "latin-1").
        """
        self.field_definitions = field_definitions
        self.line_pattern = line_pattern
        self.skip_lines = skip_lines
        self.stop_on_empty = stop_on_empty
        self.record_length = record_length
        self.record_delimiter = record_delimiter
        self.encoding = encoding