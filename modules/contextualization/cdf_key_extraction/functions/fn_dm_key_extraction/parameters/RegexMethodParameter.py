from typing import Optional, List

class RegexOptions:
    """
    Configuration options for the underlying regular expression engine.
    """
    def __init__(
        self,
        multiline: bool = False,
        dotall: bool = False,
        ignore_case: bool = False,
        unicode: bool = True
    ):
        """
        Initializes the RegexOptions.

        :param multiline: Enable multiline mode (e.g., False).
        :param dotall: Make '.' match newlines (e.g., False).
        :param ignore_case: Case-insensitive matching (e.g., False).
        :param unicode: Enable Unicode support (e.g., True).
        """
        self.multiline = multiline
        self.dotall = dotall
        self.ignore_case = ignore_case
        self.unicode = unicode

class RegexMethodParameter:
    """
    A class to define the parameters for a regex-based data extraction method.
    """
    def __init__(
        self,
        pattern: str,
        regex_options: RegexOptions,
        validation_pattern: Optional[str] = None,
        capture_groups: Optional[List[Dict[str, Any]]] = None,
        reassemble_format: Optional[str] = None,
        max_matches_per_field: Optional[int] = None,
        early_termination: bool = False
    ):
        """
        Initializes the RegexMethodParameter configuration.

        :param pattern: Regular expression pattern (e.g., '\bP[-_]?\d{2,4}[A-Z]?\b').
        :param regex_options: Configuration options for the regex engine.
        :param validation_pattern: Additional pattern for post-extraction validation (optional, e.g., '^P\d{2,4}[A-Z]?$').
        :param capture_groups: Named capture group definitions (optional).
        :param reassemble_format: Template for reassembling captured components (optional, e.g., "{prefix}-{number}{suffix}").
        :param max_matches_per_field: Limit number of matches (optional, e.g., 50).
        :param early_termination: Stop after first match (e.g., False).
        """
        self.pattern = pattern
        self.validation_pattern = validation_pattern
        self.regex_options = regex_options
        self.capture_groups = capture_groups if capture_groups is not None else []
        self.reassemble_format = reassemble_format
        self.max_matches_per_field = max_matches_per_field
        self.early_termination = early_termination