from typing import List, Optional

class TokenizationParameters:
    """
    Configuration for breaking down source fields into individual tokens.
    """
    def __init__(
        self,
        token_patterns: List[str],
        separator_patterns: Optional[List[str]] = None,
        max_tokens: Optional[int] = None,
        extract_from_multiple_fields: Optional[List[str]] = None
    ):
        """
        Initializes the TokenizationParameters.

        :param token_patterns: Token extraction patterns.
        :param separator_patterns: Characters/strings that separate tokens (e.g., ['-', '_', ' ']).
        :param max_tokens: Maximum tokens to extract (e.g., 6).
        :param extract_from_multiple_fields: Cross-field token extraction (e.g., ["name", "description"]).
        """
        self.token_patterns = token_patterns
        self.separator_patterns = separator_patterns if separator_patterns is not None else []
        self.max_tokens = max_tokens
        self.extract_from_multiple_fields = extract_from_multiple_fields if extract_from_multiple_fields is not None else []


class AssemblyRule:
    """
    Defines a single rule for reassembling extracted tokens.
    """
    def __init__(
        self,
        format: str,
        priority: int,
        conditions: Optional[Dict[str, Any]] = None
    ):
        """
        Initializes an AssemblyRule.

        :param format: Template with placeholders (e.g., "{site}-{unit}-{tag}").
        :param priority: Rule precedence (e.g., 10).
        :param conditions: Conditions for applying this rule (e.g., {"token_count": 3}).
        """
        self.format = format
        self.priority = priority
        self.conditions = conditions if conditions is not None else {}

class ValidationParameters:
    """
    Configuration for validating the extracted tokens and the final assembled result.
    """
    def __init__(
        self,
        min_tokens: Optional[int] = None,
        max_tokens: Optional[int] = None,
        validate_assembled: bool = False,
        validation_pattern: Optional[str] = None
    ):
        """
        Initializes the ValidationParameters.

        :param min_tokens: Minimum tokens required (e.g., 3).
        :param max_tokens: Maximum tokens allowed (e.g., 5).
        :param validate_assembled: Validate final assembled result (e.g., True).
        :param validation_pattern: Regex for assembled result validation (e.g., '^[A-Z]+-\d+-[A-Z]+\d+$').
        """
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.validate_assembled = validate_assembled
        self.validation_pattern = validation_pattern

class TokenReassemblyMethodParameter:
    """
    A class defining parameters for token extraction, reassembly, and validation.
    """
    def __init__(
        self,
        tokenization: TokenizationParameters,
        assembly_rules: List[AssemblyRule],
        validation: ValidationParameters
    ):
        """
        Initializes the TokenReassemblyMethodParameter configuration.

        :param tokenization: Configuration for token extraction and separation.
        :param assembly_rules: Rules for reassembling tokens.
        :param validation: Configuration for validating tokens and the final result.
        """
        self.tokenization = tokenization
        self.assembly_rules = assembly_rules
        self.validation = validation