from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ...common.logger import CogniteFunctionLogger
from ...utils.DataStructures import ExtractedKey


class ExtractionMethodHandler(ABC):
    """Abstract base class for extraction handlers (entity-level API)."""

    def __init__(
        self,
        logger: Optional[CogniteFunctionLogger] = None,
    ):
        self.logger = logger or CogniteFunctionLogger("INFO", False)

    @abstractmethod
    def extract_from_entity(
        self,
        entity: Dict[str, Any],
        rule: Any,
        context: Dict[str, Any],
        *,
        get_field_value: Any,
    ) -> List[ExtractedKey]:
        """
        Extract keys from entity using rule configuration.

        get_field_value: callable (entity, field_spec_like, rule_name) -> Optional[str]
        implemented by KeyExtractionEngine._get_field_value.
        """
        pass
