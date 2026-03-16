from abc import ABC, abstractmethod
from typing import Optional

from ...common.logger import CogniteFunctionLogger
from ...utils.DataStructures import *


class ExtractionMethodHandler(ABC):
    """Abstract base class for extraction method handlers."""

    def __init__(self, logger: Optional[CogniteFunctionLogger] = None):
        self.logger = logger or CogniteFunctionLogger("INFO", False)

    @abstractmethod
    def extract(
        self, text: str, rule: ExtractionRule, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys from text using the specific method."""
        pass
