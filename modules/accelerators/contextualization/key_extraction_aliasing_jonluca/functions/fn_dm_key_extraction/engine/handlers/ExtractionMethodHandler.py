from abc import ABC, abstractmethod
from typing import Optional

from cognite.client import CogniteClient

from ...common.logger import CogniteFunctionLogger
from ...utils.DataStructures import *
from ...config import ExtractionRuleConfig


class ExtractionMethodHandler(ABC):
    """Abstract base class for extraction method handlers."""

    def __init__(
        self,
        logger: CogniteFunctionLogger,
        client: CogniteClient = None,
    ):
        self.logger = logger
        self.client = client

    @abstractmethod
    def extract(
        self, text: str, rule: ExtractionRuleConfig, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys from text using the specific method."""
        pass
