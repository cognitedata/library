"""Base handler class for alias transformers."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set

from cognite.client import CogniteClient

from ...common.logger import CogniteFunctionLogger


class AliasTransformerHandler(ABC):
    """Abstract base class for alias transformer handlers."""

    def __init__(
        self,
        logger: Optional[CogniteFunctionLogger] = None,
        client: Optional[CogniteClient] = None,
    ):
        self.logger = logger or CogniteFunctionLogger("INFO", False)
        self.client = client

    @abstractmethod
    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """Apply transformation to generate new aliases."""
        pass
