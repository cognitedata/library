"""Base handler class for alias transformers."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set

from ...common.logger import CogniteFunctionLogger


class AliasTransformerHandler(ABC):
    """Abstract base class for alias transformer handlers.

    CDF access for RAW-backed rules is owned by :class:`AliasingEngine` (``engine.client``),
    not by individual transformer handlers.
    """

    def __init__(
        self,
        logger: Optional[CogniteFunctionLogger] = None,
    ):
        """Create a transformer handler with optional logger."""
        self.logger = logger or CogniteFunctionLogger("INFO", False)

    @abstractmethod
    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """Apply transformation to generate new aliases."""
        pass
