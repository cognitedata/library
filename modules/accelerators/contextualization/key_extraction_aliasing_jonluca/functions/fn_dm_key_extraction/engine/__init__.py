"""
Key Extraction Engine Module

This module provides the core key extraction functionality for CDF.
"""

import sys
from pathlib import Path

# Handle both relative imports (when used as module) and absolute imports (when run as script)
if __name__ == "__main__":
    # Add parent directory to path when running as script
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from common.config_utils import load_config_from_yaml
    from key_extraction_engine import (
        ExtractedKey,
        ExtractionMethod,
        ExtractionMethodHandler,
        ExtractionResult,
        ExtractionRule,
        ExtractionType,
        FixedWidthExtractionHandler,
        HeuristicExtractionHandler,
        KeyExtractionEngine,
        RegexExtractionHandler,
        SourceField,
        TokenReassemblyExtractionHandler,
    )
else:
    # from .config_utils import load_config_from_yaml
    from .key_extraction_engine import (
        ExtractedKey,
        ExtractionMethod,
        ExtractionMethodHandler,
        ExtractionResult,
        ExtractionRule,
        ExtractionType,
        FixedWidthExtractionHandler,
        HeuristicExtractionHandler,
        KeyExtractionEngine,
        RegexExtractionHandler,
        SourceField,
        TokenReassemblyExtractionHandler,
    )

__all__ = [
    "ExtractionMethod",
    "ExtractionType",
    "SourceField",
    "ExtractionRule",
    "ExtractedKey",
    "ExtractionResult",
    "ExtractionMethodHandler",
    "RegexExtractionHandler",
    "FixedWidthExtractionHandler",
    "TokenReassemblyExtractionHandler",
    "HeuristicExtractionHandler",
    "KeyExtractionEngine",
    "load_config_from_yaml",
]
