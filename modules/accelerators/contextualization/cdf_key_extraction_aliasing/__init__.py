"""
Key Extraction and Aliasing Module

This package contains both key extraction and aliasing engines and related components.
Supports both standalone usage and CDF-compatible workflows.
"""

# Aliasing exports
from .functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
    AliasingResult,
    AliasRule,
    TransformationType,
)

# Key Extraction exports
from .functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractionResult,
    KeyExtractionEngine,
)

# CDF-compatible exports
try:
    # Key extraction CDF exports
    # Aliasing CDF exports
    from .functions.fn_dm_aliasing.cdf_adapter import (
        convert_cdf_config_to_aliasing_config,
    )
    from .functions.fn_dm_aliasing.handler import handle as cdf_aliasing_handle
    from .functions.fn_dm_aliasing.pipeline import tag_aliasing as cdf_tag_aliasing
    from .functions.fn_dm_key_extraction.cdf_adapter import (
        convert_cdf_config_to_engine_config,
        load_config_from_cdf,
        load_config_from_yaml,
    )
    from .functions.fn_dm_key_extraction.handler import (
        handle as cdf_key_extraction_handle,
    )
    from .functions.fn_dm_key_extraction.pipeline import (
        key_extraction as cdf_key_extraction,
    )

    __all__ = [
        # Core engines
        "KeyExtractionEngine",
        "ExtractionResult",
        "AliasingEngine",
        "AliasingResult",
        "AliasRule",
        "TransformationType",
        # CDF-compatible exports - key extraction
        "cdf_key_extraction_handle",
        "cdf_key_extraction",
        "convert_cdf_config_to_engine_config",
        "load_config_from_yaml",
        "load_config_from_cdf",
        # CDF-compatible exports - aliasing
        "cdf_aliasing_handle",
        "cdf_tag_aliasing",
        "convert_cdf_config_to_aliasing_config",
    ]
except ImportError:
    # CDF dependencies not available - only export core engines
    __all__ = [
        "KeyExtractionEngine",
        "ExtractionResult",
        "AliasingEngine",
        "AliasingResult",
        "AliasRule",
        "TransformationType",
    ]
