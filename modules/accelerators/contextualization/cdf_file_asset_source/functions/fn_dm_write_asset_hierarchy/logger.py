"""
Logger module for write asset hierarchy.

Re-exports CogniteFunctionLogger from fn_dm_extract_assets_by_pattern.
"""

try:
    from fn_dm_extract_assets_by_pattern.logger import CogniteFunctionLogger
except ImportError:
    from ..fn_dm_extract_assets_by_pattern.logger import CogniteFunctionLogger

__all__ = ["CogniteFunctionLogger"]
