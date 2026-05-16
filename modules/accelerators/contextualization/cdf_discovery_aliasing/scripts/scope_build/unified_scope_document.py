"""Re-export :func:`normalize_root_graph_into_canvas` for ``scope_build`` imports.

Implementation lives in ``cdf_fn_common.scope_canvas_merge`` (shared with UI server and local runner).
"""

from __future__ import annotations

from cdf_fn_common.scope_canvas_merge import normalize_root_graph_into_canvas

__all__ = ["normalize_root_graph_into_canvas"]
