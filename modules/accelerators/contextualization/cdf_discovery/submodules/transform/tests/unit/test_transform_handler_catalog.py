from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_build_index.registry import (  # noqa: E402
    HANDLER_BY_ID as BUILD_INDEX_HANDLERS,
    build_index_handler_catalog,
)
from cdf_fn_common.etl_transform.registry import (  # noqa: E402
    HANDLER_BY_ID as TRANSFORM_HANDLERS,
    transform_handler_catalog,
)


def test_transform_handlers_have_verbose_descriptions() -> None:
    catalog = {e["handler_id"]: e["description"] for e in transform_handler_catalog()}
    assert set(catalog) == set(TRANSFORM_HANDLERS)
    for handler_id, cls in TRANSFORM_HANDLERS.items():
        desc = str(getattr(cls, "description", "") or "").strip()
        assert len(desc) >= 40, f"{handler_id} description too short: {desc!r}"
        assert catalog[handler_id] == desc


def test_build_index_handlers_have_verbose_descriptions() -> None:
    catalog = {e["handler_id"]: e["description"] for e in build_index_handler_catalog()}
    assert set(catalog) == set(BUILD_INDEX_HANDLERS)
    for handler_id, cls in BUILD_INDEX_HANDLERS.items():
        desc = str(getattr(cls, "description", "") or "").strip()
        assert len(desc) >= 40, f"{handler_id} description too short: {desc!r}"
        assert catalog[handler_id] == desc
