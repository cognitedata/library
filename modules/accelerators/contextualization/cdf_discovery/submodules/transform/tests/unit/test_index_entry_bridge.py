"""Unit tests for index_entry_bridge (ETL module path)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))
os.environ.setdefault("CDF_INVERTED_INDEX_ROOT", str(ROOT.parent / "inverted_index"))

from cdf_fn_common.index_entry_bridge import cohort_index_rows_to_index_entries  # noqa: E402


def test_cohort_index_rows_to_index_entries() -> None:
    entries = cohort_index_rows_to_index_entries(
        [
            {
                "key": "tag-1:sp|metadata",
                "columns": {
                    "INDEX_KIND": "metadata",
                    "LOOKUP_KEY": "tag-1",
                    "POSTINGS_JSON": (
                        '[{"external_id":"ext1","instance_space":"sp",'
                        '"view_external_id":"CogniteFile","source_property":"indexKey","confidence":0.8}]'
                    ),
                },
            }
        ],
        match_scope_key="site:SITE_01",
        match_scope={"site": "SITE_01"},
        build_job_id="run1",
    )
    assert len(entries) == 1
    assert entries[0]["source_type"] == "file_metadata"
    assert entries[0]["match_scope_key"] == "site:SITE_01"
