"""Tests for pipeline run_id retention helpers."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.run_id_retention import (  # noqa: E402
    is_run_id_older_than,
    parse_pipeline_run_id_utc,
    run_id_from_row,
    should_purge_cohort_row,
)


def test_parse_pipeline_run_id_new_and_legacy_suffix() -> None:
    ts = parse_pipeline_run_id_utc("20260516T125807.080874Z-a1b2c3d4e5f6")
    assert ts is not None
    assert ts.year == 2026 and ts.month == 5 and ts.day == 16
    legacy = parse_pipeline_run_id_utc("20260516T125807.080874Z")
    assert legacy == ts


def test_parse_pipeline_run_id_rejects_operator_id() -> None:
    assert parse_pipeline_run_id_utc("operator-run-1") is None


def test_is_run_id_older_than_boundary() -> None:
    rid = "20200101T000000.000000Z-000000000000"
    now = datetime(2020, 1, 4, 1, 0, 0, tzinfo=timezone.utc)
    assert is_run_id_older_than(rid, max_age_hours=72, now=now)
    assert not is_run_id_older_than(rid, max_age_hours=72, now=datetime(2020, 1, 2, 12, 0, 0, tzinfo=timezone.utc))


def test_run_id_from_row_prefers_key_prefix() -> None:
    assert run_id_from_row("run1:scope:n1", {"RUN_ID": "other"}) == "run1"


def test_should_purge_current_run_and_stale() -> None:
    now = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
    cutoff = now - timedelta(hours=72)
    current = "20260517T120000.000000Z-aaaaaaaaaaaa"
    stale = "20260501T120000.000000Z-bbbbbbbbbbbb"
    fresh_other = "20260516T120000.000000Z-cccccccccccc"
    assert should_purge_cohort_row(
        f"{current}:sk:n1", {"RUN_ID": current, "RECORD_KIND": "entity"},
        current_run_id=current,
        cutoff_utc=cutoff,
    )
    assert should_purge_cohort_row(
        f"{stale}:sk:n1", {"RUN_ID": stale, "RECORD_KIND": "entity"},
        current_run_id=current,
        cutoff_utc=cutoff,
    )
    assert not should_purge_cohort_row(
        f"{fresh_other}:sk:n1", {"RUN_ID": fresh_other, "RECORD_KIND": "entity"},
        current_run_id=current,
        cutoff_utc=cutoff,
    )


def test_should_purge_skips_watermark_and_scope_wm() -> None:
    now = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
    cutoff = now - timedelta(hours=72)
    assert not should_purge_cohort_row(
        "scope_wm_abc",
        {"RECORD_KIND": "watermark"},
        current_run_id="x",
        cutoff_utc=cutoff,
    )
    stale_rid = "20260501T120000.000000Z-bbbbbbbbbbbb"
    assert not should_purge_cohort_row(
        f"{stale_rid}:n1",
        {"RECORD_KIND": "watermark", "RUN_ID": stale_rid},
        current_run_id="current",
        cutoff_utc=cutoff,
    )
