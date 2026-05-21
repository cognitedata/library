#!/usr/bin/env python3
"""Summarize local_run_results task timings and rows_read vs duration (schema v2 only)."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _parse_task_output(out: Any) -> Dict[str, Any]:
    if isinstance(out, dict):
        return out
    if isinstance(out, str) and out.strip().startswith("{"):
        try:
            parsed = json.loads(out)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _pipeline_tasks_from_v2(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    if int(doc.get("schema_version") or 0) != 2:
        return []
    pipeline = doc.get("pipeline") if isinstance(doc.get("pipeline"), dict) else {}
    raw = pipeline.get("tasks") if isinstance(pipeline.get("tasks"), list) else []
    return [t for t in raw if isinstance(t, dict)]


def _load_runs(results_dir: Path) -> List[Tuple[str, List[Dict[str, Any]]]]:
    runs: List[Tuple[str, List[Dict[str, Any]]]] = []
    for path in sorted(results_dir.glob("*_discovery_run.json")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        tasks = _pipeline_tasks_from_v2(doc)
        if tasks:
            runs.append((path.name[:15], tasks))
    return runs


def analyze(results_dir: Path, *, slow_transform_sec: float, slow_rows_read: int) -> int:
    runs = _load_runs(results_dir)
    if not runs:
        print(f"No *_discovery_run.json (schema v2) under {results_dir}", file=sys.stderr)
        return 1

    print(f"Analyzed {len(runs)} run(s) in {results_dir}\n")

    transform_by_rows: Dict[int, List[float]] = defaultdict(list)
    flagged: List[str] = []

    for stamp, tasks in runs:
        total_sec = sum(float(t.get("duration_sec") or 0) for t in tasks)
        by_fn: Dict[str, float] = defaultdict(float)
        for t in tasks:
            fn = str(t.get("function_external_id") or t.get("task_id") or "?")
            by_fn[fn] += float(t.get("duration_sec") or 0)
        top = sorted(by_fn.items(), key=lambda x: -x[1])[:5]
        print(f"{stamp}  tasks={len(tasks)}  sum_task_sec={total_sec:.1f}  top={top}")

        for t in tasks:
            if t.get("function_external_id") != "fn_dm_transform":
                continue
            dt = float(t.get("duration_sec") or 0)
            out = _parse_task_output(t.get("output"))
            rr = out.get("rows_read")
            if rr is not None:
                transform_by_rows[int(rr)].append(dt)
            if (
                rr is not None
                and int(rr) >= slow_rows_read
                and dt >= slow_transform_sec
            ):
                flagged.append(
                    f"  {stamp} {t.get('task_id')}: rows_read={rr} duration_sec={dt:.1f}"
                )

    if transform_by_rows:
        print("\nfn_dm_transform duration_sec by rows_read (median):")
        for rr in sorted(transform_by_rows):
            dts = transform_by_rows[rr]
            print(
                f"  rows_read={rr:5}  n={len(dts):3}  "
                f"median={statistics.median(dts):.2f}s  max={max(dts):.1f}s"
            )

    if flagged:
        print(
            f"\nSlow transforms (rows_read>={slow_rows_read}, duration>={slow_transform_sec}s):"
        )
        for line in flagged:
            print(line)

    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "local_run_results",
    )
    parser.add_argument(
        "--slow-transform-sec",
        type=float,
        default=30.0,
        help="Flag transform tasks slower than this (seconds)",
    )
    parser.add_argument(
        "--slow-rows-read",
        type=int,
        default=500,
        help="Only flag when rows_read is at least this",
    )
    args = parser.parse_args(argv)
    return analyze(
        args.results_dir.resolve(),
        slow_transform_sec=args.slow_transform_sec,
        slow_rows_read=args.slow_rows_read,
    )


if __name__ == "__main__":
    raise SystemExit(main())
