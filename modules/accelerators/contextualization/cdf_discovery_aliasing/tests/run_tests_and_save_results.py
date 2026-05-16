#!/usr/bin/env python3
"""
Run pytest for this module and save a JSON summary under ``tests/results/``.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def run_tests_and_save_results() -> int:
    """Run pytest and save results to JSON files."""
    print("=" * 80)
    print("Running tests and collecting results")
    print("=" * 80)

    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    all_result = subprocess.run(
        [sys.executable, "-m", "pytest", str(Path(__file__).parent), "-v", "--tb=short", "-q"],
        capture_output=True,
        text=True,
    )

    all_tests = parse_test_results(all_result.stdout)
    all_summary = create_test_summary(all_result, all_tests, "All Tests")

    summary_file = results_dir / f"{timestamp}_test_summary.json"
    with summary_file.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "all_tests": all_summary,
            },
            f,
            indent=2,
        )
    print(f"Saved test summary to {summary_file}")

    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(
        f"\nAll Tests: {all_summary['tests_passed']} passed, "
        f"{all_summary['tests_failed']} failed"
    )

    return 0 if all_result.returncode == 0 else 1


def parse_test_results(output: str) -> list:
    """Parse test output to extract individual test results."""
    tests = []
    for line in output.split("\n"):
        match = re.match(r"(.+?::.+?::.+?)\s+(PASSED|FAILED|ERROR)\s+\[", line)
        if match:
            tests.append(
                {
                    "name": match.group(1),
                    "status": match.group(2).lower(),
                }
            )
    return tests


def create_test_summary(result: subprocess.CompletedProcess, tests: list, test_type: str) -> dict:
    """Create a summary of test results."""
    passed = len([t for t in tests if t["status"] == "passed"])
    failed = len([t for t in tests if t["status"] == "failed"])

    return {
        "test_type": test_type,
        "timestamp": datetime.now().isoformat(),
        "returncode": result.returncode,
        "tests_passed": passed,
        "tests_failed": failed,
        "total_tests": len(tests),
    }


if __name__ == "__main__":
    sys.exit(run_tests_and_save_results())
