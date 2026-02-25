#!/usr/bin/env python3
"""
Run tests and save results to the results folder.
"""

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def run_tests_and_save_results():
    """Run pytest and save results to JSON files."""
    print("=" * 80)
    print("Running tests and collecting results")
    print("=" * 80)

    # Create results directory
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Run key extraction tests
    print("\n" + "=" * 80)
    print("Running Key Extraction Tests")
    print("=" * 80)

    extraction_result = subprocess.run(
        [
            "poetry",
            "run",
            "pytest",
            "integration/key_extraction/test_key_extraction_scenarios.py",
            "unit/key_extraction/test_extraction_engine.py",
            "unit/key_extraction/test_extraction_handlers.py",
            "-v",
            "--tb=short",
            "-q",
        ],
        capture_output=True,
        text=True,
    )

    # Run aliasing tests
    print("\n" + "=" * 80)
    print("Running Aliasing Tests")
    print("=" * 80)

    aliasing_result = subprocess.run(
        [
            "poetry",
            "run",
            "pytest",
            "unit/aliasing/test_aliasing_engine.py",
            "-v",
            "--tb=short",
            "-q",
        ],
        capture_output=True,
        text=True,
    )

    # Run all tests
    print("\n" + "=" * 80)
    print("Running All Tests")
    print("=" * 80)

    all_result = subprocess.run(
        ["poetry", "run", "pytest", ".", "-v", "--tb=short", "-q"],
        capture_output=True,
        text=True,
    )

    # Parse extraction results
    extraction_tests = parse_test_results(extraction_result.stdout)
    extraction_summary = create_test_summary(
        extraction_result, extraction_tests, "Key Extraction"
    )

    # Parse aliasing results
    aliasing_tests = parse_test_results(aliasing_result.stdout)
    aliasing_summary = create_test_summary(aliasing_result, aliasing_tests, "Aliasing")

    # Parse all results
    all_tests = parse_test_results(all_result.stdout)
    all_summary = create_test_summary(all_result, all_tests, "All Tests")

    # Save extraction results
    extraction_file = results_dir / f"{timestamp}_key_extraction_results.json"
    with open(extraction_file, "w") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "summary": extraction_summary,
                "tests": extraction_tests,
                "output": extraction_result.stdout,
            },
            f,
            indent=2,
        )
    print(f"✓ Saved extraction results to {extraction_file}")

    # Save aliasing results
    aliasing_file = results_dir / f"{timestamp}_aliasing_results.json"
    with open(aliasing_file, "w") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "summary": aliasing_summary,
                "tests": aliasing_tests,
                "output": aliasing_result.stdout,
            },
            f,
            indent=2,
        )
    print(f"✓ Saved aliasing results to {aliasing_file}")

    # Save all test summary
    summary_file = results_dir / f"{timestamp}_test_summary.json"
    with open(summary_file, "w") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "key_extraction": extraction_summary,
                "aliasing": aliasing_summary,
                "all_tests": all_summary,
            },
            f,
            indent=2,
        )
    print(f"✓ Saved test summary to {summary_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(
        f"\nKey Extraction: {extraction_summary['tests_passed']} passed, "
        f"{extraction_summary['tests_failed']} failed"
    )
    print(
        f"Aliasing: {aliasing_summary['tests_passed']} passed, "
        f"{aliasing_summary['tests_failed']} failed"
    )
    print(
        f"All Tests: {all_summary['tests_passed']} passed, "
        f"{all_summary['tests_failed']} failed"
    )

    return 0 if all_result.returncode == 0 else 1


def parse_test_results(output):
    """Parse test output to extract individual test results."""
    tests = []
    for line in output.split("\n"):
        # Match test lines like "unit/key_extraction/test_file.py::TestClass::test_method PASSED [ 42%]"
        match = re.match(r"(.+?::.+?::.+?)\s+(PASSED|FAILED|ERROR)\s+\[", line)
        if match:
            test_name = match.group(1)
            status = match.group(2).lower()
            tests.append(
                {
                    "name": test_name,
                    "status": status,
                }
            )
    return tests


def create_test_summary(result, tests, test_type):
    """Create a summary of test results."""
    passed = len([t for t in tests if t["status"] == "passed"])
    failed = len([t for t in tests if t["status"] == "failed"])

    summary = {
        "test_type": test_type,
        "timestamp": datetime.now().isoformat(),
        "exit_code": result.returncode,
        "tests_run": len(tests),
        "tests_passed": passed,
        "tests_failed": failed,
        "tests_errors": len([t for t in tests if t["status"] == "error"]),
    }

    if summary["tests_run"] > 0:
        summary["success_rate"] = (passed / summary["tests_run"]) * 100
    else:
        summary["success_rate"] = 0

    return summary


if __name__ == "__main__":
    exit_code = run_tests_and_save_results()
    sys.exit(exit_code)
