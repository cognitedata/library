#!/usr/bin/env python3
"""
View detailed test results from the results folder.
"""

import json
import sys
from pathlib import Path


def find_latest_results():
    """Find the latest detailed result files."""
    results_dir = Path(__file__).parent / "results"

    # Support both old and new filename patterns
    extraction_files = list(
        results_dir.glob("*detailed_key_extraction_results.json")
    ) + list(results_dir.glob("*pipeline_*_detailed_key_extraction_results.json"))
    aliasing_files = list(results_dir.glob("*detailed_aliasing_results.json"))

    if not extraction_files:
        print("No detailed extraction results found.")
        return None, None

    if not aliasing_files:
        print("No detailed aliasing results found.")
        return extraction_files[0], None

    # Get most recent files
    extraction_files.sort(reverse=True)
    aliasing_files.sort(reverse=True)

    return (
        extraction_files[0] if extraction_files else None,
        aliasing_files[0] if aliasing_files else None,
    )


def print_extraction_results(file_path):
    """Print extraction results."""
    with open(file_path, "r") as f:
        data = json.load(f)

    print("=" * 80)
    print("KEY EXTRACTION RESULTS")
    print("=" * 80)

    # Print summary
    summary = data["summary"]
    print(f"\nSummary:")
    print(f"  Total Assets: {summary['total_assets']}")
    print(f"  Candidate Keys: {summary['total_candidate_keys']}")
    print(f"  Foreign Keys: {summary['total_foreign_keys']}")

    # Print detailed results
    print("\n" + "=" * 80)
    print("DETAILED RESULTS")
    print("=" * 80)

    for i, result in enumerate(data["results"], 1):
        asset = result["asset"]
        extraction = result["extraction_result"]

        print(f"\n--- Asset {i}: {asset['name']} ---")
        print(f"ID: {asset['id']}")
        print(f"Description: {asset.get('description', 'N/A')}")
        if asset.get("metadata"):
            print(f"Metadata: {asset['metadata']}")

        print(f"\n  Candidate Keys ({len(extraction['candidate_keys'])}):")
        for key in extraction["candidate_keys"]:
            print(f"    - {key['value']}")
            print(f"      Confidence: {key['confidence']:.2f}")
            print(f"      Source: {key['source_field']}")
            print(f"      Method: {key['method']}")

        if extraction["foreign_key_references"]:
            print(
                f"\n  Foreign Key References ({len(extraction['foreign_key_references'])}):"
            )
            for key in extraction["foreign_key_references"]:
                print(f"    - {key['value']}")
                print(f"      Confidence: {key['confidence']:.2f}")
                print(f"      Source: {key['source_field']}")


def print_aliasing_results(file_path):
    """Print aliasing results."""
    with open(file_path, "r") as f:
        data = json.load(f)

    print("\n" + "=" * 80)
    print("ALIASING RESULTS")
    print("=" * 80)

    # Print summary
    summary = data["summary"]
    print(f"\nSummary:")
    print(f"  Total Tags: {summary['total_tags']}")
    print(f"  Total Aliases: {summary['total_aliases']}")
    print(f"  Average Aliases/Tag: {summary['average_aliases_per_tag']:.2f}")

    # Print detailed results
    print("\n" + "=" * 80)
    print("DETAILED RESULTS")
    print("=" * 80)

    for i, result in enumerate(data["results"], 1):
        tag = result["tag"]
        aliases = result["aliases"]
        metadata = result["metadata"]

        print(f"\n--- Tag {i}: {tag} ---")
        print(f"Total Aliases: {len(aliases)}")
        print(f"Rules Applied: {', '.join(metadata['rules_applied'])}")
        print(f"\nAliases:")
        for alias in sorted(aliases):
            marker = " (original)" if alias == tag else ""
            print(f"  - {alias}{marker}")


def main():
    """Main function to view detailed results."""
    extraction_file, aliasing_file = find_latest_results()

    if not extraction_file and not aliasing_file:
        print(
            "No detailed results found. Run: poetry run python tests/generate_detailed_results.py"
        )
        sys.exit(1)

    if extraction_file:
        print(f"\nReading from: {extraction_file.name}")
        print_extraction_results(extraction_file)

    if aliasing_file:
        print(f"\nReading from: {aliasing_file.name}")
        print_aliasing_results(aliasing_file)

    print("\n" + "=" * 80)
    print("End of Results")
    print("=" * 80)


if __name__ == "__main__":
    main()
