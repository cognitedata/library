#!/usr/bin/env python3
"""
Test script for AssetTagClassifier with CFIHOS and ISA 5.1 configs.
Tests the 4-level hierarchy extraction and classification functionality.
"""

import sys
from pathlib import Path

# Add the module directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from asset_tag_classifier import AssetTagClassifier


def test_classifier(config_path: str, config_name: str):
    """Test the classifier with a given config file."""
    print(f"\n{'='*80}")
    print(f"Testing {config_name} Configuration")
    print(f"{'='*80}")

    try:
        classifier = AssetTagClassifier(config_path)
        print(f"✓ Successfully loaded {config_name} config")
    except Exception as e:
        print(f"✗ Failed to load {config_name} config: {e}")
        return False

    # Tags that are expected to fail for ISA 5.1 (not covered by that standard)
    isa51_expected_failures = {
        "IC-401",  # Instrument Air Compressor - CFIHOS specific
        "STHE-401",  # Shell and Tube Heat Exchanger - CFIHOS specific
        "ST-101",  # Steam Turbine - CFIHOS specific
        "GT-201",  # Gas Turbine - CFIHOS specific
        "G-101",  # Generator - CFIHOS specific
        "SW-301",  # Switchgear - CFIHOS specific
    }

    # Test cases organized by equipment type
    test_cases = [
        # Pumps
        ("CP-101", "Centrifugal Pump (CFIHOS) or Centrifugal Pump (ISA)"),
        ("P-201", "Standard Pump"),
        ("FWP-301", "Feed Water Pump"),
        ("CWP-401", "Cooling Water Pump"),
        ("PD-501", "Positive Displacement Pump"),
        # Compressors
        ("CC-101", "Centrifugal Compressor"),
        ("AC-201", "Air Compressor"),
        ("RC-301", "Reciprocating Compressor"),
        ("IC-401", "Instrument Air Compressor (CFIHOS only)"),
        ("C-501", "Standard Compressor"),
        # Valves
        ("FCV-101", "Flow Control Valve"),
        ("PCV-201", "Pressure Control Valve"),
        ("PSV-301", "Pressure Safety Valve"),
        ("V-401", "Manual Valve"),
        # Instruments
        ("FIC-101", "Flow Indicating Controller"),
        ("PIT-201", "Pressure Indicating Transmitter"),
        ("TIC-301", "Temperature Indicating Controller"),
        ("LIC-401", "Level Indicating Controller"),
        # Vessels and Tanks
        ("T-101", "Tank"),
        ("V-201", "Vessel"),
        ("R-301", "Reactor"),
        ("PV-401", "Pressure Vessel"),
        # Heat Exchangers
        ("E-101", "Heat Exchanger"),
        ("HE-201", "Heat Exchanger"),
        ("CE-301", "Condenser"),
        ("STHE-401", "Shell and Tube Heat Exchanger (CFIHOS only)"),
        # Turbines (CFIHOS only, not in ISA 5.1)
        ("ST-101", "Steam Turbine (CFIHOS only)"),
        ("GT-201", "Gas Turbine (CFIHOS only)"),
        # Columns
        ("C-101", "Column (may match Compressor or Column)"),
        ("DC-201", "Distillation Column"),
        # Electrical (CFIHOS only, not in ISA 5.1)
        ("G-101", "Generator (CFIHOS only)"),
        ("TR-201", "Transformer (may match instrument in ISA 5.1)"),
        ("SW-301", "Switchgear (CFIHOS only)"),
        # With numeric prefixes
        ("24-CP-1234", "Centrifugal Pump with unit prefix"),
        ("15-FWP-801", "Feed Water Pump with unit prefix"),
        ("12-FIC-101", "Flow Controller with unit prefix"),
    ]

    results = {"passed": 0, "failed": 0, "errors": []}

    for tag, description in test_cases:
        try:
            result = classifier.classify_tag(tag)

            if result:
                # Extract hierarchy levels
                level1 = result.get("resourceType", "")
                level2 = result.get("resourceSubType", "")
                level3 = result.get("resourceSubSubType", "")
                level4 = result.get("resourceVariant", "")
                standard = result.get("standard", "")
                pattern = result.get("matched_pattern", "")

                # Format output
                levels = []
                if level1:
                    levels.append(f"L1: {level1}")
                if level2:
                    levels.append(f"L2: {level2}")
                if level3:
                    levels.append(f"L3: {level3}")
                if level4:
                    levels.append(f"L4: {level4}")

                level_str = " | ".join(levels) if levels else "No levels"

                print(f"\n✓ {tag:15} | {description}")
                print(f"  {level_str}")
                print(f"  Standard: {standard} | Pattern: {pattern}")

                results["passed"] += 1
            else:
                # Check if this is an expected failure for ISA 5.1
                is_expected_failure = (
                    config_name == "ISA 5.1" and tag in isa51_expected_failures
                )
                marker = " (expected)" if is_expected_failure else ""
                print(
                    f"\n{'⚠' if is_expected_failure else '✗'} {tag:15} | {description}{marker}"
                )
                print(f"  No classification found")
                if not is_expected_failure:
                    results["failed"] += 1
                    results["errors"].append(f"{tag}: No classification")
                else:
                    results["passed"] += 1  # Count expected failures as passed

        except Exception as e:
            print(f"\n✗ {tag:15} | {description}")
            print(f"  Error: {e}")
            results["failed"] += 1
            results["errors"].append(f"{tag}: {e}")

    # Summary
    print(f"\n{'-'*80}")
    print(f"Summary for {config_name}:")
    print(f"  Passed: {results['passed']}")
    print(f"  Failed: {results['failed']}")
    if results["errors"]:
        print(f"  Errors: {len(results['errors'])}")
        for error in results["errors"][:5]:  # Show first 5 errors
            print(f"    - {error}")

    return results["failed"] == 0


def test_hierarchy_levels():
    """Test that hierarchy levels are correctly extracted."""
    print(f"\n{'='*80}")
    print("Testing 4-Level Hierarchy Extraction")
    print(f"{'='*80}")

    # Test CFIHOS config
    cfihos_classifier = AssetTagClassifier(
        "patterns/asset_tag_classifier.CFIHOS.config.yaml"
    )

    # Test cases that should have different hierarchy depths
    hierarchy_tests = [
        # Should have 3 levels (no variant)
        (
            "CP-101",
            {
                "expected_levels": 3,
                "expected_l1": "Rotating Equipment",
                "expected_l2": "Pump",
                "expected_l3": "Centrifugal Pump",
            },
        ),
        # Should have 4 levels (with variant)
        (
            "FWP-201",
            {
                "expected_levels": 4,
                "expected_l1": "Rotating Equipment",
                "expected_l2": "Pump",
                "expected_l4": "Feed Water Pump",
            },
        ),
        # Should have 3 levels
        (
            "CC-301",
            {
                "expected_levels": 3,
                "expected_l1": "Rotating Equipment",
                "expected_l2": "Compressor",
                "expected_l3": "Centrifugal Compressor",
            },
        ),
        # Should have 4 levels
        (
            "IC-401",
            {
                "expected_levels": 4,
                "expected_l1": "Rotating Equipment",
                "expected_l2": "Compressor",
                "expected_l4": "Instrument Air Compressor",
            },
        ),
    ]

    for tag, expectations in hierarchy_tests:
        result = cfihos_classifier.classify_tag(tag)
        if result:
            level1 = result.get("resourceType", "")
            level2 = result.get("resourceSubType", "")
            level3 = result.get("resourceSubSubType", "")
            level4 = result.get("resourceVariant", "")

            levels = [l for l in [level1, level2, level3, level4] if l]
            num_levels = len(levels)

            print(f"\n{tag}:")
            print(
                f"  Expected {expectations['expected_levels']} levels, got {num_levels}"
            )
            print(f"  L1: {level1}")
            print(f"  L2: {level2}")
            print(f"  L3: {level3}")
            print(f"  L4: {level4}")

            # Check expectations
            if num_levels == expectations["expected_levels"]:
                print(f"  ✓ Level count matches")
            else:
                print(f"  ✗ Level count mismatch")

            if (
                expectations.get("expected_l1")
                and level1 == expectations["expected_l1"]
            ):
                print(f"  ✓ L1 matches")
            elif expectations.get("expected_l1"):
                print(
                    f"  ✗ L1 mismatch: expected '{expectations['expected_l1']}', got '{level1}'"
                )
        else:
            print(f"\n{tag}: No classification found")


def main():
    """Run all tests."""
    print("Asset Tag Classifier Test Suite")
    print("=" * 80)

    # Test CFIHOS config
    cfihos_path = "patterns/asset_tag_classifier.CFIHOS.config.yaml"
    cfihos_success = test_classifier(cfihos_path, "CFIHOS")

    # Test ISA 5.1 config
    isa51_path = "patterns/asset_tag_classifier.ISA51.config.yaml"
    isa51_success = test_classifier(isa51_path, "ISA 5.1")

    # Test hierarchy levels
    test_hierarchy_levels()

    # Final summary
    print(f"\n{'='*80}")
    print("Final Summary")
    print(f"{'='*80}")
    print(f"CFIHOS Config: {'✓ PASSED' if cfihos_success else '✗ FAILED'}")
    print(f"ISA 5.1 Config: {'✓ PASSED' if isa51_success else '✗ FAILED'}")

    if cfihos_success and isa51_success:
        print("\n✓ All tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
