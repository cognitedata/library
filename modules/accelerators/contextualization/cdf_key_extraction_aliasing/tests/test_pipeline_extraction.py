#!/usr/bin/env python3
"""
Test Key Extraction using extraction pipeline configs

This script loads an extraction pipeline config (regex)
and tests it with data queried from CDF based on the source views configuration.

Results are saved to the results/ directory (relative to this test file) in detailed JSON format compatible with view_detailed_results.py.

Usage:
    python tests/test_pipeline_extraction.py [test_type] [--limit N]

    test_type options:
        - regex (default)

    options:
        --limit N    Maximum number of entities to query from CDF (default: 10)
        --use-hardcoded    Use hardcoded test entities instead of querying from CDF
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# Add project root to path (since we're now in tests/)
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig
    from cognite.client.credentials import OAuthClientCredentials, Token
    from cognite.client.data_classes.data_modeling.ids import ViewId
    from dotenv import load_dotenv

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    load_config_from_yaml,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)

# Load environment variables
if CDF_AVAILABLE:
    load_dotenv()

# Map test types to example scope YAML under config/examples/
CONFIG_MAP = {
    "regex": "key_extraction/regex_pump_tag_simple.key_extraction_aliasing.yaml",
}


def load_extraction_config(test_type: str = "regex"):
    """Load the extraction pipeline config for the specified test type."""
    if test_type not in CONFIG_MAP:
        raise ValueError(
            f"Invalid test_type: {test_type}. "
            f"Must be one of: {', '.join(CONFIG_MAP.keys())}"
        )

    config_filename = CONFIG_MAP[test_type]
    config_path = project_root / "config" / "examples" / config_filename

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Use validate=False to work without Pydantic models if fn_dm_key_extraction is not available
    return load_config_from_yaml(str(config_path), validate=False)


def create_cognite_client() -> CogniteClient:
    """Create and return a CogniteClient instance."""
    if not CDF_AVAILABLE:
        raise ImportError("cognite-sdk not available. Install with: poetry install")

    # Try multiple environment variable names
    project = (
        os.getenv("COGNITE_PROJECT") or os.getenv("PROJECT") or os.getenv("CDF_PROJECT")
    )
    base_url = (
        os.getenv("COGNITE_BASE_URL")
        or os.getenv("BASE_URL")
        or os.getenv("CDF_BASE_URL")
        or os.getenv("CDF_URL")
    )
    api_key = (
        os.getenv("COGNITE_API_KEY") or os.getenv("API_KEY") or os.getenv("CDF_API_KEY")
    )

    if api_key:
        if not project:
            raise ValueError(
                "Missing required environment variable: COGNITE_PROJECT or PROJECT or CDF_PROJECT. "
                "Please set it in your .env file or environment."
            )

        config = ClientConfig(
            client_name="key-extraction-testing",
            project=project,
            base_url=base_url,
            credentials=Token(api_key),
            timeout=60,
        )
        return CogniteClient(config=config)

    # Try OAuth (Client Credentials)
    tenant_id = (
        os.getenv("COGNITE_TENANT_ID")
        or os.getenv("TENANT_ID")
        or os.getenv("IDP_TENANT_ID")
    )
    client_id = (
        os.getenv("COGNITE_CLIENT_ID")
        or os.getenv("CLIENT_ID")
        or os.getenv("IDP_CLIENT_ID")
    )
    client_secret = (
        os.getenv("COGNITE_CLIENT_SECRET")
        or os.getenv("CLIENT_SECRET")
        or os.getenv("IDP_CLIENT_SECRET")
    )
    token_url = (
        os.getenv("COGNITE_TOKEN_URL")
        or os.getenv("TOKEN_URL")
        or os.getenv("IDP_TOKEN_URL")
    )
    scopes_str = (
        os.getenv("COGNITE_SCOPES")
        or os.getenv("SCOPES")
        or os.getenv("IDP_SCOPES")
        or ""
    )
    scopes = [s for s in scopes_str.split(" ") if s]

    # If we have a cluster but no base_url, construct it
    if not base_url:
        cluster = os.getenv("CDF_CLUSTER")
        if cluster:
            base_url = f"https://{cluster}.cognitedata.com"

    if tenant_id and client_id and client_secret and token_url and scopes:
        if not project:
            raise ValueError(
                "Missing required environment variable: COGNITE_PROJECT or PROJECT. "
                "Please set it in your .env file or environment."
            )

        credentials = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )
        config = ClientConfig(
            client_name="key-extraction-testing",
            project=project,
            base_url=base_url,
            credentials=credentials,
            timeout=60,
        )
        return CogniteClient(config=config)

    raise ValueError(
        "Missing CDF credentials: provide COGNITE_API_KEY (or API_KEY) or OAuth client credentials in .env"
    )


def _extract_properties_from_cdm_instance(
    instance: Any, view_space: str, view_external_id: str, view_version: str
) -> Dict[str, Any]:
    """Extract properties from a CDM instance and flatten to dict format."""
    try:
        # Get properties from the instance
        instance_data = instance.dump()
        properties = (
            instance_data.get("properties", {})
            .get(view_space, {})
            .get(f"{view_external_id}/{view_version}", {})
        )

        # Return flattened properties
        return properties
    except Exception as e:
        print(f"Warning: Could not extract properties from instance: {e}")
        return {}


def query_entities_from_cdf(
    engine_config: Dict[str, Any], limit: int = 10
) -> List[Dict[str, Any]]:
    """Query entities from CDF based on the source views configuration."""
    if not CDF_AVAILABLE:
        raise ImportError("cognite-sdk not available. Install with: poetry install")

    client = create_cognite_client()
    source_views = engine_config.get("source_views", [])

    if not source_views:
        print("Warning: No source views configured in the extraction config")
        return []

    all_entities = []

    # Process each source view
    for view_config in source_views:
        view_external_id = view_config.get("view_external_id")
        view_space = view_config.get("view_space", "cdf_cdm")
        view_version = view_config.get("view_version", "v1")
        instance_space = view_config.get("instance_space")
        entity_type = view_config.get("entity_type", "asset")
        include_properties = view_config.get("include_properties", [])

        print(f"Querying view: {view_space}/{view_external_id}/{view_version}")
        print(f"  Instance space: {instance_space}")
        print(f"  Entity type: {entity_type}")
        print(f"  Limit: {limit}")

        try:
            view_id = ViewId(
                space=view_space, external_id=view_external_id, version=view_version
            )

            # Query instances (simplified - filters not fully implemented here)
            instances = client.data_modeling.instances.list(
                instance_type="node",
                space=instance_space if instance_space else None,
                sources=[view_id],
                limit=limit,
            )

            instances_list = list(instances)
            print(f"  Found {len(instances_list)} instances")

            # Convert instances to entity format expected by extraction engine
            for instance in instances_list:
                # Extract properties from CDM structure
                properties = _extract_properties_from_cdm_instance(
                    instance, view_space, view_external_id, view_version
                )

                # Build entity dict with required fields
                entity = {
                    "id": instance.external_id,
                    "externalId": instance.external_id,
                    "space": getattr(instance, "space", None),
                    "entity_type": entity_type,  # Store entity type for later use
                }

                # Add properties to entity
                if include_properties:
                    for prop_name in include_properties:
                        if prop_name in properties:
                            entity[prop_name] = properties[prop_name]
                else:
                    # If no specific properties requested, include all
                    entity.update(properties)

                all_entities.append(entity)

        except Exception as e:
            print(f"  Error querying view {view_external_id}: {e}")
            import traceback

            traceback.print_exc()
            continue

    return all_entities


def create_test_entities(test_type: str = "regex"):
    """Create test entities based on the specified test type."""

    if test_type == "regex":
        # Based on test samples from test_extraction_handlers.py and test_extraction_engine.py:
        # Rule 1: pattern '\bP[-_]?\d{1,6}[A-Z]?\b' - pump tags (P-10001, P-10002, P-101, P101A)
        # Rule 2: pattern '\bT[-_]?\d{1,6}[A-Z]?\b' - tank references (T-301, T-201)
        # Rule 3: pattern '\b[A-Z]{2,3}[-_]?\d{1,6}[A-Z]?\b' - instrument tags (FIC-2001, FIC-1001, TIC-4001, PIC-4002)
        # Rule 4: pattern '\bFIC[-_]?\d{4}[A-Z]?\b' - FIC specific (FIC-1001, FIC-2001)
        test_entities = [
            {
                "id": "test_001",
                "externalId": "test_001",
                "name": "P-10001",  # Should match Rule 1 (pump tag)
                "description": "Main feed pump for Tank T-301, controlled by FIC-2001",  # Should extract T-301 (Rule 2) and FIC-2001 (Rule 3)
                "equipmentType": "pump",
                "entity_type": "asset",
            },
            {
                "id": "test_002",
                "externalId": "test_002",
                "name": "P-10002",  # Should match Rule 1 (pump tag)
                "description": "Pump P-10001 connected to P-10002 and P-10003",  # Should extract multiple pump tags
                "equipmentType": "pump",
                "entity_type": "asset",
            },
            {
                "id": "test_003",
                "externalId": "test_003",
                "name": "FIC-1001",  # Should match Rule 3 and Rule 4 (instrument tag)
                "description": "Flow Indicator Controller for process line P-101 feeding Tank T-201",  # Should extract P-101, T-201
                "equipmentType": "instrument",
                "entity_type": "asset",
            },
            {
                "id": "test_004",
                "externalId": "test_004",
                "name": "TIC-4001",  # Should match Rule 3 (instrument tag)
                "description": "Reactor with temperature control TIC-4001 and pressure control PIC-4002",  # Should extract TIC-4001, PIC-4002
                "equipmentType": "instrument",
                "entity_type": "asset",
            },
            {
                "id": "test_005",
                "externalId": "test_005",
                "name": "P101A",  # Should match Rule 1 (pump tag, no separator)
                "description": "Test pump",
                "equipmentType": "pump",
                "entity_type": "asset",
            },
            {
                "id": "test_006",
                "externalId": "test_006",
                "name": "A-FIC-1001",  # Should match Rule 3 (instrument tag with prefix)
                "description": "Flow Indicator Controller for Unit A, process line A-P-101",  # Should extract A-P-101
                "equipmentType": "instrument",
                "entity_type": "asset",
            },
            {
                "id": "test_007",
                "externalId": "test_007",
                "name": "ASSET-P-101",  # Should match Rule 5 (generic asset pattern) → extracts "P-101", also matches Rule 1
                "description": "Main feed pump",
                "equipmentType": "pump",
                "entity_type": "asset",
            },
            {
                "id": "test_008",
                "externalId": "test_008",
                "name": "ASSET-FCV-2001",  # Should match Rule 5 → extracts "FCV-2001", also matches Rule 3
                "description": "Flow control valve",
                "equipmentType": "valve",
                "entity_type": "asset",
            },
            {
                "id": "test_009",
                "externalId": "test_009",
                "name": "ASSET-HEUR001",  # Should match Rule 5 (generic asset pattern) → extracts "HEUR001"
                "description": "Generic equipment without standard tag format",
                "equipmentType": "equipment",
                "entity_type": "asset",
            },
        ]

    else:
        raise ValueError(f"Unknown test_type: {test_type}")

    return test_entities


def format_extraction_result(
    entity: Dict[str, Any], extraction_result: Any, engine_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Format extraction result in the same format as generate_detailed_results.py."""
    entity_type = extraction_result.entity_type

    # Create result data structure
    result_data = {
        entity_type: entity,  # Use entity_type as key (asset, file, timeseries)
        "extraction_result": {
            "entity_id": entity.get("id") or entity.get("externalId"),
            "entity_type": entity_type,
            "candidate_keys": [],
            "foreign_key_references": [],
            "document_references": [],
            "metadata": {
                "extraction_timestamp": datetime.now().isoformat(),
                "total_candidate_keys": len(extraction_result.candidate_keys),
                "total_foreign_keys": len(extraction_result.foreign_key_references),
                "total_document_references": len(extraction_result.document_references),
                "validation_config": engine_config.get(
                    "validation", {"min_confidence": 0.5, "max_keys_per_type": 1000}
                ),
            },
        },
    }

    # Add candidate keys
    for key in extraction_result.candidate_keys:
        result_data["extraction_result"]["candidate_keys"].append(
            {
                "value": key.value,
                "confidence": float(key.confidence),
                "source_field": key.source_field,
                "method": key.method.name
                if hasattr(key.method, "name")
                else str(key.method),
                "rule_id": key.rule_id,
            }
        )

    # Add foreign keys
    for key in extraction_result.foreign_key_references:
        result_data["extraction_result"]["foreign_key_references"].append(
            {
                "value": key.value,
                "confidence": float(key.confidence),
                "source_field": key.source_field,
                "method": key.method.name
                if hasattr(key.method, "name")
                else str(key.method),
                "rule_id": key.rule_id,
            }
        )

    # Add document references
    for doc in extraction_result.document_references:
        result_data["extraction_result"]["document_references"].append(
            {
                "value": doc.value,
                "confidence": float(doc.confidence),
                "source_field": doc.source_field,
                "method": doc.method.name
                if hasattr(doc.method, "name")
                else str(doc.method),
                "rule_id": doc.rule_id,
            }
        )

    return result_data


def save_detailed_results(
    results: List[Dict[str, Any]], test_type: str, summary: Dict[str, Any]
) -> Path:
    """Save detailed results to results/ directory (relative to this test file)."""
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_pipeline_{test_type}_detailed_key_extraction_results.json"
    filepath = results_dir / filename

    # Create output structure
    output = {
        "summary": summary,
        "results": results,
    }

    # Write JSON file
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    return filepath


def main():
    """Run the key extraction test and save detailed results."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Test key extraction with different extraction methods",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tests/test_pipeline_extraction.py regex
        """,
    )
    parser.add_argument(
        "test_type",
        nargs="?",
        default="regex",
        choices=["regex"],
        help="Type of extraction test to run (default: regex)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of entities to query from CDF (default: 10)",
    )
    parser.add_argument(
        "--use-hardcoded",
        action="store_true",
        help="Use hardcoded test entities instead of querying from CDF (for testing without CDF connection)",
    )

    args = parser.parse_args()
    test_type = args.test_type

    print("=" * 80)
    print(f"Key Extraction Test - Using {test_type} extraction method")
    print("=" * 80)
    print()

    # Load config
    print(f"Loading {test_type} extraction config...")
    try:
        engine_config = load_extraction_config(test_type)
        print(f"✓ Config loaded and converted successfully")
        print(f"  - Extraction rules: {len(engine_config.get('extraction_rules', []))}")
        print()
    except Exception as e:
        print(f"✗ Failed to load config: {e}")
        import traceback

        traceback.print_exc()
        return

    # Initialize engine
    print("Initializing KeyExtractionEngine...")
    try:
        engine = KeyExtractionEngine(engine_config)
        print(f"✓ Engine initialized successfully")
        print()
    except Exception as e:
        print(f"✗ Failed to initialize engine: {e}")
        import traceback

        traceback.print_exc()
        return

    # Get test entities - either from CDF or hardcoded
    if args.use_hardcoded:
        print("Using hardcoded test entities...")
        test_entities = create_test_entities(test_type)
    else:
        print("Querying entities from CDF based on source views configuration...")
        try:
            test_entities = query_entities_from_cdf(engine_config, limit=args.limit)
            if not test_entities:
                print("  ⚠ No entities found. Falling back to hardcoded test entities.")
                test_entities = create_test_entities(test_type)
            else:
                print(f"  ✓ Queried {len(test_entities)} entities from CDF")
        except Exception as e:
            print(f"  ✗ Failed to query entities from CDF: {e}")
            print("  Falling back to hardcoded test entities...")
            test_entities = create_test_entities(test_type)

    # Run extraction on each entity
    print()
    print("Running extraction on test entities...")
    print("=" * 80)
    print()

    if not test_entities:
        print("No test entities available.")
        return

    # Collect results for detailed output
    detailed_results = []
    entity_counts = {"asset": 0, "file": 0, "timeseries": 0}

    for i, entity in enumerate(test_entities, 1):
        entity_type = entity.get("entity_type", "asset")
        entity_name = entity.get("name", entity.get("id", "unknown"))

        print(f"Test Entity {i}: {entity_name}")
        print(f"  ID: {entity.get('id')}")
        print(f"  Name: {entity.get('name', 'N/A')}")
        print(f"  Entity Type: {entity_type}")
        print(f"  Description: {entity.get('description', 'N/A')}")
        print()

        try:
            # Extract keys
            result = engine.extract_keys(entity, entity_type=entity_type)

            # Format result for detailed output
            formatted_result = format_extraction_result(entity, result, engine_config)
            detailed_results.append(formatted_result)

            # Track entity counts
            entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1

            # Display results
            print(f"  📊 Extraction Results:")
            print(f"     Candidate Keys: {len(result.candidate_keys)}")
            for key in result.candidate_keys:
                print(f"       • {key.value}")
                print(f"         - Confidence: {key.confidence:.2f}")
                print(f"         - Method: {key.method.value}")
                print(f"         - Source Field: {key.source_field}")
                print(f"         - Rule: {key.rule_id}")

            if result.foreign_key_references:
                print(
                    f"     Foreign Key References: {len(result.foreign_key_references)}"
                )
                for key in result.foreign_key_references:
                    print(f"       • {key.value}")

            if result.document_references:
                print(f"     Document References: {len(result.document_references)}")
                for doc in result.document_references:
                    print(f"       • {doc.value}")

            if (
                len(result.candidate_keys) == 0
                and len(result.foreign_key_references) == 0
            ):
                print("       (No keys extracted)")

            print()

        except Exception as e:
            print(f"  ✗ Error during extraction: {e}")
            import traceback

            traceback.print_exc()
            print()

    # Create summary
    summary = {
        "total_assets": entity_counts.get("asset", 0),
        "total_files": entity_counts.get("file", 0),
        "total_timeseries": entity_counts.get("timeseries", 0),
        "total_candidate_keys": sum(
            len(r["extraction_result"]["candidate_keys"]) for r in detailed_results
        ),
        "total_foreign_keys": sum(
            len(r["extraction_result"]["foreign_key_references"])
            for r in detailed_results
        ),
        "total_document_references": sum(
            len(r["extraction_result"].get("document_references", []))
            for r in detailed_results
        ),
    }

    # Save detailed results
    results_filepath = save_detailed_results(detailed_results, test_type, summary)

    print("=" * 80)
    print("Test completed!")
    print("=" * 80)
    print()
    print(f"📊 Summary:")
    print(f"  Total Assets: {summary['total_assets']}")
    print(f"  Total Files: {summary['total_files']}")
    print(f"  Total Timeseries: {summary['total_timeseries']}")
    print(f"  Candidate Keys: {summary['total_candidate_keys']}")
    print(f"  Foreign Keys: {summary['total_foreign_keys']}")
    print(f"  Document References: {summary['total_document_references']}")
    print()
    print(f"💾 Detailed results saved to: {results_filepath}")
    print(f"   View with: python tests/view_detailed_results.py")
    print()


if __name__ == "__main__":
    main()
