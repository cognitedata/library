"""
Main Entry Point - Fetch CDF instances from data model views, run key extraction and aliasing, write results.

Reads CDF credentials from environment (.env supported), queries instances from configured views,
runs the key extraction engine followed by the aliasing engine, and writes
JSON results into the tests/results/ directory (relative to this package).
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# Paths relative to this file (key_extraction_aliasing package dir)
SCRIPT_DIR = Path(__file__).resolve().parent
# Repo root is one level above modules (library)
REPO_ROOT = SCRIPT_DIR.parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Import our modules
try:
    from modules.contextualization.key_extraction_aliasing.functions.fn_dm_alias_persistence.pipeline import (
        persist_aliases_to_entities,
    )
    from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
        _convert_yaml_direct_to_aliasing_config,
    )
    from modules.contextualization.key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
        AliasingEngine,
    )
    from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
        _convert_rule_dict_to_engine_format,
    )
    from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
        load_config_from_yaml as load_extraction_yaml,
    )
    from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
        KeyExtractionEngine,
    )

    MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Import error: {e}")
    MODULES_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _load_env() -> None:
    """Load environment variables from .env if present. Prefer repo root .env."""
    try:
        from dotenv import load_dotenv

        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            load_dotenv()
    except Exception:
        pass


def _create_cognite_client():
    """Create a CogniteClient using either API key or OAuth credentials from env."""
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.credentials import OAuthClientCredentials, Token

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
        credentials = Token(api_key)
        config = ClientConfig(
            client_name="key-extraction-aliasing",
            project=project,
            base_url=base_url,
            credentials=credentials,
        )
        return CogniteClient(config=config)

    # OAuth (Client Credentials) - support both COGNITE_* and IDP_* prefixes
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

    if not (tenant_id and client_id and client_secret and token_url and scopes):
        raise RuntimeError(
            "Missing CDF credentials: provide COGNITE_API_KEY or OAuth client credentials in .env"
        )

    credentials = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )
    config = ClientConfig(
        client_name="key-extraction-aliasing",
        project=project,
        base_url=base_url,
        credentials=credentials,
    )
    return CogniteClient(config=config)


def _load_configs() -> tuple[
    Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Optional[str]
]:
    """Load extraction, aliasing configs, all source view configs, and alias persistence options.

    Loads extraction rules and source views from all pipeline configs,
    and aliasing config from aliasing pipeline configs.
    alias_writeback_property is read from the first *aliasing*.config.yaml whose
    config.parameters defines alias_writeback_property.
    """

    # Load aliasing rules from aliasing pipeline configs
    pipelines_dir = SCRIPT_DIR / "pipelines"
    all_aliasing_rules = []
    alias_writeback_property: Optional[str] = None

    for config_file in sorted(pipelines_dir.glob("*aliasing*.config.yaml")):
        try:
            with open(config_file, "r") as f:
                pipeline_config = yaml.safe_load(f)

            config_data = pipeline_config.get("config", {}).get("data", {})
            parameters = pipeline_config.get("config", {}).get("parameters", {})
            if (
                alias_writeback_property is None
                and isinstance(parameters, dict)
                and parameters.get("alias_writeback_property") is not None
            ):
                raw_prop = parameters.get("alias_writeback_property")
                if isinstance(raw_prop, str) and raw_prop.strip():
                    alias_writeback_property = raw_prop.strip()
                    logger.info(
                        f"Loaded alias_writeback_property from {config_file.name}: "
                        f"{alias_writeback_property!r}"
                    )

            # Use adapter to convert aliasing rules to engine format
            aliasing_config = _convert_yaml_direct_to_aliasing_config(
                {"config": {"data": config_data}}
            )
            converted_rules = aliasing_config.get("rules", [])
            all_aliasing_rules.extend(converted_rules)
            logger.info(
                f"Loaded {len(converted_rules)} aliasing rules from {config_file.name}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to load aliasing pipeline config {config_file.name}: {e}"
            )
            continue

    # Build aliasing config from pipeline rules only (no fallback to default.yaml)
    if not all_aliasing_rules:
        logger.warning("No aliasing pipeline configs found! Aliasing will be disabled.")
        aliasing_config = {"rules": [], "validation": {}}
    else:
        # Build aliasing config from pipeline rules
        aliasing_config = {
            "rules": all_aliasing_rules,
            "validation": {
                "max_aliases_per_tag": 50,
                "min_alias_length": 2,
                "max_alias_length": 50,
                "allowed_characters": r"A-Za-z0-9-_/. ",
            },
        }
        logger.info(f"Total aliasing rules loaded: {len(all_aliasing_rules)}")

    # Load extraction rules and source views from all pipeline configs
    all_extraction_rules = []
    all_source_views = []
    seen_source_views = set()  # Track unique source views by key

    for config_file in sorted(pipelines_dir.glob("*key_extraction*.config.yaml")):
        try:
            with open(config_file, "r") as f:
                pipeline_config = yaml.safe_load(f)

            config_data = pipeline_config.get("config", {}).get("data", {})

            # Collect extraction rules and convert them using the adapter
            rules = config_data.get("extraction_rules", [])
            converted_rules = []
            for rule in rules:
                # Use adapter to convert rule parameters to config format
                converted_rule = _convert_rule_dict_to_engine_format(rule)
                if converted_rule:
                    converted_rules.append(converted_rule)
            all_extraction_rules.extend(converted_rules)
            logger.info(f"Loaded {len(converted_rules)} rules from {config_file.name}")

            # Collect unique source views
            source_views = config_data.get("source_views", [])
            for view in source_views:
                # Create unique key for view
                view_key = (
                    view.get("view_space", ""),
                    view.get("view_external_id", ""),
                    view.get("view_version", ""),
                    view.get("instance_space", ""),
                    view.get("entity_type", ""),
                )
                if view_key not in seen_source_views:
                    seen_source_views.add(view_key)
                    all_source_views.append(view)
                    logger.info(
                        f"Added source view: {view.get('view_external_id')} ({view.get('entity_type')})"
                    )
        except Exception as e:
            logger.warning(f"Failed to load pipeline config {config_file.name}: {e}")
            continue

    # Default to a single CogniteAsset view if none configured
    if not all_source_views:
        logger.warning(
            "No source views found in pipeline configs, using default CogniteAsset view"
        )
        all_source_views = [
            {
                "view_external_id": "CogniteAsset",
                "view_space": "cdf_cdm",
                "view_version": "v1",
                "entity_type": "asset",
            }
        ]

    # Collect validation config from all pipeline configs (use first non-empty one, or default)
    validation_config = {"min_confidence": 0.5, "max_keys_per_type": 1000}
    for config_file in sorted(pipelines_dir.glob("*.config.yaml")):
        try:
            with open(config_file, "r") as f:
                pipeline_config = yaml.safe_load(f)
            config_data = pipeline_config.get("config", {}).get("data", {})
            pipeline_validation = config_data.get("validation", {})
            if pipeline_validation:
                validation_config.update(pipeline_validation)
                logger.info(f"Loaded validation config from {config_file.name}")
                break  # Use first pipeline config's validation settings
        except Exception:
            continue

    extraction_config = {
        "extraction_rules": all_extraction_rules,
        "validation": validation_config,
    }

    logger.info(f"Total extraction rules loaded: {len(all_extraction_rules)}")
    logger.info(f"Total source views: {len(all_source_views)}")

    return extraction_config, aliasing_config, all_source_views, alias_writeback_property


def _ensure_results_dir() -> Path:
    """Ensure results directory exists."""
    results_dir = SCRIPT_DIR / "tests" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


def _generate_report(
    extraction_path: Path, aliasing_path: Path, logger: logging.Logger
) -> None:
    """Generate/update the key extraction aliasing report from results files."""
    from collections import Counter, defaultdict

    # Load results
    with open(extraction_path) as f:
        extraction_data = json.load(f)

    with open(aliasing_path) as f:
        aliasing_data = json.load(f)

    # Analyze extraction
    extraction_results = extraction_data.get("results", [])
    entity_types = defaultdict(int)
    candidate_keys_by_type = defaultdict(int)
    foreign_keys_by_type = defaultdict(int)
    methods = Counter()
    rules = Counter()
    confidence_scores = []
    view_stats = defaultdict(int)

    for result in extraction_results:
        entity = result.get("entity", {})
        view_id = result.get("view_external_id", "unknown")
        view_stats[view_id] += 1

        ext_result = result.get("extraction_result", {})
        entity_type = ext_result.get("entity_type", "unknown")
        entity_types[entity_type] += 1

        for key in ext_result.get("candidate_keys", []):
            candidate_keys_by_type[entity_type] += 1
            methods[key.get("method", "unknown")] += 1
            rules[key.get("rule_name", "unknown")] += 1
            confidence_scores.append(key.get("confidence", 0))

        for fk in ext_result.get("foreign_key_references", []):
            foreign_keys_by_type[entity_type] += 1

    # Analyze aliasing
    aliasing_results = aliasing_data.get("results", [])
    rule_usage = Counter()
    alias_counts = []
    entity_type_aliasing = defaultdict(int)
    entity_type_alias_counts = defaultdict(int)
    entity_type_rules = defaultdict(Counter)

    for result in aliasing_results:
        entity_type = result.get("entity_type", "unknown")
        aliases = result.get("aliases", [])
        alias_count = len(aliases)
        alias_counts.append(alias_count)
        entity_type_aliasing[entity_type] += 1
        entity_type_alias_counts[entity_type] += alias_count

        for rule in result.get("metadata", {}).get("applied_rules", []):
            rule_usage[rule] += 1
            entity_type_rules[entity_type][rule] += 1

    # Calculate derived stats
    avg_aliases = sum(alias_counts) / len(alias_counts) if alias_counts else 0
    min_aliases = min(alias_counts) if alias_counts else 0
    max_aliases = max(alias_counts) if alias_counts else 0
    median_aliases = sorted(alias_counts)[len(alias_counts) // 2] if alias_counts else 0

    avg_confidence = (
        sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
    )
    min_confidence = min(confidence_scores) if confidence_scores else 0
    max_confidence = max(confidence_scores) if confidence_scores else 0

    top_rule_apps = (
        sum([r[1] for r in rule_usage.most_common(3)]) if len(rule_usage) >= 3 else 0
    )
    total_rule_applications = sum(rule_usage.values())
    top_3_pct = (
        (top_rule_apps / total_rule_applications * 100)
        if total_rule_applications > 0
        else 0
    )

    top_method = methods.most_common(1)[0] if methods else None
    top_entity_type = (
        max(entity_types.items(), key=lambda x: x[1]) if entity_types else None
    )
    top_extraction_rule = rules.most_common(1)[0] if rules else None
    top_3_rules = (
        [r[0] for r in rule_usage.most_common(3)] if len(rule_usage) >= 3 else []
    )

    # Extract timestamp from filename
    results_source = extraction_path.stem.replace("_cdf_extraction", "")

    # Generate markdown report
    report_content = f"""# Key Extraction and Aliasing Results Summary

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Results Source:** {results_source}

## Executive Summary

This document provides a comprehensive analysis of the key extraction and aliasing pipeline execution results, covering processing statistics, entity type breakdowns, extraction methods, and aliasing transformations.

### Workflow Overview

The following diagram illustrates the complete workflow process from key extraction through aliasing to persistence:

![Workflow Diagram](../workflows/workflow_diagram.png)

**Workflow Steps:**
1. **Key Extraction** - Extracts candidate keys, foreign key references, and document references from CDF entities
2. **Result Splitting** - Separates extraction results into distinct streams based on type
3. **Aliasing** - Generates aliases for candidate keys to improve matching
4. **Write Aliases** - Persists the alias list to a property on CogniteDescribable (default `aliases`; configurable via pipeline or workflow `data`)
5. **Reference Catalog** - Stores foreign key references and document references (future implementation)

---

## 📊 Key Extraction Results

### Overall Statistics

- **Total Entities Processed:** {len(extraction_results)}
- **Total Candidate Keys Extracted:** {sum(candidate_keys_by_type.values())}
- **Total Foreign Key References:** {sum(foreign_keys_by_type.values())}
- **Total Document References:** {sum(len(r.get('extraction_result', {}).get('document_references', [])) for r in extraction_results)}

### Entity Type Breakdown

| Entity Type | Count | Candidate Keys | Foreign Keys | Document Refs |
|------------|-------|---------------|--------------|---------------|
"""
    for entity_type in sorted(entity_types.keys()):
        doc_refs = len(
            [
                r
                for r in extraction_results
                if r.get("extraction_result", {}).get("entity_type") == entity_type
                and r.get("extraction_result", {}).get("document_references")
            ]
        )
        report_content += f"| {entity_type.capitalize()} | {entity_types[entity_type]} | {candidate_keys_by_type[entity_type]} | {foreign_keys_by_type[entity_type]} | {doc_refs} |\n"

    report_content += f"""
### Source View Statistics

| View | Entity Count |
|------|--------------|
"""
    for view, count in sorted(view_stats.items()):
        report_content += f"| {view} | {count} |\n"

    report_content += f"""
### Extraction Methods

| Method | Count | Percentage |
|--------|-------|------------|
"""
    total_methods = sum(methods.values())
    for method, count in methods.most_common():
        pct = (count / total_methods * 100) if total_methods > 0 else 0
        report_content += f"| {method.capitalize()} | {count} | {pct:.1f}% |\n"

    report_content += f"""
### Top Extraction Rules

| Rule Name | Count | Percentage |
|-----------|-------|------------|
"""
    total_rules = sum(rules.values())
    for rule, count in rules.most_common(10):
        pct = (count / total_rules * 100) if total_rules > 0 else 0
        report_content += f"| {rule} | {count} | {pct:.1f}% |\n"

    if confidence_scores:
        report_content += f"""
### Confidence Score Statistics

- **Average Confidence:** {avg_confidence:.3f}
- **Minimum Confidence:** {min_confidence:.3f}
- **Maximum Confidence:** {max_confidence:.3f}
"""

    report_content += f"""

### Extraction Methods Description

The key extraction system uses different methods to identify and extract key information from entity data:

#### 1. Regex Extraction ({methods.get('regex', 0) / total_methods * 100 if total_methods > 0 else 0:.1f}% of extractions)

**What it does:** Identifies structured patterns in text, such as equipment tags like `P-101` or `FCV-2001A`.

**How it works:**
- Looks for specific patterns and formats in entity names and descriptions
- Works best with consistent, well-formatted identifiers
- Commonly used for equipment tags, instrument identifiers, and file names

**Best for:** Structured data with consistent naming conventions

#### 2. Heuristic Extraction ({methods.get('heuristic', 0) / total_methods * 100 if total_methods > 0 else 0:.1f}% of extractions)

**What it does:** Identifies keys in data that doesn't follow strict patterns, using statistical analysis and context clues.

**How it works:**
- Uses multiple strategies to find identifiers even when formats vary
- Analyzes text position, character patterns, and surrounding context
- Learns from examples to identify similar patterns

**Best for:** Inconsistent or legacy data where formats vary across systems

#### 3. Fixed Width Extraction

**What it does:** Extracts information from structured text where data appears in fixed positions (like columns in a table).

**Best for:** Tabular data or fixed-format records from legacy systems

#### 4. Token Reassembly Extraction

**What it does:** Builds complete identifiers by combining pieces of information from multiple fields (like combining site, unit, and equipment codes).

**Best for:** Hierarchical naming systems where tags are constructed from multiple components


---

## 🔄 Aliasing Results

### Overall Statistics

- **Total Tags Processed:** {len(aliasing_results)}
- **Total Aliases Generated:** {sum(alias_counts)}
- **Average Aliases per Tag:** {avg_aliases:.2f}
- **Unique Transformation Rules Applied:** {len(rule_usage)}
"""

    if alias_counts:
        report_content += f"""
### Alias Distribution

- **Minimum Aliases per Tag:** {min_aliases}
- **Maximum Aliases per Tag:** {max_aliases}
- **Median Aliases per Tag:** {median_aliases}
"""

    report_content += f"""
### Entity Type Aliasing Statistics

| Entity Type | Tags Processed | Total Aliases | Avg Aliases/Tag |
|------------|---------------|---------------|-----------------|
"""
    for entity_type in sorted(entity_type_aliasing.keys()):
        tags = entity_type_aliasing[entity_type]
        aliases = entity_type_alias_counts[entity_type]
        avg = aliases / tags if tags > 0 else 0
        report_content += (
            f"| {entity_type.capitalize()} | {tags} | {aliases} | {avg:.2f} |\n"
        )

    report_content += f"""
### Top Applied Transformation Rules by Entity Type

#### Asset Entity Type

| Rule Name | Application Count | Percentage |
|-----------|------------------|------------|
"""
    asset_total = sum(entity_type_rules.get("asset", {}).values())
    for rule, count in entity_type_rules.get("asset", {}).most_common(15):
        pct = (count / asset_total * 100) if asset_total > 0 else 0
        report_content += f"| {rule} | {count} | {pct:.1f}% |\n"

    if asset_total > 0:
        asset_rule_count = len(entity_type_rules.get("asset", {}))
        report_content += f"\n**Note:** Asset entities receive the most comprehensive transformation coverage, with {asset_rule_count} transformation rules applied across {entity_type_aliasing.get('asset', 0)} asset tags processed.\n"

    report_content += f"""
#### File Entity Type

| Rule Name | Application Count | Percentage |
|-----------|------------------|------------|
"""
    file_total = sum(entity_type_rules.get("file", {}).values())
    for rule, count in entity_type_rules.get("file", {}).most_common(15):
        pct = (count / file_total * 100) if file_total > 0 else 0
        report_content += f"| {rule} | {count} | {pct:.1f}% |\n"

    if file_total > 0:
        report_content += f"\n**Note:** File entities receive specialized document-specific transformations, focusing on document naming variants and revision number handling.\n"

    report_content += f"""
#### Timeseries Entity Type

| Rule Name | Application Count | Percentage |
|-----------|------------------|------------|
"""
    ts_total = sum(entity_type_rules.get("timeseries", {}).values())
    for rule, count in entity_type_rules.get("timeseries", {}).most_common(15):
        pct = (count / ts_total * 100) if ts_total > 0 else 0
        report_content += f"| {rule} | {count} | {pct:.1f}% |\n"

    if ts_total > 0:
        report_content += f"\n**Note:** Timeseries entities receive streamlined transformations focusing on separator variants, instrument expansion, case variations, and zero normalization - the most commonly needed transformations for data stream identifiers.\n"

    report_content += f"""

### Aliasing Transformation Types Description

The aliasing system generates alternative names (aliases) for extracted keys to improve matching across different systems. This is important because the same equipment might be referred to differently in various systems (e.g., `P-101`, `P_101`, `P101` all refer to the same pump).

**12 transformation types are available:**

1. **Character Substitution** - Creates variations by changing separators (hyphens, underscores, spaces)
2. **Prefix/Suffix Operations** - Adds site, unit, or equipment type prefixes/suffixes based on context
3. **Regex Substitution** - Normalizes tag formats to standard conventions
4. **Case Transformation** - Creates uppercase, lowercase, and title case variants
5. **Equipment Type Expansion** - Expands abbreviations (P → PUMP, PMP) to generate full names
6. **Related Instruments** - Generates related instrument tags (FIC, PIC, TIC, LIC) for equipment
7. **Hierarchical Expansion** - Creates full hierarchical paths (site-unit-equipment) when context is available
8. **Document Aliases** - Handles document naming variants, removes revision numbers from file names
9. **Leading Zero Normalization** - Normalizes numeric formatting (P-001, P-01, P-1)
10. **Pattern Recognition** - Recognizes industry-standard patterns (ISA/ANSI) and identifies equipment types
11. **Pattern-Based Expansion** - Generates ISA-compliant aliases following industry standards
12. **Composite Transformations** - Chains multiple transformations together for complex alias generation

**Why this matters:** Different systems may use different formats for the same identifier. By generating aliases, the system can match entities even when they're named differently, improving data integration and search capabilities.


---

## 📈 Key Insights

### Extraction Insights

1. **Method Distribution:** {top_method[0].capitalize() if top_method else 'N/A'} extraction method is the most commonly used ({top_method[1] if top_method else 0} occurrences).
2. **Entity Type Coverage:** The pipeline processed {len(entity_types)} different entity types, with {top_entity_type[0] if top_entity_type else 'N/A'} representing the largest processing volume ({top_entity_type[1] if top_entity_type else 0} entities).
3. **Rule Effectiveness:** The top extraction rule ({top_extraction_rule[0] if top_extraction_rule else 'N/A'}) was applied {top_extraction_rule[1] if top_extraction_rule else 0} times.

### Aliasing Insights

1. **Transformation Coverage:** {len(rule_usage)} unique transformation rules were applied across all processed tags.
2. **Alias Generation:** On average, each tag generated {avg_aliases:.2f} aliases, demonstrating effective variant generation.
3. **Most Common Transformations:** The top 3 transformation rules ({', '.join(top_3_rules) if top_3_rules else 'N/A'}) account for {top_3_pct:.1f}% of all rule applications.
"""

    # Add sample results table
    report_content += f"""

---

## 🔍 Sample Results

### Sample Extraction Results

The following table shows examples of entities processed by the extraction pipeline, organized by entity type:

| Entity Type | Entity ID | Entity Name | Candidate Keys | Foreign Key References | Document References |
|-------------|-----------|-------------|----------------|----------------------|---------------------|
"""

    # Select diverse examples by entity type
    examples = {"asset": [], "file": [], "timeseries": []}

    for result in extraction_results:
        entity_type = result.get("extraction_result", {}).get("entity_type", "unknown")
        if entity_type in examples and len(examples[entity_type]) < 3:
            entity = result.get("entity", {})
            ext_result = result.get("extraction_result", {})

            candidate_keys = [
                k.get("value", "") for k in ext_result.get("candidate_keys", [])
            ]
            foreign_keys = [
                fk.get("value", "")
                for fk in ext_result.get("foreign_key_references", [])
            ]
            doc_refs = [
                dr.get("value", "") for dr in ext_result.get("document_references", [])
            ]

            examples[entity_type].append(
                {
                    "id": entity.get("id", entity.get("externalId", "N/A"))[:40],
                    "name": entity.get("name", "N/A")[:40],
                    "candidate_keys": ", ".join(candidate_keys[:2])
                    if candidate_keys
                    else "None",
                    "foreign_keys": ", ".join(foreign_keys[:2])
                    if foreign_keys
                    else "None",
                    "doc_refs": ", ".join(doc_refs[:2]) if doc_refs else "None",
                }
            )

    for entity_type in ["asset", "file", "timeseries"]:
        for example in examples[entity_type]:
            report_content += f"| {entity_type.capitalize()} | {example['id']} | {example['name']} | {example['candidate_keys']} | {example['foreign_keys']} | {example['doc_refs']} |\n"

    report_content += """
**What this shows:**

- **Candidate Keys** are the primary identifiers extracted from entity names (e.g., equipment tags like `P-101`, file names like `PH-25578-P-4110006-001.pdf`)
- **Foreign Key References** are references to other entities found in descriptions (e.g., pump P-101 references tank T-301)
- **Document References** are references to engineering drawings or documents (none found in these examples)
- **Entity Types** indicate the type of entity: Assets (equipment), Files (documents), or Timeseries (data streams)

---

## 📝 Notes

- Results were generated from CDF Data Model views: CogniteAsset, CogniteFile, and CogniteTimeSeries
- All extracted candidate keys were processed through the aliasing pipeline
- Aliases have been written back on the CogniteDescribable view using the configured DM property (default `aliases`; see `alias_writeback_property` in aliasing pipeline parameters or `aliasWritebackProperty` in persistence task `data`)
"""

    if confidence_scores:
        report_content += f"- Confidence scores for extraction range from {min_confidence:.3f} to {max_confidence:.3f}\n"

    report_content += "\n---\n\n*This summary was automatically generated from the latest pipeline execution results.*\n"

    # Write report
    report_path = SCRIPT_DIR / "docs" / "key_extraction_aliasing_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report_content, encoding="utf-8")
    logger.info(f"Generated report: {report_path}")


def main():
    """Fetch instances from CDF views, run extraction & aliasing, write results to tests/results/."""
    parser = argparse.ArgumentParser(
        description="Run key extraction + aliasing on CDF data model instances"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max instances per view (0 = no limit, fetch all). Default 0.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without persisting aliases to CDF (skip alias persistence step)",
    )
    parser.add_argument(
        "--instance-space",
        type=str,
        default=None,
        help="Only process source views with this instance_space (e.g. sp_enterprise_schema)",
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not MODULES_AVAILABLE:
        logger.error("Required modules not available.")
        sys.exit(1)

    _load_env()

    # Create CDF client
    try:
        client = _create_cognite_client()
    except Exception as e:
        logger.error(f"Failed to create CogniteClient: {e}")
        sys.exit(1)

    # Load configs
    try:
        (
            extraction_config,
            aliasing_config,
            source_views,
            alias_writeback_property,
        ) = _load_configs()
    except Exception as e:
        logger.error(f"Failed to load configs: {e}")
        sys.exit(1)

    if args.instance_space:
        source_views = [
            v
            for v in source_views
            if v.get("instance_space", "").strip() == args.instance_space.strip()
        ]
        if not source_views:
            logger.error(
                f"No source views found with instance_space={args.instance_space!r}. "
                "Check pipeline configs for matching instance_space."
            )
            sys.exit(1)
        logger.info(
            f"Filtered to {len(source_views)} view(s) with instance_space={args.instance_space!r}"
        )

    extraction_engine = KeyExtractionEngine(extraction_config)
    aliasing_engine = AliasingEngine(aliasing_config)

    all_extraction_items: List[Dict[str, Any]] = []
    aliasing_items: List[Dict[str, Any]] = []
    # Data structure for persistence function (matches workflow format)
    entities_keys_extracted: Dict[str, Dict[str, Any]] = {}
    aliasing_results: List[Dict[str, Any]] = []

    # Process each source view from config
    for view_config in source_views:
        view_space = view_config.get("view_space", "cdf_cdm")
        view_external_id = view_config.get("view_external_id", "CogniteAsset")
        view_version = view_config.get("view_version", "v1")
        instance_space = view_config.get("instance_space", view_space)
        entity_type = view_config.get("entity_type", "asset")
        batch_size = (
            view_config.get("batch_size") or view_config.get("limit") or args.limit
        )
        # 0 means no limit (fetch all instances)
        effective_limit = batch_size if batch_size > 0 else None
        filters = view_config.get("filters", [])
        include_properties = view_config.get("include_properties", [])

        logger.info(
            f"Processing view {view_space}/{view_external_id}/{view_version} "
            f"(instance_space: {instance_space}, entity_type: {entity_type}, limit: {batch_size if batch_size else 'all'})..."
        )

        # Query data modeling instances
        try:
            from cognite.client import data_modeling as dm
            from cognite.client.data_classes.data_modeling.ids import ViewId

            view_id = ViewId(
                space=view_space, external_id=view_external_id, version=view_version
            )

            # Build filter expression from configuration
            filter_expressions = []

            # Base filter: ensure instance has data from this view
            filter_expressions.append(dm.filters.HasData(views=[view_id]))

            # Add custom filters from configuration
            if filters:
                for filter_config in filters:
                    operator = filter_config.get("operator", "").upper()
                    target_property = filter_config.get("target_property")
                    values = filter_config.get("values", [])

                    if not target_property:
                        continue

                    # Create property reference
                    property_ref = view_id.as_property_ref(target_property)

                    if operator == "EQUALS":
                        # EQUALS with multiple values should be OR-ed
                        if len(values) == 1:
                            filter_expressions.append(
                                dm.filters.Equals(property_ref, values[0])
                            )
                        elif len(values) > 1:
                            # Multiple EQUALS values - OR them together
                            equals_filters = [
                                dm.filters.Equals(property_ref, val) for val in values
                            ]
                            filter_expressions.append(dm.filters.Or(*equals_filters))

                    elif operator == "IN":
                        # IN operator - check if value is in list
                        filter_expressions.append(dm.filters.In(property_ref, values))

                    elif operator == "CONTAINSALL":
                        # CONTAINSALL - check if array property contains all values
                        if values:
                            filter_expressions.append(
                                dm.filters.ContainsAll(
                                    property=property_ref, values=values
                                )
                            )

                    elif operator == "CONTAINSANY":
                        # CONTAINSANY - check if array property contains any of the values
                        if values:
                            filter_expressions.append(
                                dm.filters.ContainsAny(
                                    property=property_ref, values=values
                                )
                            )

                    elif operator == "EXISTS":
                        # EXISTS - check if property exists (not null)
                        filter_expressions.append(
                            dm.filters.HasData(
                                views=[view_id], properties=[target_property]
                            )
                        )

                    elif operator == "SEARCH":
                        # SEARCH - full text search (requires search query string)
                        if values:
                            # Use first value as search query
                            logger.warning(
                                f"SEARCH operator not fully supported, using IN for property {target_property}"
                            )
                            filter_expressions.append(
                                dm.filters.In(property_ref, values)
                            )

            # Combine all filters with AND
            final_filter = (
                dm.filters.And(*filter_expressions)
                if len(filter_expressions) > 1
                else filter_expressions[0]
                if filter_expressions
                else None
            )

            # Query instances using list method (supports filters)
            # Try with filters first, fall back to no filters if filter fails
            instances = None
            if final_filter is not None:
                try:
                    instances = client.data_modeling.instances.list(
                        instance_type="node",
                        space=instance_space,
                        sources=[view_id],
                        filter=final_filter,
                        limit=effective_limit,
                    )
                except Exception as filter_error:
                    logger.warning(
                        f"Filter failed for view {view_external_id}: {filter_error}. "
                        f"Retrying without filters..."
                    )
                    # Fall back to query without filters
                    instances = client.data_modeling.instances.list(
                        instance_type="node",
                        space=instance_space,
                        sources=[view_id],
                        limit=effective_limit,
                    )
            else:
                # No filters configured, query without filters
                instances = client.data_modeling.instances.list(
                    instance_type="node",
                    space=instance_space,
                    sources=[view_id],
                    limit=effective_limit,
                )
        except Exception as e:
            logger.warning(
                f"Failed to fetch instances from view {view_external_id}: {e}"
            )
            continue

        # Convert instances to dict format expected by extraction engine
        instances_dicts: List[Dict[str, Any]] = []
        for instance in instances:
            # Get instance identifier
            instance_external_id = getattr(instance, "external_id", None)
            instance_id = instance_external_id or str(
                getattr(instance, "instance_id", "")
            )

            # Extract properties from CDM structure (same as in pipeline)
            instance_dump = instance.dump()
            entity_props = (
                instance_dump.get("properties", {})
                .get(view_space, {})
                .get(f"{view_external_id}/{view_version}", {})
            )

            # Build entity dict with flattened properties
            # If include_properties is specified, only include those properties
            if include_properties:
                filtered_props = {
                    prop: entity_props.get(prop)
                    for prop in include_properties
                    if prop in entity_props
                }
                entity_dict = {
                    "id": instance_id,
                    "externalId": instance_external_id,
                    **filtered_props,
                }
            else:
                # Include all properties if no filter specified
                entity_dict = {
                    "id": instance_id,
                    "externalId": instance_external_id,
                    **entity_props,  # Spread extracted properties at top level
                }
            instances_dicts.append(entity_dict)

        logger.info(f"  Fetched {len(instances_dicts)} instances")

        # Run extraction for this view
        view_extraction_items: List[Dict[str, Any]] = []
        for entity in instances_dicts:
            res = extraction_engine.extract_keys(entity, entity_type=entity_type)
            entity_id = res.entity_id

            # Build entities_keys_extracted structure for persistence (workflow format)
            keys_by_field = {}
            for key in res.candidate_keys:
                field_name = key.source_field
                if field_name not in keys_by_field:
                    keys_by_field[field_name] = {}
                # Handle both enum and string extraction_type
                extraction_type_value = (
                    key.extraction_type.value
                    if hasattr(key.extraction_type, "value")
                    else key.extraction_type
                )
                keys_by_field[field_name][key.value] = {
                    "confidence": key.confidence,
                    "extraction_type": extraction_type_value,
                }

            entities_keys_extracted[entity_id] = {
                "keys": keys_by_field,
                "view_space": view_space,
                "view_external_id": view_external_id,
                "view_version": view_version,
                "instance_space": instance_space,
                "entity_type": entity_type,
            }

            view_extraction_items.append(
                {
                    "entity": entity,  # Pass entity dict as-is with all properties
                    "view_external_id": view_external_id,
                    "extraction_result": {
                        "entity_id": res.entity_id,
                        "entity_type": res.entity_type,
                        "candidate_keys": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_name": k.rule_name,
                            }
                            for k in res.candidate_keys
                        ],
                        "foreign_key_references": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_name": k.rule_name,
                            }
                            for k in res.foreign_key_references
                        ],
                        "document_references": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_name": k.rule_name,
                            }
                            for k in res.document_references
                        ],
                        "metadata": res.metadata,
                    },
                }
            )

        # Run aliasing for each candidate key from this view
        logger.info(f"  Running aliasing on extracted candidate keys...")
        for item in view_extraction_items:
            entity = item["entity"]
            entity_id = entity.get("id")
            context = {
                "site": entity.get("site"),
                "unit": entity.get("unit"),
                "equipment_type": entity.get("equipmentType")
                or entity.get("equipment_type"),
            }
            for k in item["extraction_result"]["candidate_keys"]:
                tag = k["value"]
                source_field = k.get("source_field")
                aliases_result = aliasing_engine.generate_aliases(
                    tag=tag, entity_type=entity_type, context=context
                )
                # Sort aliases alphabetically (case-insensitive, then case-sensitive)
                sorted_aliases = sorted(
                    aliases_result.aliases, key=lambda x: (x.lower(), x)
                )

                aliasing_items.append(
                    {
                        "entity_id": entity_id,
                        "entity_type": entity_type,
                        "view_external_id": view_external_id,
                        "base_tag": tag,
                        "aliases": sorted_aliases,
                        "metadata": aliases_result.metadata,
                    }
                )

                # Build aliasing_results structure for persistence (workflow format)
                aliasing_results.append(
                    {
                        "original_tag": tag,
                        "aliases": sorted_aliases,
                        "metadata": aliases_result.metadata,
                        "entities": [
                            {
                                "entity_id": entity_id,
                                "field_name": source_field,
                                "view_space": view_space,
                                "view_external_id": view_external_id,
                                "view_version": view_version,
                                "instance_space": instance_space,
                                "entity_type": entity_type,
                            }
                        ],
                    }
                )

        all_extraction_items.extend(view_extraction_items)

    # Write results
    results_dir = _ensure_results_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    extraction_path = results_dir / f"{ts}_cdf_extraction.json"
    aliasing_path = results_dir / f"{ts}_cdf_aliasing.json"

    with extraction_path.open("w", encoding="utf-8") as f:
        json.dump({"results": all_extraction_items}, f, indent=2)

    # Sort aliasing results by entity_id, then base_tag
    sorted_aliasing_items = sorted(
        aliasing_items, key=lambda x: (x.get("entity_id", ""), x.get("base_tag", ""))
    )

    with aliasing_path.open("w", encoding="utf-8") as f:
        json.dump({"results": sorted_aliasing_items}, f, indent=2)

    logger.info(f"✓ Wrote extraction results: {extraction_path}")
    logger.info(f"✓ Wrote aliasing results:   {aliasing_path}")

    # Persist aliases to CogniteDescribable view (unless dry-run)
    if args.dry_run:
        logger.info(
            "Dry-run mode: Skipping alias persistence to CDF. "
            f"Would persist {len(aliasing_results)} aliasing results to {len(entities_keys_extracted)} entities"
        )
    else:
        logger.info("Persisting aliases to CogniteDescribable view...")
        try:
            persistence_data = {
                "aliasing_results": aliasing_results,
                "entities_keys_extracted": entities_keys_extracted,
                "logLevel": "INFO",
            }
            if alias_writeback_property:
                persistence_data["alias_writeback_property"] = alias_writeback_property
            persist_aliases_to_entities(
                client=client,
                logger=logger,
                data=persistence_data,
            )
            logger.info(
                f"✓ Persisted aliases: {persistence_data.get('entities_updated', 0)} entities updated, "
                f"{persistence_data.get('aliases_persisted', 0)} aliases persisted"
            )
        except Exception as e:
            logger.error(f"Failed to persist aliases: {e}", exc_info=True)


if __name__ == "__main__":
    main()
