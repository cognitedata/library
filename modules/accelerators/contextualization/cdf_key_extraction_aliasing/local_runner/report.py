"""Markdown report generation for local CLI results."""
import json
import logging
from datetime import datetime
from pathlib import Path

from .paths import SCRIPT_DIR

def ensure_results_dir() -> Path:
    """Ensure results directory exists."""
    results_dir = SCRIPT_DIR / "tests" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


def generate_report(
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
            rules[key.get("rule_id", "unknown")] += 1
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
    report_content = f"""# Key Discovery and Aliasing Results Summary

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Results Source:** {results_source}

## Executive Summary

This document provides a comprehensive analysis of the key discovery and aliasing pipeline execution results, covering processing statistics, entity type breakdowns, extraction methods, and aliasing transformations.

### Workflow Overview

Diagram (Mermaid): [workflow_template/workflow_diagram.md](../workflow_template/workflow_diagram.md).

**Workflow Steps:**
1. **Key Extraction** - Extracts candidate keys, foreign key references, and document references from CDF entities
2. **Result Splitting** - Separates extraction results into distinct streams based on type
3. **Aliasing** - Generates aliases for candidate keys to improve matching
4. **Write Aliases** - Persists the alias list to a property on CogniteDescribable (default `aliases`; configurable via pipeline or workflow `data`)
5. **Reference Index** - `fn_dm_reference_index` builds a RAW inverted index from FK and document-reference JSON written by key extraction (optional DM projection remains on the module roadmap)

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

#### 2. Passthrough Extraction

**What it does:** Uses the full processed field value as the key without pattern parsing (when `method` is `passthrough` or omitted).

**Best for:** Identifiers already stored whole in a single field (`name`, `externalId`, pre-normalized codes).


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
