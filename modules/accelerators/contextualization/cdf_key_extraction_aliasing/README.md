# Key Extraction and Aliasing Module

A comprehensive Python library for extracting candidate keys and foreign key references from entity metadata using multiple extraction methods, and generating aliases for improved entity matching. This standalone system works with Cognite Data Fusion (CDF) and can run independently.

## 📊 Results Report

Latest run results: [Key Extraction and Aliasing Report](docs/key_extraction_aliasing_report.md).

## 🎯 Overview

The module provides:

- **5 Extraction Methods**: Passthrough (default when `method` is omitted), Regex, Fixed Width, Token Reassembly, and Heuristic
- **3 Extraction Types**: Candidate keys, Foreign key references, Document references
- **12 Transformation Types**: Character substitution, prefix/suffix, regex, case, equipment expansion, related instruments, hierarchical expansion, document aliases, leading zero normalization, pattern recognition, pattern-based expansion, composite
- **CDF Integration**: Data model views, functions, and workflows
- **Configuration**: YAML-based pipeline configs with validation
- **Testing**: Comprehensive test suite (unit and integration)

## Roadmap

- [ ] Implement the state store for target entities into CDM and avoid RAW
- [ ] Get functions/pipelines deployable
- [ ] Write foreign_key/document references to reference catalog (RAW or DM); candidate-key alias write-back to CogniteDescribable is implemented (`fn_dm_alias_persistence`)
- [ ] Refine default rules for more targeted configs per entity_type
- [ ] Test against non-ISA standard tags
- [ ] Reverse lookup against reference catalog for missing tags and create relationships/annotations

---

## 🚀 Quick Start

### Installation

From the repository root:

```bash
git clone <repository-url>
cd library
poetry install
```

### Run the workflow

`main.py` fetches entities from CDF data model views (from pipeline config), extracts candidate keys and references, generates aliases, and persists them to the `CogniteDescribable` view (`cdf_cdm` / `v1`) unless `--dry-run` is used. The data model property that receives the alias list defaults to `aliases` and is configurable (see **Alias write-back** below).

From the repository root:

```bash
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --limit 50 --verbose
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --dry-run
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --instance-space sp_enterprise_schema
```

| Option | Description | Default |
|--------|-------------|---------|
| `--limit` | Max instances per view; 0 = no limit (fetch all) | 0 |
| `--verbose` | Verbose logging | False |
| `--dry-run` | Skip alias persistence to CDF | False |
| `--instance-space` | Only process source views with this instance space | All views |

Outputs (timestamped) in `tests/results/`: `YYYYMMDD_HHMMSS_cdf_extraction.json`, `YYYYMMDD_HHMMSS_cdf_aliasing.json`. When not using `--dry-run`, the alias persistence pipeline writes the generated alias list to each instance via the CogniteDescribable view, using the property name from configuration (default `aliases`).

Generate report from latest results (writes `docs/key_extraction_aliasing_report.md`):

```bash
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/scripts/generate_report.py
```

**Prerequisites:** `.env` (or env vars) for CDF (`CDF_PROJECT`, `CDF_CLUSTER`, `IDP_*` or API key), pipeline configs present, source views, and instances readable/updatable through CogniteDescribable for alias persistence.

### Alias write-back

The alias persistence step applies the generated alias list to a property on **CogniteDescribable** (`cdf_cdm` / `v1`). Configure the property name as follows:

| Source | Field | Notes |
|--------|--------|--------|
| CDF workflow | `aliasWritebackProperty` (or `alias_writeback_property`) in the `fn_dm_alias_persistence` task `data` | Overrides YAML when set |
| `main.py` / pipeline | `alias_writeback_property` under `config.parameters` in `*aliasing*.config.yaml` (first file with the key wins) | e.g. `ctx_aliasing_default.config.yaml` |
| Default | `aliases` | Used when nothing is configured |

See `docs/guides/configuration_guide.md` (Aliasing Pipeline `parameters`).

### Basic usage (Python)

```python
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import KeyExtractionEngine
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import AliasingEngine

extraction_config = {
    "extraction_rules": [{
        "name": "pump_tags", "method": "regex", "extraction_type": "candidate_key",
        "priority": 50, "enabled": True,
        "source_fields": [{"field_name": "name", "required": True}],
        "config": {"pattern": "P[-_]?\\d{1,6}[A-Z]?"}
    }],
    "validation": {"min_confidence": 0.5, "max_keys_per_type": 10}
}
extraction_engine = KeyExtractionEngine(extraction_config)
aliasing_engine = AliasingEngine({"rules": [], "validation": {}})

# For `alias_mapping_table` rules that load a catalog from Cognite RAW, pass a Cognite client:
# AliasingEngine(aliasing_config, client=cognite_client). See configuration_guide.md (Alias mapping table RAW catalog).

sample_data = {"id": "001", "name": "P-10001", "description": "Feed pump"}
result = extraction_engine.extract_keys(sample_data, "asset")
for key in result.candidate_keys:
    alias_result = aliasing_engine.generate_aliases(key.value, "asset")
    print(f"{key.value}: {alias_result.aliases}")
```

---

## 📁 Module structure

Paths below are relative to this module directory (`modules/accelerators/contextualization/cdf_key_extraction_aliasing/`).

```
cdf_key_extraction_aliasing/
├── main.py                                    # Entry point: CDF fetch → extract → alias → persist
├── config/                                     # Configuration utilities
├── functions/
│   ├── fn_dm_key_extraction/                   # Key extraction CDF Function + engine
│   │   ├── engine/key_extraction_engine.py
│   │   ├── engine/handlers/                    # Regex, FixedWidth, TokenReassembly, Heuristic
│   │   ├── cdf_adapter.py
│   │   └── utils/                              # DataStructures, method params
│   ├── fn_dm_aliasing/                         # Aliasing CDF Function + engine
│   │   ├── engine/tag_aliasing_engine.py
│   │   ├── tag_pattern_library.py
│   │   └── transformer_utils.py
│   └── fn_dm_alias_persistence/                # Write alias list to a CogniteDescribable property (default aliases)
│       └── pipeline.py
├── pipelines/                                  # ctx_key_extraction_default, ctx_aliasing_default
├── data_sets/
├── workflows/
├── scripts/
│   └── generate_report.py                      # Generate report from latest results
├── tests/                                      # Unit and integration tests; tests/results/ for JSON
└── docs/                                       # Specifications, guides, key_extraction_aliasing_report.md
```

### Key paths

| Path | Purpose |
|------|---------|
| `pipelines/` | Pipeline YAML configs (key extraction + aliasing) |
| `functions/fn_dm_key_extraction/` | Extraction engine + CDF function |
| `functions/fn_dm_aliasing/` | Aliasing engine + CDF function |
| `functions/fn_dm_alias_persistence/` | Persist alias list to CogniteDescribable (property name configurable; used by main.py) |
| `tests/` | Unit and integration tests; `tests/results/` for JSON outputs |
| `scripts/generate_report.py` | Generate markdown report from latest results |
| `docs/` | Specifications and guides |

---

## 🏗️ Architecture

### Core components

- **KeyExtractionEngine** (`functions/fn_dm_key_extraction/engine/key_extraction_engine.py`): Runs extraction rules via handlers (passthrough default, regex, fixed width, token reassembly, heuristic), applies validation and confidence filtering, returns `ExtractionResult`.
- **AliasingEngine** (`functions/fn_dm_aliasing/engine/tag_aliasing_engine.py`): Applies transformation rules, uses pattern library, returns `AliasingResult`.
- **Alias persistence** (`functions/fn_dm_alias_persistence/pipeline.py`): Writes the generated alias list to a configurable property on CogniteDescribable (default `aliases`; see **Alias write-back** above). Used by `main.py` unless `--dry-run`.
- **CDF Functions**: `fn_dm_key_extraction`, `fn_dm_aliasing`, `fn_dm_alias_persistence` (handlers + pipelines).
- **Pipeline configs**: `ctx_key_extraction_default`, `ctx_aliasing_default`.

### Extraction methods

| Method | Description | Use case |
|--------|-------------|----------|
| **Passthrough** | Use whole field value as key | Default when `method` is omitted; value is already the identifier |
| **Regex** | Pattern-based extraction | Structured tags (e.g. `P-101`, `FCV-2001`) |
| **Fixed Width** | Column-based extraction | Tabular / fixed-format text |
| **Token Reassembly** | Assemble from components | Hierarchical tags (site-unit-equipment) |
| **Heuristic** | Multiple strategies | Inconsistent or unstructured data |

### Transformation types (aliasing)

| Type | Example |
|------|---------|
| Character Substitution | `P-101` → `P_101`, `P101` |
| Prefix/Suffix | `P-101` → `PA-P-101` |
| Regex Substitution | `P101A` → `P-101A` |
| Case Transformation | `p-101` → `P-101` |
| Equipment Type Expansion | `P-101` → `PUMP-P-101` |
| Related Instruments | `P-101` → `FIC-101`, `PI-101` |
| Hierarchical Expansion | `P-101` → `U100-P-101` |
| Document Aliases | `P&ID-2001` → `PID-2001` |
| Leading Zero Normalization | `P-001` ↔ `P-1` |
| Pattern Recognition / Pattern-Based Expansion | ISA/ANSI patterns |
| Composite | Chain multiple rules |

---

## 🔧 Configuration

Pipeline YAMLs live in `pipelines/`:

- **Key extraction**: `ctx_key_extraction_default.config.yaml` — `source_views`, `extraction_rules`, `validation`. Omit `method` on a rule to use **passthrough** (whole field value as the key).
- **Aliasing**: `ctx_aliasing_default.config.yaml` — `aliasing_rules`, `validation`

**Source view filters** (`source_views[].filters` in the key extraction pipeline) limit which instances are queried per view. Supported operators: `EQUALS`, `IN`, `CONTAINSALL`, `CONTAINSANY`, `EXISTS`, `SEARCH`. Example: `CONTAINSANY` on `tags` with values `[asset_tag]` to process only instances whose `tags` array contains `asset_tag`. Per-view `batch_size` (or `limit`) overrides the `--limit` CLI for that view when set to a positive value; when `--limit` is 0 and the view omits `batch_size`/`limit`, all instances are fetched.

### Validation

- **Extraction**: `min_confidence`, `max_keys_per_type`, `blacklist_keywords`
- **Aliasing**: `max_aliases_per_tag`, `min_alias_length`, `max_alias_length`, `allowed_characters`

### Field selection

- **`first_match`**: Use first field (by priority) that yields results
- **`merge_all`**: Process all fields and merge, keeping highest confidence for duplicates

### Loading config

- **From YAML**: `load_config_from_yaml("path/to/ctx_key_extraction_default.config.yaml")`
- **From CDF**: `load_config_from_cdf(client, "ctx_key_extraction_default")`

See the pipeline config files and the docs (e.g. `docs/1. key_extraction.md`, `docs/guides/configuration_guide.md`) for full structure and recent updates (e.g. timeseries extraction).

---

## 🔌 CDF integration

### As CDF Functions

- **fn_dm_key_extraction**: `handle(data, client)` — input e.g. `ExtractionPipelineExtId`, `logLevel`; returns keys and `entities_keys_extracted`.
- **fn_dm_aliasing**: `handle(data, client)` — uses aliasing pipeline and (optionally) key extraction results to produce `aliasing_results`.
- **fn_dm_alias_persistence**: `handle(data, client)` — reads `aliasing_results` and writes the alias list back via CogniteDescribable (configurable property; see **Alias write-back**).

### As CDF Workflow

Workflow **`cdf_key_extraction_aliasing`** runs key extraction → aliasing → alias persistence (`fn_dm_key_extraction` → `fn_dm_aliasing` → `fn_dm_alias_persistence`). The aliasing task consumes `entities_keys_extracted`; persistence consumes `aliasing_results` and `entities_keys_extracted`.

```bash
cdf-tk deploy workflows
cdf-tk deploy functions
cdf-tk deploy pipelines
cdf-tk deploy data_sets
```

---

## 📦 Extraction types

- **Candidate keys**: Primary identifiers (e.g. equipment tags)
- **Foreign key references**: References to other entities in text (e.g. "Refer to pump P-102")
- **Document references**: References to documents/drawings/specs

---

## 🧪 Testing

From repository root:

```bash
poetry run pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/ -v
poetry run pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/ --cov=modules --cov-report=html
```

Generate detailed results (no test run):

```bash
poetry run python modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/generate_detailed_results.py
```

---

## 🛠️ Troubleshooting

- **No keys extracted**: Check source views, rule patterns, and `min_confidence`
- **No aliases**: Check aliasing rules are enabled and tag format; check `max_aliases_per_tag`
- **Workflow fails**: Ensure functions and pipelines are deployed; check function logs
- **Import errors**: Repo root on `PYTHONPATH`; Python 3.11+; dependencies installed

Enable debug in pipeline config: `parameters.debug: true`, `logLevel: "DEBUG"`.

---

## 📚 Documentation

- **Key extraction**: `docs/1. key_extraction.md`
- **Aliasing**: `docs/2. aliasing.md`
- **Configuration**: `docs/guides/configuration_guide.md`
- **Quick start**: `docs/guides/quick_start.md`
- **Troubleshooting**: `docs/troubleshooting/common_issues.md`
- **Workflows**: `workflows/README.md`

---

## 💡 Example use cases

- Extract equipment tags from descriptions
- Generate aliases for cross-system tag matching
- Normalize naming conventions
- Build tag hierarchies
- Improve search via variant names

---

## 🤝 Contributing

1. Run tests: `poetry run pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/`
2. Follow project style; add tests for new features; update docs as needed.

---

## 📄 License

[Add your license information here]

---

## 👥 Authors

- Darren Downtain

---

## 🙏 Acknowledgments

Built for industrial asset tagging and data integration with Cognite Data Fusion.
