# Technical Specification: Create Asset Hierarchy from Files

**Document Version:** 1.0
**Date:** November 2024
**Author:** Technical Architecture Team
**Status:** Approved
**Based on:** Functional Specification v1.0

---

## Table of Contents

1. [Introduction](#introduction)
2. [System Architecture](#system-architecture)
3. [Component Design](#component-design)
4. [Data Models and Schemas](#data-models-and-schemas)
5. [API Interfaces](#api-interfaces)
6. [Algorithm Specifications](#algorithm-specifications)
7. [Configuration System](#configuration-system)
8. [Error Handling and Recovery](#error-handling-and-recovery)
9. [Performance and Scalability](#performance-and-scalability)
10. [Security and Authentication](#security-and-authentication)
11. [Deployment Architecture](#deployment-architecture)
12. [Testing Strategy](#testing-strategy)
13. [Appendices](#appendices)

---

## Introduction

### Purpose

This technical specification document provides detailed technical design and implementation details for the Create Asset Hierarchy from Files solution. It serves as a reference for developers, system architects, and technical stakeholders implementing, maintaining, or extending the system.

### Scope

This specification covers:

- System architecture and component design
- Data models and schemas
- API interfaces and function signatures
- Algorithm specifications
- Configuration system design
- Error handling mechanisms
- Performance optimization strategies
- Security and authentication
- Deployment architecture

### Document Relationship

This technical specification is based on and complements the Functional Specification. While the functional specification describes *what* the system does, this document describes *how* it is implemented.

### Technology Stack

- **Language**: Python 3.12+
- **Framework**: Cognite SDK (cognite-sdk >= 7.83.0)
- **Platform**: Cognite Data Fusion (CDF)
- **Functions**: CDF Functions runtime
- **Workflows**: CDF Workflows service
- **Storage**: CDF RAW database, CDF Data Modeling
- **Configuration**: YAML-based configuration files
- **Dependencies**: PyYAML, python-dotenv, cognite-toolkit

---

## System Architecture

### High-Level Architecture

The system follows a three-tier pipeline architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    CDF Workflow Layer                        │
│         (create_asset_hierarchy_from_files)                  │
│                  - Task Orchestration                        │
│                  - Dependency Management                      │
│                  - Error Handling & Retries                   │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
│   Function   │  │   Function       │  │   Function   │
│   Layer      │  │   Layer          │  │   Layer      │
│              │  │                  │  │              │
│  Extract     │  │  Create          │  │  Write       │
│  Assets      │─▶│  Hierarchy       │─▶│  Assets      │
│              │  │                  │  │              │
└──────────────┘  └──────────────────┘  └──────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
│   Pipeline   │  │   Pipeline       │  │   Pipeline   │
│   Layer      │  │   Layer           │  │   Layer      │
│              │  │                  │  │              │
│  Pattern     │  │  Hierarchy       │  │  Batch        │
│  Matching    │  │  Generation      │  │  Writing      │
└──────────────┘  └──────────────────┘  └──────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
│   Storage    │  │   Storage         │  │   Storage    │
│   Layer      │  │   Layer           │  │   Layer      │
│              │  │                  │  │              │
│  RAW Tables  │  │  RAW Tables       │  │  CDF Data    │
│  (Results)   │  │  (Assets)         │  │  Modeling    │
└──────────────┘  └──────────────────┘  └──────────────┘
```

### Component Architecture

#### Function 1: Extract Assets by Pattern

**Component ID**: `fn_dm_extract_assets_by_pattern`
**Type**: CDF Function
**Runtime**: Python 3.12+
**Dependencies**: cognite-sdk, PyYAML

**Responsibilities**:
- Query CDF files based on filters
- Process files using diagram detection API
- Extract asset tags using pattern matching
- Store results in RAW tables
- Track processing state

**Key Modules**:
- `handler.py`: CDF function handler interface
- `pipeline.py`: Main extraction pipeline logic
- `config.py`: Configuration loading and parsing
- `dependencies.py`: Client and logger creation
- `utils/file_utils.py`: File handling utilities
- `logger.py`: Logging service

#### Function 2: Create Asset Hierarchy

**Component ID**: `fn_dm_create_asset_hierarchy`
**Type**: CDF Function
**Runtime**: Python 3.12+
**Dependencies**: cognite-sdk, PyYAML, asset_tag_classifier

**Responsibilities**:
- Load extracted assets from RAW tables
- Match files to systems based on location configuration
- Generate hierarchical asset structure
- Classify assets using pattern matching (optional)
- Store generated hierarchy in RAW tables

**Key Modules**:
- `handler.py`: CDF function handler interface
- `pipeline.py`: Main hierarchy creation logic
- `config.py`: Configuration loading and parsing
- `dependencies.py`: Client and logger creation
- `utils/hierarchy_utils.py`: Hierarchy generation algorithms
- `utils/location_utils.py`: Location matching utilities
- `logger.py`: Logging service

#### Function 3: Write Asset Hierarchy

**Component ID**: `fn_dm_write_asset_hierarchy`
**Type**: CDF Function
**Runtime**: Python 3.12+
**Dependencies**: cognite-sdk, PyYAML

**Responsibilities**:
- Load asset hierarchy from RAW tables
- Convert assets to CogniteAsset format
- Write assets to CDF data modeling in batches
- Handle updates to existing assets
- Report write operation results

**Key Modules**:
- `handler.py`: CDF function handler interface
- `pipeline.py`: Main writing pipeline logic
- `config.py`: Configuration loading and parsing
- `dependencies.py`: Client and logger creation
- `utils/asset_utils.py`: Asset conversion utilities
- `logger.py`: Logging service

### Shared Components

#### Asset Tag Classifier

**Location**: `modules/create_asset_hierarchy_from_files/asset_tag_classifier.py`

**Purpose**: Classifies asset tags based on pattern matching against industry-standard patterns.

**Key Features**:
- Pattern compilation and matching
- Validation rules support
- ISA 5.1 classification mappings
- Document pattern classification
- File I/O operations (JSON/YAML)

**API**:
```python
class AssetTagClassifier:
    def __init__(config_path: Union[str, Path], document_patterns_path: Optional[Union[str, Path]] = None)
    def classify_tag(tag: str, validate: bool = True) -> Optional[Dict[str, Any]]
    def classify_assets(assets: Union[List[Dict], Dict], tag_field: str = "externalId", skip_classified: bool = False) -> Union[List[Dict], Dict]
    def load_assets(assets_path: Union[str, Path]) -> Union[List[Dict], Dict]
    def save_assets(assets: Union[List[Dict], Dict], output_path: Union[str, Path], format: str = "yaml") -> None
```

#### Shared Utilities

**Location**: `modules/create_asset_hierarchy_from_files/functions/shared/utils/`

**Components**:
- `hierarchy_utils.py`: Shared hierarchy generation utilities
- `config_validator.py`: Configuration validation utilities

**Key Functions**:
```python
def create_asset_instance(
    external_id: str,
    name: str,
    description: Optional[str] = None,
    parent_external_id: Optional[str] = None,
    space: str = "sp_enterprise_schema",
    level: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]
```

#### Common Utilities

**Location**: `modules/create_asset_hierarchy_from_files/common.py`

**Purpose**: Common utilities for Cognite client setup and file operations.

**Key Functions**:
```python
def setup_cognite_client(client_name: str = "CDF-Script") -> CogniteClient
def extract_file_id_from_node(node, property_names: Optional[List[str]] = None) -> Optional[int]
def extract_uploaded_time_from_node(cognite_file_node) -> Optional[str]
```

---

## Component Design

### Handler Pattern

All functions follow a consistent handler pattern:

```python
def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of extraction pipeline
            - [function-specific parameters]
        client: CogniteClient instance

    Returns:
        Dictionary with status and result information:
            - status: "succeeded" | "failure"
            - data: Updated data dictionary (on success)
            - message: Error message (on failure)
    """
```

**Handler Responsibilities**:
1. Initialize logging service
2. Load configuration from CDF extraction pipeline
3. Merge configuration parameters into data dictionary
4. Call pipeline function
5. Return status and results

### Pipeline Pattern

Each function has a main pipeline function:

```python
def [function_name](
    client: Optional[CogniteClient],
    logger: Optional[CogniteFunctionLogger],
    data: Dict[str, Any]
) -> None:
    """
    Main pipeline function.

    Args:
        client: CogniteClient instance
        logger: Logger instance
        data: Data dictionary with configuration and parameters
    """
```

**Pipeline Responsibilities**:
1. Validate inputs and configuration
2. Load data from sources (CDF, RAW tables, files)
3. Process data according to business logic
4. Store results in targets (RAW tables, CDF)
5. Log progress and errors

### Configuration Loading

Configuration is loaded from CDF extraction pipelines:

```python
def load_config_parameters(
    client: CogniteClient,
    data: Dict[str, Any]
) -> Config:
    """
    Load configuration from CDF extraction pipeline.

    Args:
        client: CogniteClient instance
        data: Data dictionary containing ExtractionPipelineExtId

    Returns:
        Config object with parameters and data sections
    """
```

**Configuration Structure**:
```python
class Config:
    parameters: Dict[str, Any]  # Technical settings
    data: Dict[str, Any]         # Business configuration
```

---

## Data Models and Schemas

### RAW Table Schemas

#### State Table Schema

**Table**: `extract_assets_by_pattern_state`
**Database**: `db_extract_assets_by_pattern`

**Schema**:
```json
{
  "key": "file_id (integer, primary key)",
  "state": {
    "file_info": {
      "id": "integer",
      "name": "string",
      "mime_type": "string",
      "uploaded_time": "string (ISO datetime)",
      "page_count": "integer"
    },
    "status": "string (pending|processing|completed|failed)",
    "attempts": "integer",
    "error": "string (optional)",
    "updated_at": "string (ISO datetime)",
    "results": {
      "items": [
        {
          "annotations": [
            {
              "text": "string",
              "confidence": "float",
              "entities": [
                {
                  "sample": "string",
                  "resourceType": "string (optional)",
                  "resourceSubType": "string (optional)",
                  "standard": "string (optional)"
                }
              ]
            }
          ]
        }
      ]
    }
  },
  "results": "JSON string (duplicate of state.results for direct access)"
}
```

#### Results Table Schema

**Table**: `extract_assets_by_pattern_results` (optional, if separate from state table)
**Database**: `db_extract_assets_by_pattern`

**Schema**: Same as state table `results` field structure.

#### Assets Table Schema

**Table**: `extract_assets_by_pattern_assets`
**Database**: `db_extract_assets_by_pattern`

**Schema**:
```json
{
  "key": "external_id (string, primary key)",
  "asset": {
    "externalId": "string",
    "space": "string",
    "properties": {
      "name": "string",
      "description": "string (optional)",
      "parent": {
        "space": "string",
        "externalId": "string"
      },
      "tags": ["string"],
      "resourceType": "string (optional)",
      "resourceSubType": "string (optional)",
      "standard": "string (optional)"
    }
  }
}
```

### CDF Data Model Schema

#### CogniteAsset View

**View ID**: `cdf_cdm.CogniteAsset.v1` (default, configurable)

**Properties**:
- `name`: String (required)
- `description`: String (optional)
- `parent`: NodeReference (optional)
- `tags`: List[String] (optional)
- `resourceType`: String (optional)
- `resourceSubType`: String (optional)
- `standard`: String (optional)

**Instance Format**:
```python
{
    "externalId": "string",
    "space": "string",
    "properties": {
        "name": "string",
        "description": "string (optional)",
        "parent": {
            "space": "string",
            "externalId": "string"
        },
        "tags": ["string"],
        "resourceType": "string (optional)",
        "resourceSubType": "string (optional)",
        "standard": "string (optional)"
    }
}
```

### Internal Data Structures

#### Location Structure

```python
{
    "site_code": "string",
    "plant_code": "string",
    "area_code": "string",
    "system_code": "string",
    "site_name": "string",
    "plant_name": "string",
    "area_name": "string",
    "system_name": "string",
    "site_description": "string (optional)",
    "plant_description": "string (optional)",
    "area_description": "string (optional)",
    "system_description": "string (optional)",
    "files": ["string"]  # File names assigned to this system
}
```

#### Tag Structure

```python
{
    "file_id": "integer",
    "file_name": "string",
    "text": "string",  # Extracted tag text
    "confidence": "float",
    "resourceType": "string (optional)",
    "resourceSubType": "string (optional)",
    "standard": "string (optional)",
    "matched_pattern": "string (optional)"
}
```

#### Asset Instance Structure

```python
{
    "externalId": "string",
    "space": "string",
    "properties": {
        "name": "string",
        "description": "string (optional)",
        "parent": {
            "space": "string",
            "externalId": "string"
        },
        "tags": ["string"],
        "resourceType": "string (optional)",
        "resourceSubType": "string (optional)",
        "standard": "string (optional)"
    }
}
```

---

## API Interfaces

### Function Handler APIs

#### Extract Assets Handler

```python
def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]
```

**Input Parameters** (`data` dictionary):
- `ExtractionPipelineExtId` (str, optional): Pipeline external ID
- `patterns` (List[Dict], optional): Pattern definitions
- `limit` (int, optional): File processing limit (-1 for all)
- `mime_type` (str, optional): MIME type filter
- `instance_space` (str, optional): Data model space filter
- `partial_match` (bool, default: True): Enable partial matching
- `min_tokens` (int, default: 2): Minimum tokens required
- `batch_size` (int, default: 5): Files per batch
- `max_attempts` (int, default: 3): Retry attempts
- `max_pages_per_chunk` (int, default: 50): Pages per chunk
- `diagram_detect_config` (Dict, optional): Diagram detection config
- `logLevel` (str, default: "INFO"): Log level

**Output**:
```python
{
    "status": "succeeded" | "failure",
    "data": Dict[str, Any],  # Updated data (on success)
    "message": str  # Error message (on failure)
}
```

#### Create Hierarchy Handler

```python
def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]
```

**Input Parameters** (`data` dictionary):
- `ExtractionPipelineExtId` (str, optional): Pipeline external ID
- `locations` (List[Dict], optional): Location hierarchy
- `hierarchy_levels` (List[str], optional): Hierarchy level names
- `space` (str, default: "sp_enterprise_schema"): Instance space
- `include_resource_type` (bool, default: False): Include resource type level
- `include_resource_subtype` (bool, default: False): Include resource subtype level
- `pattern_config_path` (str, optional): Path to pattern config
- `output_file` (str, optional): Output file path (local mode)
- `logLevel` (str, default: "INFO"): Log level

**Output**: Same as Extract Assets Handler

#### Write Assets Handler

```python
def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]
```

**Input Parameters** (`data` dictionary):
- `ExtractionPipelineExtId` (str, optional): Pipeline external ID
- `raw_db` (str, optional): RAW database name
- `raw_table_assets` (str, optional): RAW assets table name
- `hierarchy_file` (str, optional): Hierarchy file path (fallback)
- `batch_size` (int, default: 100): Assets per batch
- `dry_run` (bool, default: False): Dry run mode
- `view_space` (str, default: "cdf_cdm"): View space
- `view_external_id` (str, default: "CogniteAsset"): View external ID
- `view_version` (str, default: "v1"): View version
- `logLevel` (str, default: "INFO"): Log level

**Output**: Same as Extract Assets Handler

### Pipeline APIs

#### Extract Assets Pipeline

```python
def extract_assets_by_pattern(
    client: Optional[CogniteClient],
    logger: Optional[CogniteFunctionLogger],
    data: Dict[str, Any]
) -> None
```

**Key Operations**:
1. Query CDF files based on filters
2. Process files in batches
3. Extract tags using diagram detection
4. Match patterns against extracted text
5. Store results in RAW tables
6. Update processing state

#### Create Hierarchy Pipeline

```python
def create_asset_hierarchy(
    client: Optional[CogniteClient],
    logger: Optional[CogniteFunctionLogger],
    data: Dict[str, Any]
) -> None
```

**Key Operations**:
1. Load extracted assets from RAW tables
2. Load location configuration
3. Match files to systems
4. Generate hierarchy structure
5. Classify assets (optional)
6. Store hierarchy in RAW tables

#### Write Assets Pipeline

```python
def write_asset_hierarchy(
    client: CogniteClient,
    logger: Optional[CogniteFunctionLogger],
    data: Dict[str, Any]
) -> None
```

**Key Operations**:
1. Load asset hierarchy from RAW tables
2. Convert to CogniteAsset format
3. Write to CDF in batches
4. Handle updates and errors
5. Report results

### Utility APIs

#### File Utilities

```python
def get_cognite_files(
    client: CogniteClient,
    limit: int = -1,
    mime_type: Optional[str] = None,
    instance_space: Optional[str] = None
) -> List[File]
```

```python
def chunk_file_into_page_blocks(
    file_id: int,
    page_count: int,
    max_pages_per_chunk: int = 50
) -> List[Tuple[int, int]]
```

#### Hierarchy Utilities

```python
def generate_hierarchy(
    locations: List[Dict[str, str]],
    tags: List[Dict[str, Any]],
    space: str = "sp_enterprise_schema",
    include_resource_subtype: bool = False,
    include_resource_type: bool = False,
    hierarchy_levels: Optional[List[str]] = None
) -> List[Dict[str, Any]]
```

```python
def create_asset_instance(
    external_id: str,
    name: str,
    description: Optional[str] = None,
    parent_external_id: Optional[str] = None,
    space: str = "sp_enterprise_schema",
    level: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]
```

#### Location Utilities

```python
def match_file_to_system(
    file_name: str,
    locations: List[Dict[str, str]]
) -> Optional[Dict[str, str]]
```

```python
def convert_locations_dict_to_flat_list(
    locations_dict: List[Dict],
    hierarchy_levels: List[str]
) -> List[Dict[str, str]]
```

---

## Algorithm Specifications

### Pattern Matching Algorithm

**Purpose**: Match asset tags in diagram text against configured patterns.

**Input**:
- Text content from diagram files
- Pattern definitions (list of pattern dictionaries)

**Algorithm**:
1. **Pattern Compilation**:
   - Convert pattern strings to regex patterns
   - Handle special syntax: `[X]` → any letter, `X` → any letters, numbers → digits
   - Compile regex patterns for efficient matching
   - Sort patterns by priority (lower number = higher priority)

2. **Text Extraction**:
   - Extract text from diagram using diagram detection API
   - Extract annotations (if available)
   - Extract embedded text (if available)

3. **Pattern Matching**:
   - For each pattern (in priority order):
     - Search text for pattern matches
     - If match found:
       - Extract matched text
       - Calculate confidence score
       - Apply validation rules (if enabled)
       - If validation passes, return match
   - If no match found, continue to next pattern

4. **Result Aggregation**:
   - Collect all matches
   - Remove duplicates
   - Sort by confidence (descending)
   - Return top matches

**Output**: List of matched asset tags with metadata

**Time Complexity**: O(n × m × p) where n = text length, m = number of patterns, p = average pattern length

**Space Complexity**: O(n + m) for text storage and pattern compilation

### Hierarchy Generation Algorithm

**Purpose**: Generate hierarchical asset structure from locations and extracted tags.

**Input**:
- Location hierarchy (nested structure)
- Extracted tags (list of tag dictionaries)
- Hierarchy options (include_resource_type, include_resource_subtype)

**Algorithm**:
1. **Location Flattening**:
   - Convert nested location structure to flat list
   - Extract codes and names for each hierarchy level
   - Build location key tuples for matching

2. **File-to-System Matching**:
   - For each tag:
     - Extract file name
     - Match file name to system in location hierarchy
     - Store file-to-location mapping

3. **Tag Grouping**:
   - Group tags by location key (all hierarchy levels)
   - Group tags by file name within each location

4. **Hierarchy Generation**:
   - For each unique location:
     - Create hierarchy levels (Site → Plant → Area → System)
     - Generate external IDs for each level
     - Create parent-child relationships
     - Store level external IDs for tag processing

5. **Intermediate Level Creation** (if enabled):
   - If `include_resource_type`: Create resource type level
   - If `include_resource_subtype`: Create resource subtype level
   - Group tags by resource type/subtype

6. **Asset Tag Creation**:
   - For each tag:
     - Determine parent (system or intermediate level)
     - Generate external ID for asset tag
     - Create asset instance with metadata
     - Add to assets list

7. **Deduplication**:
   - Track created external IDs
   - Skip duplicate assets

**Output**: List of asset instances with hierarchical relationships

**Time Complexity**: O(l × t) where l = number of locations, t = number of tags

**Space Complexity**: O(l + t) for location and tag storage

### External ID Generation Algorithm

**Purpose**: Generate unique external IDs for hierarchy levels and assets.

**Algorithm**:
1. **Level External ID**:
   - Concatenate codes from all hierarchy levels: `{level1}_{level2}_{level3}_{level4}`
   - Prefix with level name: `{level}_{codes}`
   - Example: `site_DEMO_SITE`, `plant_DEMO_SITE_PLANT_A`

2. **Asset Tag External ID**:
   - Start with level external ID
   - Add resource type (if enabled): `{level_external_id}_{resourceType}`
   - Add resource subtype (if enabled): `{level_external_id}_{resourceSubType}`
   - Add tag text: `{level_external_id}_{tag_text}`
   - Prefix with asset type: `asset_tag_{...}`
   - Example: `asset_tag_site_DEMO_SITE_plant_PLANT_A_area_AREA_1_system_COOLING_SYS_P-101`

**Uniqueness Guarantee**: External IDs are unique within the configured space.

**Collision Handling**: System tracks created external IDs to prevent duplicates.

### Batch Processing Algorithm

**Purpose**: Process files and assets in batches for efficient resource usage.

**Algorithm**:
1. **Batch Creation**:
   - Divide items into batches of configured size
   - Process batches sequentially

2. **Batch Processing**:
   - For each batch:
     - Process all items in batch
     - Collect results
     - Handle errors per item
     - Update progress

3. **Error Handling**:
   - Track failed items
   - Retry failed items (up to max_attempts)
   - Continue processing remaining items

4. **State Management**:
   - Update state after each batch
   - Support resuming from last successful batch

**Batch Size Recommendations**:
- File processing: 5-10 files per batch
- Asset writing: 100 assets per batch

---

## Configuration System

### Configuration File Structure

Configuration files use YAML format with two main sections:

```yaml
externalId: pipeline_external_id
config:
  parameters:
    # Technical settings
    raw_db: string
    raw_table_state: string
    raw_table_results: string
    raw_table_assets: string
    space: string
    include_resource_type: boolean
    include_resource_subtype: boolean
    pattern_config_path: string (optional)
    output_file: string (optional)
    debug: boolean
    run_all: boolean
    overwrite: boolean
    initialize_state: boolean
    logLevel: string

  data:
    # Business configuration
    hierarchy_levels: [string]
    locations: [dict]
    patterns: [dict]
    limit: integer
    batch_size: integer
    max_attempts: integer
    max_pages_per_chunk: integer
    mime_type: string (optional)
    instance_space: string (optional)
    partial_match: boolean
    min_tokens: integer
    diagram_detect_config: dict
```

### Configuration Loading Process

1. **Handler Receives Request**:
   - Extracts `ExtractionPipelineExtId` from data
   - Calls `load_config_parameters()` function

2. **Load from CDF**:
   - Queries CDF extraction pipeline by external ID
   - Retrieves configuration YAML
   - Parses YAML into Config object

3. **Merge Configuration**:
   - Extracts `parameters` section → technical settings
   - Extracts `data` section → business configuration
   - Merges into data dictionary (config overrides defaults)

4. **Validation**:
   - Validates required fields
   - Validates data types
   - Validates structure (hierarchy levels, locations)

### Configuration Validation

**Validation Functions**:
- `validate_hierarchy_config(config: Dict) -> List[str]`: Validates hierarchy configuration
- `validate_extract_config(config: Dict) -> List[str]`: Validates extraction configuration
- `format_validation_errors(errors: List[str]) -> str`: Formats error messages

**Validation Rules**:
- Hierarchy levels: Must be list, minimum 2 levels, no duplicates
- Locations: Must match hierarchy levels, required fields present
- Patterns: Must have samples, valid pattern syntax
- Processing settings: Valid ranges (limit >= -1, batch_size > 0)

---

## Error Handling and Recovery

### Error Categories

1. **Configuration Errors**:
   - Missing required fields
   - Invalid data types
   - Structure mismatches
   - **Handling**: Validation before processing, clear error messages

2. **CDF API Errors**:
   - Authentication failures
   - Permission errors
   - Rate limiting
   - **Handling**: Retry with exponential backoff, error logging

3. **File Processing Errors**:
   - File not found
   - Unsupported file format
   - Processing failures
   - **Handling**: Track in state table, retry up to max_attempts

4. **Data Errors**:
   - Invalid data format
   - Missing required fields
   - Data corruption
   - **Handling**: Validation, skip invalid items, log warnings

### Error Handling Strategy

1. **Retry Logic**:
   - Transient errors: Retry with exponential backoff
   - Maximum attempts: Configurable (default: 3)
   - Retry delays: 1s, 2s, 4s

2. **State Tracking**:
   - Track processing state in RAW tables
   - Record failed items with error messages
   - Support resuming from last successful state

3. **Error Reporting**:
   - Log errors with context
   - Return error messages in handler response
   - Track error statistics

4. **Graceful Degradation**:
   - Continue processing remaining items on individual failures
   - Skip invalid items with warnings
   - Provide partial results

### Recovery Mechanisms

1. **Incremental Processing**:
   - Process only new/unprocessed files
   - Skip already processed files
   - Update state incrementally

2. **State Recovery**:
   - Load state from RAW tables
   - Resume from last successful batch
   - Recover from partial failures

3. **Data Validation**:
   - Validate data before processing
   - Skip invalid items
   - Report validation errors

---

## Performance and Scalability

### Performance Characteristics

**File Processing**:
- Average processing time: 2-5 seconds per file
- Throughput: 100+ files per hour
- Batch processing: 5-10 files per batch

**Hierarchy Generation**:
- Processing time: O(l × t) where l = locations, t = tags
- Memory usage: O(l + t) for location and tag storage
- Typical performance: 10,000+ assets in < 1 minute

**Asset Writing**:
- Batch writing: 100 assets per batch
- Throughput: 1,000+ assets per minute
- API rate limits: Respect CDF API limits

### Optimization Strategies

1. **Batch Processing**:
   - Process files in batches to reduce memory usage
   - Process assets in batches for efficient API usage
   - Configurable batch sizes

2. **Caching**:
   - Cache compiled patterns
   - Cache location hierarchy
   - Cache file-to-system mappings

3. **Parallel Processing** (Future Enhancement):
   - Process files in parallel (within batch)
   - Parallel hierarchy generation
   - Parallel asset writing

4. **State Management**:
   - Incremental processing (only new files)
   - Efficient state storage in RAW tables
   - Minimal state updates

### Scalability Considerations

**Horizontal Scaling**:
- Functions can run in parallel (multiple instances)
- State stored in shared RAW tables
- No shared state between function instances

**Vertical Scaling**:
- Increase batch sizes for larger instances
- Increase memory for large hierarchies
- Tune based on workload

**Limits**:
- CDF API rate limits
- Function timeout limits (3600 seconds)
- Memory limits (function instance size)

---

## Security and Authentication

### Authentication

**OAuth 2.0 Client Credentials Flow**:
- Function uses OAuth client credentials
- Credentials stored in environment variables
- Scopes: CDF API access

**Environment Variables**:
- `CDF_PROJECT`: CDF project name
- `CDF_CLUSTER`: CDF cluster name
- `CDF_URL`: CDF API URL
- `IDP_CLIENT_ID`: OAuth client ID
- `IDP_CLIENT_SECRET`: OAuth client secret
- `IDP_TENANT_ID`: OAuth tenant ID
- `IDP_TOKEN_URL`: OAuth token URL

### Authorization

**Function Permissions**:
- Read access to CDF files
- Read/write access to RAW tables
- Write access to CDF data model
- Workflow execution permissions

**Data Set Permissions**:
- Functions operate within configured data sets
- Respect data set access controls
- Support multi-tenant scenarios

### Data Security

**Data Encryption**:
- Data encrypted in transit (HTTPS)
- Data encrypted at rest (CDF storage)
- Credentials encrypted in environment

**Data Privacy**:
- No sensitive data in logs
- Credentials never logged
- Error messages sanitized

---

## Deployment Architecture

### CDF Functions Deployment

**Function Structure**:
```
functions/
├── fn_dm_extract_assets_by_pattern/
│   ├── handler.py
│   ├── pipeline.py
│   ├── config.py
│   ├── dependencies.py
│   ├── logger.py
│   ├── requirements.txt
│   └── utils/
├── fn_dm_create_asset_hierarchy/
│   └── [similar structure]
└── fn_dm_write_asset_hierarchy/
    └── [similar structure]
```

**Deployment Process**:
1. Package function code
2. Deploy to CDF Functions
3. Configure function environment variables
4. Set function permissions
5. Test function execution

### CDF Workflows Deployment

**Workflow Structure**:
```
workflows/
├── create_asset_hierarchy_from_files.Workflow.yaml
├── create_asset_hierarchy_from_files.WorkflowVersion.yaml
└── create_asset_hierarchy_from_files.WorkflowTrigger.yaml
```

**Deployment Process**:
1. Deploy workflow definition
2. Deploy workflow version
3. Configure workflow trigger (if scheduled)
4. Test workflow execution

### Module configuration deployment

**Configuration** (single file):
```
default.config.yaml
└── file_asset_source.{extract,create,write}  # parameters + data per step
```

**Deployment Process**:
1. Edit `default.config.yaml` and run `python module.py validate`
2. Run `python module.py build` to sync workflow trigger `input.configuration`
3. Deploy via Cognite Toolkit (`cdf build` / deploy)
4. Test workflow execution (tasks receive `step` + `configuration` from trigger input)

### Infrastructure Requirements

**CDF Services**:
- CDF Functions runtime
- CDF Workflows service
- CDF RAW database
- CDF Data Modeling
- CDF Files API
- CDF Documents API (for diagram detection)

**Storage**:
- RAW database: `db_extract_assets_by_pattern`
- RAW tables: State, results, assets tables
- CDF data model: CogniteAsset view

**Network**:
- Internet access for CDF API
- No inbound network requirements

---

## Testing Strategy

### Unit Testing

**Test Coverage**:
- Individual functions and utilities
- Pattern matching algorithms
- Hierarchy generation algorithms
- Configuration parsing and validation
- Error handling logic

**Test Tools**:
- pytest
- unittest.mock for mocking
- pytest-cov for coverage

**Test Files**:
- `tests/test_asset_tag_classifier.py`
- `tests/test_common.py`
- `tests/test_config_validator.py`
- `tests/test_validate_config.py`

### Integration Testing

**Test Scenarios**:
- End-to-end workflow execution
- Function interactions
- RAW table operations
- CDF data model writes
- Configuration loading

**Test Environment**:
- CDF test project
- Test data sets
- Mock CDF services (where applicable)

### Performance Testing

**Test Metrics**:
- Processing throughput
- Memory usage
- API call efficiency
- Batch processing performance

**Test Scenarios**:
- Small dataset (10 files, 100 assets)
- Medium dataset (100 files, 1,000 assets)
- Large dataset (1,000 files, 10,000 assets)

---

## Appendices

### Appendix A: Data Flow Diagrams

#### Extraction Phase Data Flow

```
CDF Files → File Query → File List → Batch Processing
    ↓
Diagram Detection API → Text Extraction → Pattern Matching
    ↓
Tag Extraction → Result Aggregation → RAW Table Storage
```

#### Hierarchy Creation Data Flow

```
RAW Results Table → Load Extracted Assets → File-to-System Matching
    ↓
Location Configuration → Hierarchy Generation → Asset Classification (optional)
    ↓
Asset Instance Creation → RAW Assets Table Storage
```

#### Asset Writing Data Flow

```
RAW Assets Table → Load Asset Hierarchy → Format Conversion
    ↓
Batch Creation → CDF Data Model API → Batch Writing
    ↓
Result Aggregation → Status Reporting
```

### Appendix B: External ID Naming Conventions

**Level External IDs**:
- Format: `{level_name}_{level1_code}_{level2_code}_{...}`
- Example: `site_DEMO_SITE`, `plant_DEMO_SITE_PLANT_A`

**Asset Tag External IDs**:
- Format: `asset_tag_{level_external_id}_{resource_type?}_{resource_subtype?}_{tag_text}`
- Example: `asset_tag_site_DEMO_SITE_plant_PLANT_A_area_AREA_1_system_COOLING_SYS_P-101`

**Uniqueness**: External IDs are unique within the configured space.

### Appendix C: Pattern Syntax Reference

| Syntax | Description | Example | Matches |
|--------|-------------|---------|---------|
| `[X]` | Any single letter | `[C]-00` | `A-00`, `B-00`, `C-00` |
| `X` | Any letter(s) | `X-00` | `P-00`, `V-00`, `PU-00` |
| `00` | Exact digits | `P-00` | `P-00`, `P-01` (not `P-000`) |
| `000` | Exact digits | `P-000` | `P-000`, `P-001` (not `P-00`) |
| `[X\|Y]` | Letter choice | `[C\|G]-00` | `C-00`, `G-00` |

### Appendix D: API Rate Limits

**CDF API Limits** (typical):
- Files API: 100 requests/second
- RAW API: 100 requests/second
- Data Modeling API: 50 requests/second
- Documents API: 20 requests/second

**Batch Sizing Recommendations**:
- File processing: 5-10 files per batch
- Asset writing: 100 assets per batch
- Adjust based on API rate limits

### Appendix E: Error Codes

| Error Code | Description | Resolution |
|-----------|-------------|------------|
| CONFIG_ERROR | Configuration validation failed | Fix configuration file |
| AUTH_ERROR | Authentication failure | Check credentials |
| PERMISSION_ERROR | Insufficient permissions | Grant required permissions |
| FILE_ERROR | File processing error | Check file format and accessibility |
| API_ERROR | CDF API error | Check API status and retry |
| DATA_ERROR | Data validation error | Fix data format |

---

## Document Control

| Version | Date | Author | Changes |
|--------|------|--------|---------|
| 1.0 | November 2024 | Technical Architecture Team | Initial release |

---

**Document Status**: Approved
**Next Review Date**: Q1 2025
**Distribution**: Technical Teams, Development Teams
