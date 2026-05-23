# Functional Specification: Create Asset Hierarchy from Files

**Document Version:** 1.0
**Date:** November 2024
**Author:** Business Analysis & Technical Writing Team
**Status:** Approved

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Business Context and Objectives](#business-context-and-objectives)
3. [System Overview](#system-overview)
4. [Functional Requirements](#functional-requirements)
5. [System Architecture](#system-architecture)
6. [Data Flow and Processing](#data-flow-and-processing)
7. [Configuration Requirements](#configuration-requirements)
8. [Use Cases and Scenarios](#use-cases-and-scenarios)
9. [Non-Functional Requirements](#non-functional-requirements)
10. [Dependencies and Prerequisites](#dependencies-and-prerequisites)
11. [Testing and Validation](#testing-and-validation)
12. [Deployment and Operations](#deployment-and-operations)
13. [Appendices](#appendices)

---

## Executive Summary

The **Create Asset Hierarchy from Files** solution is an automated system that extracts asset tags from engineering diagram files (PDFs, DWG, etc.) and creates hierarchical asset structures in Cognite Data Fusion (CDF). The solution transforms unstructured diagram data into organized, queryable asset hierarchies that reflect an organization's physical and operational structure.

### Key Capabilities

- **Automated Asset Extraction**: Extracts asset tags from diagram files using pattern-based matching
- **Hierarchical Organization**: Creates multi-level asset hierarchies based on organizational structure (sites, plants, areas, systems)
- **Flexible Configuration**: Supports customizable hierarchy levels and industry-specific patterns
- **Batch Processing**: Handles large volumes of files efficiently with configurable batch sizes
- **CDF Integration**: Seamlessly integrates with CDF data modeling for asset management

### Business Value

- **Time Savings**: Reduces manual asset entry from days/weeks to hours
- **Accuracy**: Eliminates human error in asset tag transcription
- **Scalability**: Processes thousands of files and assets automatically
- **Consistency**: Ensures standardized asset naming and hierarchy across the organization
- **Traceability**: Maintains links between source files and extracted assets

---

## Business Context and Objectives

### Business Problem

Organizations managing industrial facilities face the challenge of digitizing and organizing asset information from engineering diagrams. Traditional manual processes are:

- **Time-consuming**: Requiring weeks or months to process hundreds of diagrams
- **Error-prone**: Human transcription errors lead to inconsistent asset data
- **Inflexible**: Difficult to update when diagrams change or new assets are added
- **Non-scalable**: Cannot efficiently handle large-scale facility digitization projects

### Business Objectives

1. **Automate Asset Extraction**: Eliminate manual data entry from engineering diagrams
2. **Create Structured Hierarchies**: Organize assets according to organizational structure
3. **Enable Scalability**: Process large volumes of files and assets efficiently
4. **Ensure Data Quality**: Maintain consistency and accuracy in asset data
5. **Support Multiple Industries**: Accommodate different hierarchy structures and naming conventions

### Success Criteria

- **Processing Speed**: Process 100+ files per hour
- **Extraction Accuracy**: Achieve 95%+ accuracy in asset tag extraction
- **Hierarchy Completeness**: Successfully create hierarchies for all configured locations
- **User Adoption**: Enable non-technical users to configure and deploy the solution

---

## System Overview

### High-Level Description

The Create Asset Hierarchy from Files solution is a three-phase automated pipeline that:

1. **Extracts** asset tags from diagram files using pattern matching
2. **Organizes** extracted assets into hierarchical structures based on location configuration
3. **Writes** the generated hierarchy to CDF data modeling for use in applications

### Solution Components

The solution consists of three primary functions orchestrated by a CDF workflow:

1. **Extract Assets by Pattern** (`fn_dm_extract_assets_by_pattern`)
2. **Create Asset Hierarchy** (`fn_dm_create_asset_hierarchy`)
3. **Write Asset Hierarchy** (`fn_dm_write_asset_hierarchy`)

### Key Features

- **Pattern-Based Extraction**: Flexible pattern matching supports various asset tag formats
- **Customizable Hierarchies**: Configurable hierarchy levels (e.g., Site → Plant → Area → System)
- **Industry Templates**: Pre-configured templates for manufacturing, oil & gas, utilities, and pharmaceuticals
- **Batch Processing**: Configurable batch sizes for efficient processing
- **Error Handling**: Retry logic and failure tracking for robust operation
- **State Management**: Tracks processing state to support incremental updates

---

## Functional Requirements

### FR-1: Asset Tag Extraction

**Requirement ID:** FR-1
**Priority:** High
**Description:** The system shall extract asset tags from diagram files using pattern-based matching.

#### Functional Details

- **FR-1.1**: Extract asset tags from PDF, DWG, and other supported diagram file formats
- **FR-1.2**: Support pattern matching using flexible syntax:
  - `[X]` for any specific letter (e.g., `[C]-00` spcifically matches C-00, C-01, but not B-01)
  - `X` for any letter(s) (e.g., `X-00` matches P-00, V-00, etc.)
  - Numbers for digits (e.g., `00`, `000`, `0000`)
- **FR-1.3**: Support multiple pattern categories:
  - Equipment (pumps, valves, tanks, etc.)
  - Instruments (controllers, transmitters, indicators, etc.)
  - Documents (P&IDs, schematics, etc.)
  - General (catch-all patterns)
- **FR-1.4**: Extract metadata associated with asset tags:
  - Resource type (e.g., "major_equipment", "instrument")
  - Resource subtype (e.g., "Rotating_Equipment", "Control_Valve")
  - Standard reference (e.g., "ISA51", "Custom")
- **FR-1.5**: Support partial matching for flexible tag recognition
- **FR-1.6**: Filter files by MIME type and instance space
- **FR-1.7**: Process files in configurable batches
- **FR-1.8**: Track processing state to support incremental updates

#### Inputs

- Diagram files stored in CDF
- Pattern configuration (YAML)
- File filters (MIME type, instance space, limit)

#### Outputs

- Extracted asset tags with metadata
- Processing results stored in RAW tables
- State tracking information

#### Acceptance Criteria

- Successfully extracts asset tags matching configured patterns
- Achieves 95%+ accuracy in tag extraction
- Handles files with embedded text and annotations
- Supports batch processing of 100+ files

---

### FR-2: Hierarchy Creation

**Requirement ID:** FR-2
**Priority:** High
**Description:** The system shall create hierarchical asset structures based on organizational location configuration.

#### Functional Details

- **FR-2.1**: Support customizable hierarchy levels (e.g., Site → Plant → Area → System)
- **FR-2.2**: Match extracted assets to systems based on file-to-system mapping
- **FR-2.3**: Generate hierarchical asset structure with parent-child relationships
- **FR-2.4**: Support optional intermediate levels:
  - Resource type grouping (e.g., all pumps together)
  - Resource subtype grouping (e.g., all centrifugal pumps together)
- **FR-2.5**: Generate unique external IDs for all hierarchy levels
- **FR-2.6**: Create descriptive names and descriptions for hierarchy nodes
- **FR-2.7**: Apply tags to assets based on hierarchy level
- **FR-2.8**: Support multiple sites, plants, areas, and systems
- **FR-2.9**: Handle files assigned to multiple systems (if applicable)
- **FR-2.10**: Support nested location structures of arbitrary depth

#### Inputs

- Extracted asset tags from FR-1
- Location hierarchy configuration
- Hierarchy options (include_resource_type, include_resource_subtype)

#### Outputs

- Hierarchical asset structure
- Asset instances with parent-child relationships
- Generated assets stored in RAW tables

#### Acceptance Criteria

- Successfully creates hierarchy matching configured structure
- All assets correctly assigned to their systems
- Parent-child relationships correctly established
- External IDs are unique and follow naming conventions

---

### FR-3: Asset Writing to CDF

**Requirement ID:** FR-3
**Priority:** High
**Description:** The system shall write the generated asset hierarchy to CDF data modeling.

#### Functional Details

- **FR-3.1**: Read asset hierarchy from RAW tables or workflow context
- **FR-3.2**: Convert assets to CogniteAsset format
- **FR-3.3**: Write assets to configured CDF data model view
- **FR-3.4**: Support batch writing for efficient processing
- **FR-3.5**: Handle updates to existing assets
- **FR-3.6**: Support dry-run mode for testing
- **FR-3.7**: Validate asset data before writing
- **FR-3.8**: Handle write errors gracefully with retry logic

#### Inputs

- Generated asset hierarchy from FR-2
- View configuration (space, external ID, version)
- Batch processing parameters

#### Outputs

- Assets written to CDF data modeling
- Write operation results and statistics

#### Acceptance Criteria

- Successfully writes all assets to CDF
- Maintains parent-child relationships in CDF
- Handles batch processing efficiently
- Provides detailed write operation results

---

### FR-4: Configuration Management

**Requirement ID:** FR-4
**Priority:** Medium
**Description:** The system shall support flexible, user-friendly configuration.

#### Functional Details

- **FR-4.1**: Support YAML-based configuration files
- **FR-4.2**: Separate business configuration from technical settings
- **FR-4.3**: Provide industry-specific configuration templates
- **FR-4.4**: Support validation of configuration files
- **FR-4.5**: Provide clear error messages for configuration issues
- **FR-4.6**: Support inline documentation in configuration files
- **FR-4.7**: Allow configuration updates without code changes

#### Configuration Sections

1. **Business Configuration**:
   - Hierarchy levels
   - Location structure
   - File assignments
   - Asset tag patterns

2. **Technical Configuration**:
   - Storage settings (RAW database, tables)
   - Processing parameters (batch size, limits)
   - Logging levels
   - Instance space configuration

#### Acceptance Criteria

- Non-technical users can configure the system
- Configuration validation catches common errors
- Clear error messages guide users to fix issues
- Templates support common industry structures

---

### FR-5: Workflow Orchestration

**Requirement ID:** FR-5
**Priority:** High
**Description:** The system shall orchestrate the three-phase pipeline via CDF workflows.

#### Functional Details

- **FR-5.1**: Execute functions in sequential order (Extract → Create → Write)
- **FR-5.2**: Pass data between functions via RAW tables and workflow context
- **FR-5.3**: Support scheduled execution (e.g., daily at 3:00 AM UTC)
- **FR-5.4**: Support manual execution via API or UI
- **FR-5.5**: Handle task failures with retry logic (3 attempts)
- **FR-5.6**: Abort workflow on critical failures
- **FR-5.7**: Provide execution status and monitoring
- **FR-5.8**: Support workflow versioning

#### Workflow Tasks

1. **Extract Assets Task**: Executes `fn_dm_extract_assets_by_pattern`
2. **Create Hierarchy Task**: Executes `fn_dm_create_asset_hierarchy` (depends on Extract)
3. **Write Assets Task**: Executes `fn_dm_write_asset_hierarchy` (depends on Create)

#### Acceptance Criteria

- Workflow executes all tasks in correct order
- Handles failures gracefully with retries
- Provides clear execution status
- Supports both scheduled and manual execution

---

## System Architecture

### Architecture Overview

The solution follows a three-tier architecture:

1. **Extraction Layer**: Processes files and extracts asset tags
2. **Transformation Layer**: Creates hierarchical structures from extracted data
3. **Integration Layer**: Writes assets to CDF data modeling

### Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CDF Workflow Orchestrator                 │
│         (create_asset_hierarchy_from_files)                  │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
│   Extract    │  │  Create Hierarchy│  │    Write     │
│   Assets     │──▶│      Assets      │──▶│   Assets     │
│              │  │                  │  │              │
└──────────────┘  └──────────────────┘  └──────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
│   RAW Tables │  │   RAW Tables      │  │  CDF Data     │
│   (Results)  │  │   (Assets)       │  │  Modeling     │
└──────────────┘  └──────────────────┘  └──────────────┘
```

### Function Details

#### Function 1: Extract Assets by Pattern

- **External ID**: `fn_dm_extract_assets_by_pattern`
- **Purpose**: Extract asset tags from diagram files
- **Input**: CDF files, pattern configuration
- **Output**: Extracted assets in RAW table
- **Technology**: Python, Cognite SDK, pattern matching

#### Function 2: Create Asset Hierarchy

- **External ID**: `fn_dm_create_asset_hierarchy`
- **Purpose**: Create hierarchical asset structure
- **Input**: Extracted assets, location configuration
- **Output**: Hierarchical assets in RAW table
- **Technology**: Python, Cognite SDK, hierarchy generation

#### Function 3: Write Asset Hierarchy

- **External ID**: `fn_dm_write_asset_hierarchy`
- **Purpose**: Write assets to CDF data modeling
- **Input**: Hierarchical assets
- **Output**: Assets in CDF data model
- **Technology**: Python, Cognite SDK, batch writing

### Data Storage

- **RAW Tables**: Store intermediate results and state
  - `extract_assets_by_pattern_state`: Processing state
  - `extract_assets_by_pattern_results`: Extracted assets
  - `extract_assets_by_pattern_assets`: Generated hierarchy
- **CDF Data Modeling**: Final asset storage
  - CogniteAsset view in configured space

---

## Data Flow and Processing

### Phase 1: Asset Extraction

1. **File Discovery**
   - Query CDF files based on filters (MIME type, instance space, limit)
   - Retrieve file metadata and content

2. **Pattern Matching**
   - Process files using diagram detection
   - Extract text and annotations
   - Match patterns against extracted text
   - Generate asset tag candidates

3. **Result Storage**
   - Store extracted assets in RAW table
   - Track processing state
   - Record file processing status

**Data Structure**:
```json
{
  "file_id": 12345,
  "file_info": {...},
  "results": {
    "items": [
      {
        "annotations": [
          {
            "text": "P-101",
            "confidence": 0.95,
            "entities": [
              {
                "sample": "P-101",
                "resourceType": "major_equipment",
                "resourceSubType": "Rotating_Equipment",
                ...
              }
            ]
          }
        ]
      }
    ]
  }
}
```

### Phase 2: Hierarchy Creation

1. **Data Loading**
   - Read extracted assets from RAW results table
   - Load location hierarchy configuration
   - Load hierarchy options

2. **File-to-System Matching**
   - Match files to systems based on file names
   - Group assets by their associated systems

3. **Hierarchy Generation**
   - Generate hierarchy levels (Site → Plant → Area → System)
   - Create intermediate levels (ResourceType, ResourceSubType) if configured
   - Generate unique external IDs for all levels
   - Create parent-child relationships

4. **Asset Creation**
   - Create asset instances for each hierarchy level
   - Assign tags based on hierarchy level
   - Generate descriptions and metadata

**Data Structure**:
```json
{
  "externalId": "site_DEMO_SITE",
  "space": "sp_enterprise_schema",
  "properties": {
    "name": "DEMO_SITE",
    "description": "Demo Site",
    "tags": ["site"]
  }
}
```

### Phase 3: Asset Writing

1. **Data Loading**
   - Read asset hierarchy from RAW assets table
   - Load view configuration

2. **Asset Conversion**
   - Convert assets to CogniteAsset format
   - Validate asset data
   - Prepare batch writes

3. **Batch Writing**
   - Write assets to CDF in batches
   - Handle updates to existing assets
   - Track write results

4. **Result Reporting**
   - Generate write operation statistics
   - Report successes and failures
   - Update processing state

---

## Configuration Requirements

### Configuration File Structure

Configuration is organized into two main sections:

1. **Business Configuration** (`data` section): User-customizable settings
2. **Technical Configuration** (`parameters` section): System settings

### Business Configuration

#### Hierarchy Levels

```yaml
hierarchy_levels:
  - site
  - plant
  - area
  - system
```

**Requirements**:
- Minimum 2 levels, maximum 10 levels
- Level names must be unique
- Level names used as tags on assets

#### Location Structure

```yaml
locations:
  - name: "SITE_NAME"
    description: "Site Description"
    locations:
      - name: "PLANT_NAME"
        description: "Plant Description"
        locations:
          - name: "AREA_NAME"
            description: "Area Description"
            locations:
              - name: "SYSTEM_NAME"
                description: "System Description"
                files:
                  - "File-001"
                  - "File-002"
```

**Requirements**:
- Each location must have a `name` field
- `description` is optional but recommended
- Files listed at the deepest level (system level)
- File names must match CDF file names (without extensions)

#### Asset Tag Patterns

```yaml
patterns:
  - category: equipment
    resourceType: major_equipment
    resourceSubType: Rotating_Equipment
    standard: Custom
    sample:
      - "[C|G]-00"
      - "[C|G]-000"
```

**Requirements**:
- At least one pattern required
- Pattern syntax:
  - `[X]` = any single letter
  - `X` = any letter(s)
  - Numbers = digits
- Categories: `equipment`, `instrument`, `document`, `general`

### Technical Configuration

#### Storage Settings

```yaml
raw_db: db_extract_assets_by_pattern
raw_table_state: extract_assets_by_pattern_state
raw_table_results: extract_assets_by_pattern_results
raw_table_assets: extract_assets_by_pattern_assets
```

#### Processing Settings

```yaml
limit: -1                    # -1 = all files, or number for testing
batch_size: 5                # Files per batch
max_attempts: 3              # Retry failed files
max_pages_per_chunk: 50      # Pages per file chunk
```

#### Hierarchy Options

```yaml
include_resource_type: false   # Group by equipment type
include_resource_subtype: true # Group by equipment subtype
```

### Industry Templates

Pre-configured templates available for:

- **Manufacturing**: Site → Plant → Area → System
- **Oil & Gas**: Site → Facility → Unit → System
- **Utilities**: Region → Site → Building → Room → System
- **Pharmaceuticals**: Site → Building → Suite → System

---

## Use Cases and Scenarios

### Use Case 1: Manufacturing Facility Digitization

**Actor**: Manufacturing Data Engineer
**Goal**: Digitize asset information from P&ID diagrams for a manufacturing facility

**Preconditions**:
- P&ID diagrams uploaded to CDF
- Facility structure known (Site → Plant → Area → System)
- Asset tag patterns identified

**Main Flow**:
1. Configure hierarchy levels: `[site, plant, area, system]`
2. Define location structure with plants, areas, and systems
3. Assign P&ID files to systems
4. Configure asset tag patterns (e.g., `P-101`, `V-201`)
5. Execute workflow
6. Review generated asset hierarchy in CDF

**Postconditions**:
- Asset hierarchy created in CDF
- All assets correctly assigned to systems
- Parent-child relationships established

**Success Criteria**:
- 95%+ of assets correctly extracted
- All systems have associated assets
- Hierarchy matches organizational structure

---

### Use Case 2: Oil & Gas Refinery Asset Management

**Actor**: Refinery Operations Manager
**Goal**: Create asset hierarchy for refinery units from engineering diagrams

**Preconditions**:
- Engineering diagrams available in CDF
- Refinery structure: Site → Facility → Unit → System
- ISA 5.1 standard tag patterns

**Main Flow**:
1. Use oil & gas template
2. Configure refinery locations (facilities, units, systems)
3. Assign diagrams to systems
4. Configure ISA 5.1 patterns
5. Execute workflow
6. Verify assets in CDF applications

**Postconditions**:
- Refinery asset hierarchy in CDF
- Assets classified by ISA 5.1 standards
- Ready for use in operations applications

---

### Use Case 3: Utilities Facility Management

**Actor**: Utilities Asset Manager
**Goal**: Organize assets across multiple sites and buildings

**Preconditions**:
- Facility diagrams in CDF
- Multi-site structure: Region → Site → Building → Room → System

**Main Flow**:
1. Use utilities template
2. Configure regions, sites, buildings, rooms, systems
3. Assign diagrams to systems
4. Configure asset patterns
5. Execute workflow
6. Review multi-site hierarchy

**Postconditions**:
- Multi-site asset hierarchy created
- Assets organized by location
- Ready for facility management applications

---

### Use Case 4: Incremental Updates

**Actor**: Data Engineer
**Goal**: Update asset hierarchy when new diagrams are added

**Preconditions**:
- Existing asset hierarchy in CDF
- New diagrams uploaded to CDF
- Processing state maintained

**Main Flow**:
1. Configure `run_all: false` to process only new files
2. Execute workflow
3. System processes only new/unprocessed files
4. Updates asset hierarchy with new assets

**Postconditions**:
- New assets added to existing hierarchy
- Existing assets unchanged
- Processing state updated

---

## Non-Functional Requirements

### NFR-1: Performance

- **Processing Speed**: Process 100+ files per hour
- **Batch Processing**: Support batch sizes of 5-10 files
- **Scalability**: Handle 10,000+ files and 100,000+ assets
- **Response Time**: Complete workflow execution within 24 hours for large datasets

### NFR-2: Reliability

- **Error Handling**: Retry failed operations up to 3 times
- **State Management**: Maintain processing state for recovery
- **Data Integrity**: Ensure no data loss during processing
- **Failure Recovery**: Support resuming from last successful state

### NFR-3: Usability

- **Configuration**: Non-technical users can configure the system
- **Documentation**: Comprehensive guides and examples provided
- **Error Messages**: Clear, actionable error messages
- **Templates**: Industry-specific templates available

### NFR-4: Maintainability

- **Modularity**: Functions are self-contained and modular
- **Configuration**: Changes via configuration files, not code
- **Logging**: Comprehensive logging for debugging
- **Versioning**: Support workflow and configuration versioning

### NFR-5: Security

- **Authentication**: OAuth-based authentication
- **Authorization**: Role-based access control
- **Data Privacy**: Respect CDF data set permissions
- **Audit Trail**: Log all operations for audit purposes

### NFR-6: Compatibility

- **File Formats**: Support PDF, DWG, and other diagram formats
- **CDF Versions**: Compatible with CDF API v1
- **Standards**: Support ISA 5.1, ISO 14224, and custom standards
- **Platforms**: Cloud-based, platform-agnostic

---

## Dependencies and Prerequisites

### System Dependencies

1. **Cognite Data Fusion (CDF)**
   - CDF project with appropriate permissions
   - CDF Functions runtime environment
   - CDF Workflows service
   - CDF Data Modeling service
   - CDF RAW database service

2. **CDF Functions**
   - `fn_dm_extract_assets_by_pattern` deployed
   - `fn_dm_create_asset_hierarchy` deployed
   - `fn_dm_write_asset_hierarchy` deployed

3. **CDF Extraction Pipelines**
   - `ctx_extract_assets_by_pattern_default` configured
   - `ctx_create_asset_hierarchy_default` configured
   - `ctx_write_asset_hierarchy_default` configured

4. **CDF Workflows**
   - `create_asset_hierarchy_from_files` workflow deployed

### Data Prerequisites

1. **CDF Files**
   - Source diagram files uploaded to CDF
   - Files accessible by function service account
   - Files match configured filters (MIME type, instance space)

2. **RAW Tables**
   - `db_extract_assets_by_pattern.extract_assets_by_pattern_state`
   - `db_extract_assets_by_pattern.extract_assets_by_pattern_results`
   - `db_extract_assets_by_pattern.extract_assets_by_pattern_assets`

3. **CDF Data Model**
   - CogniteAsset view configured in target space
   - View accessible by function service account
   - Appropriate write permissions

### Configuration Prerequisites

1. **Location Configuration**
   - Hierarchy levels defined
   - Location structure configured
   - Files assigned to systems

2. **Pattern Configuration**
   - Asset tag patterns defined
   - Pattern categories specified
   - Resource types and subtypes configured (optional)

3. **Processing Configuration**
   - Batch sizes configured
   - Limits set (for testing or production)
   - Retry settings configured

### Authentication Prerequisites

1. **Function Credentials**
   - `functionClientId` configured
   - `functionClientSecret` configured
   - OAuth client has appropriate permissions

2. **Service Account Permissions**
   - Read access to source files
   - Write access to RAW tables
   - Write access to CDF data model
   - Workflow execution permissions

---

## Testing and Validation

### Test Strategy

#### Unit Testing

- Test individual functions in isolation
- Mock external dependencies (CDF client, file system)
- Test configuration parsing and validation
- Test pattern matching logic
- Test hierarchy generation algorithms

#### Integration Testing

- Test function interactions
- Test data flow between functions
- Test RAW table operations
- Test CDF data model writes
- Test workflow orchestration

#### System Testing

- End-to-end workflow execution
- Large-scale file processing
- Error handling and recovery
- Performance under load
- Configuration validation

### Test Scenarios

#### Scenario 1: Basic Extraction

**Objective**: Verify asset tag extraction from diagram files

**Steps**:
1. Upload test diagram files to CDF
2. Configure simple patterns
3. Execute extraction function
4. Verify extracted assets in RAW table

**Expected Results**:
- Assets extracted matching patterns
- Metadata correctly assigned
- Results stored in RAW table

#### Scenario 2: Hierarchy Creation

**Objective**: Verify hierarchy generation from extracted assets

**Steps**:
1. Load extracted assets from Scenario 1
2. Configure location hierarchy
3. Execute hierarchy creation function
4. Verify hierarchy structure

**Expected Results**:
- Hierarchy matches configuration
- Parent-child relationships correct
- External IDs unique and valid

#### Scenario 3: Asset Writing

**Objective**: Verify assets written to CDF data model

**Steps**:
1. Load hierarchy from Scenario 2
2. Configure view settings
3. Execute write function
4. Verify assets in CDF

**Expected Results**:
- Assets written to CDF
- Relationships maintained
- No data loss

#### Scenario 4: End-to-End Workflow

**Objective**: Verify complete workflow execution

**Steps**:
1. Configure all three functions
2. Execute workflow
3. Monitor execution
4. Verify final results

**Expected Results**:
- All tasks complete successfully
- Data flows correctly between functions
- Final assets in CDF

### Validation Criteria

- **Extraction Accuracy**: 95%+ of assets correctly extracted
- **Hierarchy Completeness**: 100% of configured systems have assets
- **Data Integrity**: No data loss or corruption
- **Performance**: Meets processing speed requirements
- **Error Handling**: Graceful handling of failures

---

## Deployment and Operations

### Deployment Process

#### Step 1: Prerequisites Setup

1. Create CDF project and configure permissions
2. Create RAW database and tables
3. Create CDF data model view
4. Upload source diagram files
5. Configure authentication credentials

#### Step 2: Function Deployment

1. Deploy `fn_dm_extract_assets_by_pattern`
2. Deploy `fn_dm_create_asset_hierarchy`
3. Deploy `fn_dm_write_asset_hierarchy`
4. Verify function deployment

#### Step 3: Pipeline Configuration

1. Configure extraction pipeline
2. Configure hierarchy creation pipeline
3. Configure asset writing pipeline
4. Validate configurations

#### Step 4: Workflow Deployment

1. Deploy workflow definition
2. Deploy workflow version
3. Configure workflow trigger (if scheduled)
4. Verify workflow deployment

#### Step 5: Testing

1. Execute test workflow run
2. Verify results
3. Validate data quality
4. Performance testing

### Operational Procedures

#### Monitoring

- **Workflow Execution**: Monitor workflow runs via CDF UI or API
- **Function Logs**: Review function logs for errors
- **RAW Tables**: Inspect state and results tables
- **CDF Assets**: Verify assets in data model

#### Maintenance

- **Configuration Updates**: Update configuration files as needed
- **Pattern Updates**: Add new patterns for new asset types
- **Location Updates**: Update location structure when organizational changes occur
- **Performance Tuning**: Adjust batch sizes and limits based on performance

#### Troubleshooting

**Common Issues**:

1. **No Assets Extracted**
   - Verify patterns match file content
   - Check file filters (MIME type, instance space)
   - Review extraction logs

2. **Hierarchy Creation Fails**
   - Verify extracted assets exist
   - Check location configuration
   - Verify file-to-system matching

3. **Asset Writing Fails**
   - Verify asset hierarchy exists
   - Check view configuration
   - Verify write permissions

4. **Performance Issues**
   - Adjust batch sizes
   - Review file processing limits
   - Check CDF API rate limits

### Backup and Recovery

- **State Backup**: RAW tables maintain processing state
- **Incremental Processing**: Support resuming from last state
- **Data Recovery**: Assets can be regenerated from source files
- **Configuration Backup**: Version control for configuration files

---

## Appendices

### Appendix A: Configuration Examples

#### Example 1: Simple 3-Level Hierarchy

```yaml
hierarchy_levels:
  - facility
  - building
  - room

locations:
  - name: "Main Facility"
    description: "Primary manufacturing facility"
    locations:
      - name: "Building A"
        description: "Production building"
        locations:
          - name: "Control Room"
            description: "Main control room"
            files:
              - "Control-Diagram-001"
```

#### Example 2: Manufacturing Hierarchy

```yaml
hierarchy_levels:
  - site
  - plant
  - area
  - system

locations:
  - name: "MANUFACTURING_SITE"
    description: "Main Manufacturing Site"
    locations:
      - name: "PLANT_A"
        description: "Production Plant A"
        locations:
          - name: "AREA_1"
            description: "Production Area 1"
            locations:
              - name: "COOLING_SYS"
                description: "Cooling System"
                files:
                  - "CW-001"
                  - "CW-002"
```

### Appendix B: Pattern Syntax Reference

| Syntax | Description | Example | Matches |
|--------|-------------|---------|---------|
| `[X]` | Any single letter | `[C]-00` | `A-00`, `B-00`, `C-00` |
| `X` | Any letter(s) | `X-00` | `P-00`, `V-00`, `PU-00` |
| `00` | Exact digits | `P-00` | `P-00`, `P-01` (not `P-000`) |
| `000` | Exact digits | `P-000` | `P-000`, `P-001` (not `P-00`) |
| `[X\|Y]` | Letter choice | `[C\|G]-00` | `C-00`, `G-00` |

### Appendix C: Industry Templates

#### Manufacturing Template

- **Levels**: Site → Plant → Area → System
- **Use Case**: Production facilities, assembly plants
- **File**: `config.template.manufacturing.yaml`

#### Oil & Gas Template

- **Levels**: Site → Facility → Unit → System
- **Use Case**: Refineries, production fields
- **File**: `config.template.oil_gas.yaml`
- **Standards**: ISO 14224 aligned

#### Utilities Template

- **Levels**: Region → Site → Building → Room → System
- **Use Case**: Power plants, substations
- **File**: `config.template.utilities.yaml`

#### Pharmaceuticals Template

- **Levels**: Site → Building → Suite → System
- **Use Case**: Pharmaceutical facilities, cleanrooms
- **File**: `config.template.pharmaceuticals.yaml`
- **Standards**: GMP-compliant structures

### Appendix D: Glossary

- **Asset Tag**: Identifier for equipment or instruments (e.g., `P-101`, `FCV-201`)
- **Hierarchy Level**: A level in the organizational structure (e.g., Site, Plant, Area)
- **Location**: A node in the hierarchy structure with a name and description
- **Pattern**: A template for matching asset tags in diagram files
- **RAW Table**: CDF RAW database table for storing intermediate results
- **Resource Type**: Category of equipment (e.g., `major_equipment`, `instrument`)
- **Resource Subtype**: Specific type within a category (e.g., `Rotating_Equipment`, `Control_Valve`)
- **System**: The deepest level in the hierarchy where files are assigned

### Appendix E: References

- **CDF Documentation**: [Cognite Data Fusion Documentation](https://docs.cognite.com/)
- **CDF Toolkit**: [CDF Toolkit GitHub](https://github.com/cognitedata/cdf-toolkit)
- **ISA 5.1 Standard**: Instrumentation Symbols and Identification
- **ISO 14224**: Petroleum, petrochemical and natural gas industries — Collection and exchange of reliability and maintenance data for equipment

---

## Document Control

| Version | Date | Author | Changes |
|--------|------|--------|---------|
| 1.0 | November 2024 | Business Analysis & Technical Writing Team | Initial release |

---

**Document Status**: Approved
**Next Review Date**: Q1 2025
**Distribution**: Internal Use
