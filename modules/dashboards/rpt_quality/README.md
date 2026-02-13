# CDF Quality Reports Module

This module provides contextualization rate reporting through SQL transformations that calculate link rates and annotation coverage across your data model, enabling data quality monitoring and governance.

## Why Use This Module?

**Monitor Your Contextualization Progress with Automated Reporting**

Understanding how well your data is contextualized is critical for data quality. This module delivers **automated quality metrics** that track link rates across different entity types.

**Key Benefits:**

- 📊 **Comprehensive Metrics**: Link rates for timeseries, maintenance orders, operations, and files
- 🔄 **Automated Workflow**: Scheduled execution for continuous monitoring
- 📈 **Trend Analysis**: Historical data in RAW tables for tracking progress
- 🎯 **Gap Identification**: Reports assets and files not in baseline
- ⚡ **Low Overhead**: SQL transformations with minimal resource usage

**Time & Cost Savings:**

- **Manual Effort**: Eliminates manual data quality audits
- **Early Detection**: Identify contextualization gaps before they impact applications
- **Progress Tracking**: Demonstrate value through measurable improvements

## 🎯 Overview

The CDF Quality Reports module is designed to:
- **Calculate link rates** for various entity types
- **Track annotation coverage** for files
- **Identify gaps** in baseline coverage
- **Store metrics** in RAW tables for reporting
- **Automate execution** via scheduled workflows

## 🏗️ Module Architecture

```
rpt_quality/
├── 📁 data_sets/                           # Dataset definitions
│   └── 📄 dataset.yaml                            # Quality reports dataset
├── 📁 raw/                                 # RAW table definitions
│   └── 📄 raw.Table.yaml                          # Report output table
├── 📁 transformations/                     # SQL Transformations
│   ├── 📄 tr_report_files_annotationrate.Transformation.yaml
│   ├── 📄 tr_report_files_annotationrate.Transformation.sql
│   ├── 📄 tr_report_files_assets_not_in_baseline.Transformation.yaml
│   ├── 📄 tr_report_files_assets_not_in_baseline.Transformation.sql
│   ├── 📄 tr_report_files_files_not_in_baseline.Transformation.yaml
│   ├── 📄 tr_report_files_files_not_in_baseline.Transformation.sql
│   ├── 📄 tr_report_pi_timeseries_linkrate.Transformation.yaml
│   ├── 📄 tr_report_pi_timeseries_linkrate.Transformation.sql
│   ├── 📄 tr_report_sap_maintenanceorders_ref_linkrate.Transformation.yaml
│   ├── 📄 tr_report_sap_maintenanceorders_ref_linkrate.Transformation.sql
│   ├── 📄 tr_report_sap_operations_linkrate.Transformation.yaml
│   └── 📄 tr_report_sap_operations_linkrate.Transformation.sql
├── 📁 workflows/                           # Workflow definitions
│   ├── 📄 wf_contextualization_rate.Workflow.yaml
│   ├── 📄 wf_contextualization_rate.WorkflowVersion.yaml
│   └── 📄 wf_contextualization_rate.WorkflowTrigger.yaml
├── 📄 default.config.yaml                  # Module configuration
└── 📄 module.toml                          # Module metadata
```

## 🚀 Core Components

### Quality Transformations

| Transformation | Description |
|----------------|-------------|
| `tr_report_pi_timeseries_linkrate` | Calculates link rate for PI timeseries to assets |
| `tr_report_sap_maintenanceorders_ref_linkrate` | Link rate for maintenance orders |
| `tr_report_sap_operations_linkrate` | Link rate for SAP operations |
| `tr_report_files_annotationrate` | Annotation coverage for files |
| `tr_report_files_assets_not_in_baseline` | Assets missing from baseline |
| `tr_report_files_files_not_in_baseline` | Files missing from baseline |

### Workflow Orchestration

**Purpose**: Runs all quality transformations in sequence

**Execution Order**:
1. Maintenance orders link rate
2. Operations link rate
3. PI timeseries link rate
4. Files annotation rate
5. Assets not in baseline
6. Files not in baseline

## 🔧 Configuration

### Module Configuration (`default.config.yaml`)

```yaml
# Data Model Settings
organization: ORG
schemaSpace: sp_enterprise_process_industry
datamodelVersion: v1.0
dataset: ingestion

# Authentication
functionClientId: ${IDP_CLIENT_ID}
functionClientSecret: ${IDP_CLIENT_SECRET}

# Annotation Tables (for file annotation metrics)
annotation_db: db_file_annotation
annotation_patterns_tbl: annotation_documents_patterns
annotation_docs_tbl: annotation_documents_docs
annotation_tags_tbl: annotation_documents_tags

# Workflow Schedule (default: never - Feb 29)
contextualization_rate_workflow: "0 0 29 2 *"
workflow_timeout_per_job: 3600
```

## 🏃‍♂️ Getting Started

### 1. Prerequisites

- CDF project with data populated
- Data model deployed (`cdf_process_industry_extension`)
- Source system data ingested (PI, SAP, files)

### 2. Configure the Module

Update your `config.<env>.yaml` under the module variables section:

```yaml
variables:
  modules:
    rpt_quality:
      organization: YOUR_ORG
      schemaSpace: sp_enterprise_process_industry
      datamodelVersion: v1.0
      dataset: ingestion
      functionClientId: ${IDP_CLIENT_ID}
      functionClientSecret: ${IDP_CLIENT_SECRET}
      annotation_db: db_file_annotation
      annotation_patterns_tbl: annotation_documents_patterns
      annotation_docs_tbl: annotation_documents_docs
      annotation_tags_tbl: annotation_documents_tags
      contextualization_rate_workflow: "0 0 * * *"  # Daily at midnight
      workflow_timeout_per_job: 3600
```

### 3. Deploy the Module

```bash
cdf deploy --env your-environment
```

### 4. Run Reports

```bash
# Trigger workflow manually
cdf workflows trigger wf_contextualization_rate

# Or run individual transformation
cdf transformations run tr_report_pi_timeseries_linkrate
```

## 📊 Report Output

Reports are written to RAW table `tbl_contextualization_rate_report` in `db_quality_reports`:

| Field | Description |
|-------|-------------|
| entity_type | Type of entity being measured |
| total_count | Total entities of this type |
| linked_count | Entities with links/connections |
| link_rate | Percentage of linked entities |
| timestamp | When the report was generated |

## 🎯 Use Cases

### Data Quality Monitoring
- **Progress Tracking**: Monitor contextualization progress over time
- **SLA Compliance**: Ensure data quality meets requirements
- **Gap Analysis**: Identify areas needing attention

### Governance
- **Audit Trail**: Historical metrics for compliance
- **Baseline Validation**: Ensure all expected entities are present
- **Quality Gates**: Block deployments if quality thresholds not met

## 🔧 Troubleshooting

### Common Issues

1. **Low Link Rates**
   - Verify source data has matching keys
   - Check contextualization transformations have run
   - Review tag extraction logic

2. **Missing Baseline Entities**
   - Ensure baseline data is loaded
   - Verify entity name matching logic

## 📄 License

This module is part of the Cognite Templates repository and follows the same licensing terms.

