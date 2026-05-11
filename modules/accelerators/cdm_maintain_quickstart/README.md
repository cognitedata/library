# CDM Maintain Quickstart

This quickstart package provides a complete, ready-to-deploy configuration for Cognite Maintain. It includes everything needed to get maintenance operations, workflows, and asset contextualization set up.

## Why Use This Package?

**Pre-built Data Models & Configuration**

This quickstart includes pre-configured data models, views, containers, and foundational setup templates for Maintain.

**Key Benefits:**

- ⚡ **Pre-configured Data Models**: All necessary containers, views, and spaces ready to deploy
- 🎯 **Optimized configuration**: Optimized configuration for common maintenance scenarios
- 🔄 **Production-Ready Foundation**: Includes config, locations, data models, and sample data
- 📊 **Maintenance-Centric Design**: Purpose-built for work orders, assets, and maintenance workflows
- ⏱️ **Pre-built Foundation**: Eliminates manual data model design and foundational configuration

**Key Advantages:**

- **Standardized Structure**: Data model templates for common maintenance scenarios
- **Reduced Configuration**: Skip repetitive setup tasks with pre-built templates
- **Clear Architecture**: Modules organized by function (config, location, solution, source, sample data)

## 🎯 Overview

The CDM Maintain Quickstart package enables you to:
- **Deploy Maintain infrastructure** with pre-built data models and configurations
- **Organize maintenance data** with structured containers and views
- **Track work orders** with asset and location hierarchies
- **Configure asset locations** with custom hierarchies
- **Load sample data** to validate your setup quickly
- **Integrate source systems** with standardized data ingestion patterns

## 🏗️ Package Architecture

```
cdm_maintain_quickstart/
├── 📁 cdf_maintain_config_base/          # Base Maintain configuration
│   ├── 📄 module.toml                    # Module metadata
│   ├── 📄 default.config.yaml            # Configuration
│   └── 📁 containers/                    # Data model containers (APM Config)
│   └── 📁 dm/                            # Data models and spaces
│   └── 📁 views/                         # Data model views
│
├── 📁 cdf_maintain_location/             # Location hierarchy setup
│   ├── 📄 module.toml
│   ├── 📄 default.config.yaml
│   └── 📁 locations/                     # Location filters
│   └── 📁 nodes/                         # Location nodes
│
├── 📁 cdf_maintain_solution_model/       # Maintain solution data model
│   ├── 📄 module.toml
│   ├── 📄 default.config.yaml
│   ├── 📁 containers/                    # Solution model containers
│   ├── 📁 dm/                            # Solution data models
│   ├── 📁 views/                         # Solution views
│   └── 📁 globalNodes/                   # Global node definitions
│
├── 📁 cdf_maintain_source_data_model/    # Source system data model
│   ├── 📄 module.toml
│   ├── 📄 default.config.yaml
│   ├── 📁 containers/                    # Source containers
│   ├── 📁 dm/                            # Source data models
│   └── 📁 views/                         # Source views
│
└── 📁 cdf_sample_data/                   # Sample data for validation
    ├── 📄 module.toml
    ├── 📄 default.config.yaml
    ├── 📁 data_modeling/                 # Sample maintenance records
    └── 📁 files/                         # Sample attachments
```

## 🚀 Core Components

### 1. Config Base Module
Provides foundational APM configuration including:
- **APM Config Container**: Stores system configuration settings
- **Config Space**: Dedicated data model space for all config
- **Config Views**: Structured access to configuration data

### 2. Location Module
Establishes location hierarchy for asset organization:
- **Location Filters**: Define location boundaries
- **Location Nodes**: Hierarchical asset organization

### 3. Solution Model
Complete Maintain data model including:
- **Work Orders**: Maintenance order tracking
- **Comments**: Task comments and notes
- **Optimizations**: Performance improvements and tracking
- **3D Mappings**: Manual 3D asset annotations
- **Canvas & Layout**: UI configuration persistence
- **Planning**: Maintenance schedule planning

### 4. Source Data Model
Template for integrating source system data:
- **Maintenance Orders**: Source system work order mapping

### 5. Sample Data
Ready-to-load sample records to validate your setup:
- **Sample activities and assets**
- **Test documents and attachments**

## 🔧 Configuration

### Module Configuration (`default.config.yaml`)

```yaml
# Data Model Configuration
schemaSpace: sp_maintain_quickstart
instanceSpace: maintain_instances
dataset: maintain

# Naming
organization: CDM
```

Each module includes its own `default.config.yaml` that you should customize for your environment:
- `schemaSpace`: Where data models are deployed
- `instanceSpace`: Where instances are stored
- `dataset`: CDF dataset for all data
- `organization`: Prefix for your organization's naming

## 🏃‍♂️ Getting Started

### 1. Prerequisites

- CDF project with Data Modeling enabled
- Appropriate permissions for:
  - Data model deployment
  - Instance creation
  - File uploads (for sample data)

### 2. Configure the Package

Update your environment config with module variables:

```yaml
variables:
  modules:
    cdf_maintain_config_base:
      schemaSpace: sp_maintain_quickstart
      instanceSpace: maintain_instances
      dataset: maintain
    cdf_maintain_location:
      schemaSpace: sp_maintain_quickstart
      instanceSpace: maintain_instances
    cdf_maintain_solution_model:
      schemaSpace: sp_maintain_quickstart
      instanceSpace: maintain_instances
    cdf_maintain_source_data_model:
      schemaSpace: sp_maintain_quickstart
      instanceSpace: maintain_instances
    cdf_sample_data:
      dataset: maintain
      instanceSpace: maintain_instances
```

### 3. Deploy the Package

Deploy using the CDF Toolkit. See [CDF Toolkit Usage Guide](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/usage) for deployment instructions.

## 🔧 Customization

### Extend the Data Model

Add new work order properties by editing `cdf_maintain_solution_model`:

```yaml
# In views/data_modeling/MaintainWorkOrder.View.yaml
properties:
  - name: customField
    type: TEXT
    description: Your custom property
```

### Add Sample Data

Extend `cdf_sample_data` with your own records:

```yaml
# In data_modeling/your_records.Node.yaml
externalId: custom_asset_01
data:
  name: Custom Asset
  location: Field-A
```

### Adjust Location Hierarchy

Modify `cdf_maintain_location` to match your organization:
- Edit location filters in `locations/`
- Adjust node structure in `nodes/`
- Customize hierarchy for your needs

## 📄 License

This module is part of the Cognite Templates repository and follows the same licensing terms.
