# CDM Maintain

Library path: `modules/solutions/cdm_maintain/` · Deployment pack: `dp:cdm_maintain`

This deployment pack provides a complete, ready-to-deploy configuration for Cognite Maintain. It includes everything needed to get maintenance operations, workflows, and asset contextualization set up.

## Why Use This Package?

**Pre-built Data Models & Configuration**

It includes pre-configured data models, views, containers, and foundational setup templates for Maintain.

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

The CDM Maintain package enables you to:
- **Deploy Maintain infrastructure** with pre-built data models and configurations
- **Organize maintenance data** with structured containers and views
- **Track work orders** with asset and location hierarchies
- **Configure asset locations** with custom hierarchies
- **Load sample data** to validate your setup quickly
- **Integrate source systems** with standardized data ingestion patterns

## 🏗️ Package Architecture

```
cdm_maintain/
├── 📁 cdf_maintain_config_base/          # Base Maintain configuration
│   ├── 📄 module.toml                    # Module metadata
│   ├── 📄 default.config.yaml            # Configuration
│   └── 📁 data_modeling/
│       ├── 📁 containers/                # Data model containers (APM Config)
│       ├── 📁 views/                     # Data model views
│       ├── 📄 *.Space.yaml               # Data model spaces
│       └── 📄 *.DataModel.yaml           # Data models
│
├── 📁 cdf_maintain_location/             # Location hierarchy setup
│   ├── 📄 module.toml
│   ├── 📄 default.config.yaml
│   ├── 📁 locations/                     # Location filters
│   └── 📁 data_modeling/
│       └── 📁 nodes/                     # Location nodes
│
├── 📁 cdf_maintain_solution_model/       # Maintain solution data model
│   ├── 📄 module.toml
│   ├── 📄 default.config.yaml
│   └── 📁 data_modeling/
│       ├── 📁 containers/                # Solution model containers
│       ├── 📁 views/                     # Solution views
│       ├── 📁 nodes/                     # Global node definitions
│       ├── 📄 *.Space.yaml               # Solution spaces
│       └── 📄 *.DataModel.yaml           # Solution data models
│
├── 📁 cdf_maintain_source_data_model/    # Source system data model
│   ├── 📄 module.toml
│   ├── 📄 default.config.yaml
│   └── 📁 data_modeling/
│       ├── 📁 containers/                # Source containers
│       ├── 📁 views/                     # Source views
│       ├── 📄 *.Space.yaml               # Source spaces
│       └── 📄 *.DataModel.yaml           # Source data models
│
└── 📁 cdf_sample_data/                   # Sample data for validation
    ├── 📄 module.toml
    ├── 📄 default.config.yaml
    ├── 📁 data_modeling/
    │   └── 📁 nodes/                     # Sample maintenance records and grid zone annotations
    └── 📁 upload_data/                   # Files uploaded via `cdf data upload` command
        ├── 📄 *.Manifest.yaml            # Upload manifests
        └── 📁 files/                     # Sample attachments
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
- **Grid zone annotations** (`gridZoneAnnotations.Node.yaml`) — maps zones on the sample P&ID to activities via `rootLocation` and `gridReference`
- **Test documents and attachments** (uploaded via `cdf data upload` command, manifests in `upload_data/`)

## 🔧 Configuration

### Module Configuration (`default.config.yaml`)

Each module's `default.config.yaml` declares the variables used by that module. The toolkit uses these to auto-populate your `config.<env>.yaml` when you download the modules from the library.

| Module | Variable | Default | Description |
|---|---|---|---|
| `cdf_maintain_solution_model` | `schemaSpace` | `maintain_solution_model` | Space for the solution data model |
| `cdf_maintain_solution_model` | `sourceDataSpace` | `maintain_source_data` | Source data space (cross-module ref) |
| `cdf_maintain_source_data_model` | `schemaSpace` | `maintain_source_data` | Space for the source data model |
| `cdf_maintain_location` | `location` | *(required)* | LocationFilter externalId |
| `cdf_maintain_location` | `location_name` | *(required)* | Display name shown in the app |
| `cdf_maintain_location` | `appDataSpace` | `maintain_solution_model` | Solution model space (cross-module ref) |
| `cdf_maintain_location` | `sourceDataSpace` | `maintain_source_data` | Source data space (cross-module ref) |
| `cdf_sample_data` | `instanceSpace` | `maintain_source_data` | Space where sample instances are written |
| `cdf_sample_data` | `appDataSpace` | `maintain_solution_model` | Space for grid zone annotation nodes (must match `cdf_maintain_location`) |
| `cdf_sample_data` | `location` | *(required)* | Root asset externalId prefix (must match `cdf_maintain_location`) |
| `cdf_sample_data` | `location_name` | *(required)* | Human-readable location name used in gridzone and activity data (must match `cdf_maintain_location`) |

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
    cdf_maintain_solution_model:
      schemaSpace: maintain_solution_model
      sourceDataSpace: maintain_source_data
    cdf_maintain_source_data_model:
      schemaSpace: maintain_source_data
    cdf_maintain_location:
      location: <your_location_id>
      location_name: <Your Location Name>
      appDataSpace: maintain_solution_model
      sourceDataSpace: maintain_source_data
    cdf_sample_data:
      instanceSpace: maintain_source_data
      appDataSpace: maintain_solution_model
      location: <your_location_id>
      location_name: <Your Location Name>
```

### 3. Add and deploy the package

```bash
cdf modules add dp:cdm_maintain
cdf build
cdf deploy --env your-environment
```

See the [Cognite Toolkit usage guide](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/usage) for more detail.

## 🔧 Customization

### Extend the Data Model

Add new work order properties by editing `cdf_maintain_solution_model`:

```yaml
# In cdf_maintain_solution_model/data_modeling/views/MaintainWorkOrder.View.yaml
properties:
  - name: customField
    type: TEXT
    description: Your custom property
```

### Add Sample Data

Extend `cdf_sample_data` with your own records:

```yaml
# In cdf_sample_data/data_modeling/nodes/your_records.Node.yaml
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

This module is part of the [Cognite library](https://github.com/cognitedata/library) repository and follows the same licensing terms.
