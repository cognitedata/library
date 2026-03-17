# CDF Search Module

This module configures location filters for Cognite Data Fusion search functionality, enabling users to discover and search resources within specific organizational scopes based on data models and instance spaces.

## Why Use This Module?

**Enable Scoped Search for Your Industrial Data**

Large CDF deployments can contain millions of resources across multiple sites. This module delivers **location-based filtering** that helps users find relevant data within their organizational context.

**Key Benefits:**

- 🎯 **Scoped Discovery**: Filter resources by location/site for focused search
- 🏗️ **Data Model Aware**: Integrates with your process industry data model
- ⚡ **Quick Setup**: Single configuration file defines search scope
- 🔐 **Access Control**: Respects user permissions on designated spaces

## 🎯 Overview

The CDF Search module is designed to:
- **Define location filters** for scoped resource discovery
- **Configure data model integration** for search functionality
- **Specify instance spaces** users can search within
- **Enable site-specific views** of industrial data

## 🏗️ Module Architecture

```
cdf_search/
├── 📁 locations/                           # Location filter definitions
│   └── 📄 user.LocationFilter.yaml                # Location filter configuration
├── 📄 default.config.yaml                  # Module configuration
└── 📄 module.toml                          # Module metadata
```

## 🚀 Core Components

### Location Filter

**Purpose**: Defines searchable scope for users based on location

**Key Features**:
- 🎯 **Data Model Binding**: Links to specific data model and version
- 📍 **Instance Space Scoping**: Restricts search to designated spaces
- 📝 **Descriptive Naming**: Clear location names for user understanding

## 🔧 Configuration

### Module Configuration (`default.config.yaml`)

```yaml
location: springfield                       # Location identifier
locationName: Springfield                   # Display name
organization: ORG                           # Organization prefix
schemaSpace: sp_enterprise_process_industry # Data model space
datamodelVersion: v1.0                      # Data model version
instanceSpace: springfield_instances        # Instance space for search
```

### Location Filter Definition

```yaml
externalId: {{ location }}_location_filter
name: {{ locationName }}
description: Shows all resources in {{ locationName }} for users with access
dataModels:
  - externalId: {{ organization }}ProcessIndustries
    space: {{ schemaSpace }}
    version: {{ datamodelVersion }}
instanceSpaces:
  - {{ instanceSpace }}
```

## 🏃‍♂️ Getting Started

### 1. Prerequisites

- CDF project with search functionality enabled
- Data model deployed in target space
- Instance space with data populated

### 2. Configure the Module

Update your `config.<env>.yaml` under the module variables section:

```yaml
variables:
  modules:
    cdf_search:
      location: your_location              # Location identifier
      locationName: Your Location          # Display name
      organization: YOUR_ORG               # Organization prefix
      schemaSpace: sp_enterprise_process_industry
      datamodelVersion: v1.0
      instanceSpace: your_instances        # Instance space for search
```

### 3. Deploy the Module

```bash
cdf deploy --env your-environment
```

## 🎯 Use Cases

### Multi-Site Organizations
- **Site-Specific Views**: Users see only their site's resources
- **Cross-Site Search**: Administrators can search across all locations
- **Regional Grouping**: Organize by geographic or organizational boundaries

## 📄 License

This module is part of the Cognite Templates repository and follows the same licensing terms.

