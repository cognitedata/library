# RMDM v1 (Reliability Maintenance Data Model)

## Overview

The RMDM v1 module provides the core data model definitions for implementing Reliability Maintenance Data Model in your Cognite Data Fusion (CDF) environment. This module contains all the necessary containers, views, and space configurations to establish a comprehensive maintenance and reliability data framework.

## Module Components

This module contains:
- **data_models/**: Core RMDM data model definitions
  - `containers/`: Data model container definitions for all RMDM entities
  - `views/`: Data model view definitions for all RMDM entities
  - `rmdm_v1.datamodel.yaml`: Main data model configuration
  - `rmdm.space.yaml`: Space configuration for the RMDM data model

## Data Model Entities

The RMDM v1 data model includes the following key entities:

### Core Asset Entities
- **Asset**: Physical or logical assets in the maintenance system
- **Equipment**: Specific equipment items that require maintenance
- **EquipmentClass**: Classification system for equipment types
- **EquipmentFunction**: Functional categories for equipment
- **EquipmentType**: Specific types of equipment

### Maintenance Entities
- **MaintenanceOrder**: Work orders for maintenance activities
- **Notification**: System notifications and alerts

### Failure Analysis Entities
- **FailureNotification**: Notifications specifically related to failures
- **FailureCause**: Root causes of equipment failures
- **FailureMechanism**: Mechanisms through which failures occur
- **FailureMode**: Specific modes of failure for equipment

### Extended Entities
- **File_ext**: Extended file metadata for maintenance documentation
- **Timeseries_ext**: Extended time series data for monitoring and analysis

## Related Modules

- **root_cause_analysis/**: Contains intelligent agents that work with this data model to provide RCA capabilities

## Deployment

This document outlines the steps to deploy the Reliability Maintenance Data Model (RMDM) using the Cognite Toolkit. This process involves integrating this GitHub repository module into your local Toolkit setup and ensuring your configuration is correctly set up for deployment.

### Prerequisites
- Basic understanding of Git and GitHub
- Familiarity with the Cognite Data Fusion (CDF) Toolkit
- Access to your CDF project
- `cognite-toolkit` version >= 0.5.54

### Step 1: Download the RMDM Module from GitHub

The first step is to obtain the RMDM module files from the specified GitHub repository.

1. Navigate to your desired local directory where you manage your CDF Toolkit projects.

2. Download the folder `rmdm_v1` from the following GitHub URL: 
   ```
   https://github.com/cognitedata/templates/tree/main/templates/modules/rmdm_v1
   ```

You have a few options for downloading:

#### Option A: Git Clone (Recommended for ongoing updates)
If you plan to keep this repository synchronized or contribute, clone the entire templates repository and then navigate to the specific module.

```bash
git clone https://github.com/cognitedata/templates.git
```

After cloning, you will find the folder at `templates/modules/rmdm_v1`.

#### Option B: Direct Download (For one-time setup)
1. Go to the GitHub URL in your browser
2. Click the "Code" button (usually green)
3. Select "Download ZIP"
4. After downloading and unzipping, locate the `templates/modules/rmdm_v1` path within the unzipped folder and extract the `rmdm_v1` folder

**Result:** You should now have a folder named `rmdm_v1` containing the RMDM module files on your local machine.

### Step 2: Add the Downloaded Folder to Your Toolkit Modules

Now, integrate the downloaded RMDM module into your local Cognite Toolkit setup.

1. Locate your local Cognite Toolkit project directory. This is typically where your cognite-toolkit files are stored, and you'll find a `modules/` folder within it.

   > **Note:** If you don't have an existing Toolkit project, you might need to initialize one or use an existing one where you manage your CDF deployments.

2. Move or copy the `rmdm_v1` folder (from Step 1) into the `modules/` directory (or a subfolder of it) in your local Cognite Toolkit setup.

Your directory structure should now look something like this:

```
├── modules/
│   └── rmdm_v1/
│       └── data_models/
├── config.dev.yml
└── ...
```

### Step 3: Verify Your Cognite Data Fusion (CDF) Toolkit Version

It's crucial to ensure your `cognite-toolkit` version meets the minimum requirement for deploying this module.

1. Open your terminal or command prompt.

2. Run the following command to check your installed `cognite-toolkit` version:
   ```bash
   cdf --version
   ```

3. Ensure the reported version is `>=0.5.54`.

4. If your version is older, you need to upgrade. Use pip to upgrade:
   ```bash
   pip install --upgrade cognite-toolkit
   ```

   > **Note:** If you are using Poetry or another package manager, refer to its documentation for upgrading packages.

### Step 4: Configure Your Deployment for the New Module

Finally, you need to instruct your Toolkit deployment to include the newly added RMDM module. This is typically done through your `config.dev.yml` file for local deployments or a GitHub Deploy file (e.g., a GitHub Actions workflow) for CI/CD pipelines.

### Next Steps: Running the Deployment

Once you have completed these setup steps, you can proceed with deploying the RMDM by either running the `cdf build` and `cdf deploy` commands from your terminal in your project's root directory or do a `git push` to deploy with GitHub Actions.

## Module Structure

```
rmdm_v1/
├── data_models/
│   ├── containers/
│   │   ├── Asset.container.yaml
│   │   ├── Equipment.container.yaml
│   │   ├── EquipmentClass.container.yaml
│   │   ├── EquipmentFunction.container.yaml
│   │   ├── EquipmentType.container.yaml
│   │   ├── FailureCause.container.yaml
│   │   ├── FailureMechanism.container.yaml
│   │   ├── FailureMode.container.yaml
│   │   ├── FailureNotification.container.yaml
│   │   ├── File_ext.container.yaml
│   │   ├── MaintenanceOrder.container.yaml
│   │   ├── Notification.container.yaml
│   │   └── Timeseries_ext.container.yaml
│   ├── views/
│   │   ├── Asset.view.yaml
│   │   ├── Equipment.view.yaml
│   │   ├── EquipmentClass.view.yaml
│   │   ├── EquipmentFunction.view.yaml
│   │   ├── EquipmentType.view.yaml
│   │   ├── FailureCause.view.yaml
│   │   ├── FailureMechanism.view.yaml
│   │   ├── FailureMode.view.yaml
│   │   ├── FailureNotification.view.yaml
│   │   ├── File_ext.view.yaml
│   │   ├── MaintenanceOrder.view.yaml
│   │   ├── Notification.view.yaml
│   │   └── Timeseries_ext.view.yaml
│   ├── rmdm_v1.datamodel.yaml
│   └── rmdm.space.yaml
└── README.md                   # This file
```

## Prerequisites

- Cognite Data Fusion (CDF) project access
- `cognite-toolkit` version >= 0.5.54
- Proper authentication and permissions for data model deployment

## Support

For issues related to this data model or deployment questions, please refer to the Cognite documentation or contact your Cognite support team.
