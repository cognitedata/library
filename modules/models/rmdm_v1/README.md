# RMDM v1 (Reliability Maintenance Data Model)

## Overview

The **RMDM v1 module** provides the foundational data model definitions required to implement a **Reliability Maintenance Data Model** in your **Cognite Data Fusion (CDF)** environment. It includes all necessary containers, views, and space configurations to establish a comprehensive framework for **maintenance and reliability management**.

This guide walks you through the concept of RMDM and the steps to download, configure, and deploy RMDM v1 using the **Cognite Toolkit**.

## What is RMDM?

**RMDM (Reliability Maintenance Data Model)** is a **domain-specific data model** introduced by Cognite to structure and unify reliability and maintenance-related data in **Cognite Data Fusion (CDF)**.

It extends Cognite's **common data models (CDM, IDM)** to include **reliability-focused standards**, ensuring that maintenance and troubleshooting processes are supported with consistent, high-quality data.

RMDM is built to be **ISO 14224** and **NORSOK Z-008** compliant, which are globally recognized standards for equipment reliability, failure modes, and maintenance management.

## Why RMDM is Needed?

- Maintenance programs rely on **static or incomplete data**, leading to inefficiencies.
- Data is **siloed** across multiple systems (CMMS, DCS, Aveva, etc.), making it hard to transition from calendar-based to **condition-based maintenance (CBM)**.
- RCAs (Root Cause Analyses) typically require **weeks of manual data gathering** due to the lack of standardized structures.
- Poor standardization and naming conventions hinder data unification and collaboration.

**RMDM solves this by providing a common foundation for structuring and classifying data, enabling faster, more effective RCA and troubleshooting.**

## Module Components

The RMDM v1 module is structured as follows:

- **data_models/** – Core RMDM data model definitions
- **containers/** – Container definitions for all RMDM entities
- **views/** – View definitions for all RMDM entities
- **rmdm_v1.datamodel.yaml** – Main data model configuration
- **rmdm.space.yaml** – Space configuration for the RMDM data model

## Data Model Entities

The RMDM v1 defines a comprehensive set of entities, grouped into key categories:

### Core Asset Entities

- **Asset** – Physical or logical assets in the maintenance system
- **Equipment** – Specific equipment items requiring maintenance
- **EquipmentClass** – Classification system for equipment types
- **EquipmentFunction** – Functional categories for equipment
- **EquipmentType** – Specific types of equipment

### Maintenance Entities

- **MaintenanceOrder** – Work orders for maintenance activities
- **Notification** – System notifications and alerts

### Failure Analysis Entities

- **FailureNotification** – Notifications related to failures
- **FailureCause** – Root causes of equipment failures
- **FailureMechanism** – Mechanisms through which failures occur
- **FailureMode** – Specific modes of failure for equipment

### Extended Entities

- **File_ext** – Extended file metadata for maintenance documentation
- **Timeseries_ext** – Extended time series data for monitoring and analysis

## Related Modules

- **atlas_ai/rca_with_rmdm/** – Contains intelligent Atlas AI agents that work with this data model to provide RCA capabilities

## Deployment

### Prerequisites

Before you start, ensure you have the following:

- You already have a Cognite Toolkit project set up locally.
- Your project contains the standard `cdf.toml` file
- You have valid authentication to your target CDF environment

### Step 1: Enable External Libraries

Edit your project's `cdf.toml` and add the following configuration:

```toml
[alpha_flags]
external-libraries = true

[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:795a1d303af6994cff10656057238e7634ebbe1cac1a5962a5c654038a88b078"
```

This allows the Toolkit to retrieve official library packages, including RMDM.

> **📝 Note: Replacing the Default Library**
>
> By default, a Cognite Toolkit project contains a `[library.toolkit-data]` section pointing to `https://github.com/cognitedata/toolkit-data/...`. This provides core modules like Quickstart, SourceSystem, Common, etc.
>
> **These two library sections cannot coexist.** To use this Deployment Pack, you must **replace** the `toolkit-data` section with `library.cognite`:
>
> | Replace This | With This |
> |--------------|-----------|
> | `[library.toolkit-data]` | `[library.cognite]` |
> | `github.com/cognitedata/toolkit-data/...` | `github.com/cognitedata/library/...` |
>
> The `library.cognite` package includes all Deployment Packs developed by the Value Delivery Accelerator team (RMDM, RCA agents, Context Quality Dashboard, etc.).

> **⚠️ Checksum Warning**
>
> When running `cdf modules add`, you may see a warning like:
>
> ```
> WARNING [HIGH]: The provided checksum sha256:... does not match downloaded file hash sha256:...
> Please verify the checksum with the source and update cdf.toml if needed.
> This may indicate that the package content has changed.
> ```
>
> **This is expected behavior.** The checksum in this documentation may be outdated because it gets updated with every release. The package will still download successfully despite the warning.
>
> **To resolve the warning:** Copy the new checksum value shown in the warning message and update your `cdf.toml` with it. For example, if the warning shows `sha256:da2b33d60c66700f...`, update your config to:
>
> ```toml
> [library.cognite]
> url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
> checksum = "sha256:da2b33d60c66700f..."
> ```

### Step 2 (Optional but Recommended): Enable Usage Tracking

To help improve the Deployment Pack and provide insight to the Value Delivery Accelerator team, you can enable anonymous usage tracking:

```bash
cdf collect opt-in
```

This is optional, but highly recommended.

### Step 3: Add the Module

Run:

```bash
cdf modules init .
```
> **⚠️ Disclaimer**: This command will overwrite your existing modules in the current directory. Make sure to commit any changes before running this command, or use it in a fresh project directory.

This opens the interactive module selection interface.

### Step 4: Select the RMDM Data Models Package

From the menu, select:

```
Data models: Data models that extend the core data model
```

Follow the prompts. Toolkit will:

- Download the RMDM module
- Update the Toolkit configuration
- Place the files into your project

### Step 5: Verify Folder Structure

After installation, your project should now contain:

```
modules/
    └── data_models/
        └── rmdm_v1/
```

If you see this structure, RMDM has been successfully added to your project.

### Step 6: Deploy to CDF

Build and deploy as usual:

```bash
cdf build
```

```bash
cdf deploy --dry-run
```

```bash
cdf deploy
```

After deployment, the RMDM models, containers, and views will be available in your CDF environment.

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
├── module.toml
└── README.md                   # This file
```

## Support

For troubleshooting or deployment issues:

- Refer to the [Cognite Documentation](https://docs.cognite.com)
- Contact your **Cognite support team**
- Join the Slack channel **#topic-deployment-packs** for community support and discussions

## Cognite Hub Article

For more detailed information and the latest updates, visit the official Cognite Hub article:

[How to Deploy RMDM v1 (Reliability Maintenance Data Model) in Cognite Data Fusion](https://hub.cognite.com/deployment-packs-472/how-to-deploy-rmdm-v1-reliability-maintenance-data-model-in-cognite-data-fusion-5454)
