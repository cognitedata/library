# RMDM (Reliability Maintenance Data Model)

Library path: `modules/data_models/rmdm/` · Module ID: `dp:models:rmdm` · Deployment pack: `dp:models`

## Overview

The **RMDM module** provides the foundational data model definitions required to implement a **Reliability Maintenance Data Model** in your **Cognite Data Fusion (CDF)** environment. It includes all necessary containers, views, and space configurations to establish a comprehensive framework for **maintenance and reliability management**.

This guide walks you through the concept of RMDM and the steps to download, configure, and deploy RMDM using the **Cognite Toolkit**.

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

The RMDM module is structured as follows:

- **data_models/** – Core RMDM data model definitions
- **containers/** – Container definitions for all RMDM entities
- **views/** – View definitions for all RMDM entities
- **rmdm_v1.datamodel.yaml** – Main data model configuration
- **rmdm.space.yaml** – Space configuration for the RMDM data model

## Data Model Entities

RMDM defines a comprehensive set of entities, grouped into key categories:

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

## Deployment

### Prerequisites

- **Cognite Toolkit 0.7.210 or above** (`cdf --version` to check).
- A CDF project with valid authentication configured for your target environment.
- A `cdf.toml` in your Toolkit project directory.

### Choose your setup path

---

### 1. Existing Toolkit project

Use this path if you already have a local Toolkit project and only want to add RMDM.

#### a. Point `cdf.toml` at the Cognite Library

Long-time Toolkit users may still have `[library.cognite]` pointing at **toolkit-data**. Update it to the official library release:

```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
```

#### b. Enable deployment packs in `cdf.toml`

In the same `cdf.toml`, ensure the alpha flag is set:

```toml
[alpha_flags]
deployment-pack = true
```

#### c. Add the RMDM module

From your project directory:

```bash
cdf modules add -d rmdm
```

#### d. Build and deploy

Run in order:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

After deployment, the RMDM models, containers, and views are available in your CDF environment.

#### Verify folder structure

Your project should include:

```
modules/
    └── data_models/
        └── rmdm/
```

---

### 2. Starting from scratch

Use this path for a new Toolkit project with only RMDM (and the data models pack scaffolding Toolkit creates).

#### a. Initialize the project

In an **empty directory**:

```bash
cdf modules init .
```

#### b. Select RMDM in the interactive UI

In the terminal:

1. Choose **Data models** (data models that extend the core data model).
2. Use **Space** to select **rmdm**.
3. Press **Enter** to confirm.

Toolkit downloads the library, creates the module under `modules/data_models/rmdm/`, and updates `config.dev.yaml` / `config.prod.yaml`.

#### c. Build and deploy

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

---

After either path, the RMDM data model, containers, and views are deployed to your CDF environment.

## Module Structure

```
rmdm/
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

- **[Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/index)** — setup, `cdf modules` / `cdf build` / `cdf deploy`, and [working with modules](https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/packages/index)
- **[Cognite Support (Zendesk)](https://cognite.zendesk.com/hc/en-us)** — product support and tickets for CDF and Toolkit
- **#topic-deployment-packs** (Slack, internal) — deployment-pack community discussions at Cognite

## Cognite Hub Article

For more detailed information and the latest updates, visit the official Cognite Hub article:

[How to Deploy RMDM (Reliability Maintenance Data Model) in Cognite Data Fusion](https://hub.cognite.com/deployment-packs-472/how-to-deploy-rmdm-v1-reliability-maintenance-data-model-in-cognite-data-fusion-5454)
