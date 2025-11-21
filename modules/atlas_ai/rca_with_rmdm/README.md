# Root Cause Analysis (RCA) Agents Module

## Overview

The Root Cause Analysis (RCA) module provides intelligent Atlas AI agents for advanced root cause analysis capabilities in your Cognite Data Fusion (CDF) environment. This module is designed to work with the RMDM (Reliability Maintenance Data Model) v1 data model deployed in CDF.

## Getting Started

### Installation with Cognite Toolkit

To access this deployment pack through the Cognite Toolkit, you need to add the following configuration to your `cdf.toml` file:

```toml
[alpha_flags]
external-libraries = true

[library.cognite]
url = "https://github.com/cognitedata/library/raw/refs/heads/packages-menu/packages.zip"
checksum = "sha256:78d67c3a7079e5019027aa48939a0e2aab04231fd928879571c89fbb354f3f0b"
```

Once you've added this configuration, you can find and initialize this module by running:

```bash
cdf modules init .
```

The Atlas AI Deployment Pack (which includes this RCA with RMDM module) will be available for selection.

## Dependencies

This module requires the **data_models/rmdm_v1/** module to be deployed in your CDF project. All agents in this module connect to and query the RMDM data model within CDF to access equipment, failure notifications, time series, and other maintenance-related data.

## Agents

This module contains three specialized Atlas AI agents that work together to provide comprehensive root cause analysis capabilities:

### 1. Cause Map Agent (`cause_map_agent.Agent.yaml`)

The Cause Map Agent helps users generate visual cause maps for equipment failures.

**What it does:**
- Finds equipment and retrieves its latest failure notification, failure mode, and equipment class
- Generates a structured cause map showing the relationships between failure modes, failure mechanisms, root cause categories, and specific root causes
- Can work with either the latest high-priority failure notification or the most common failure mode for a piece of equipment
- Automatically builds and displays the cause map on the canvas, expanding the top 3 failure mechanisms with the highest failure rates
- Queries the RMDM data model for Equipment, FailureNotification, FailureMode, and EquipmentClass views

### 2. RCA Agent (`rca_agent.Agent.yaml`)

The RCA Agent is the main agent for conducting comprehensive root cause analysis investigations.

**What it does:**
- Guides users through a complete RCA workflow for failing equipment or assets
- Retrieves multiple types of data from the RMDM knowledge graph including:
  - Assets and their hierarchical relationships (parent, children, siblings)
  - Maintenance orders and work orders (corrective and preventive)
  - Failure notifications
  - Related documents and files (P&IDs, technical documentation)
  - Images
  - Time series metadata
- Acts as a maintenance professional and RCA expert, asking proactive follow-up questions
- Provides objective information and analysis without making assumptions or hallucinating
- Helps identify the most common failures and related patterns

### 3. Time Series Agent (`ts_agent.Agent.yaml`)

The Time Series Agent specializes in retrieving and analyzing time series data for equipment.

**What it does:**
- Retrieves time series data from the RMDM knowledge graph for assets
- Finds time series for related assets (children, siblings, or parent assets)
- Plots and visualizes time series data to identify trends, patterns, and anomalies
- Performs statistical analysis including computing averages with optional filtering
- Provides insights and recommendations based on time series analysis
- Acts as a maintenance professional and data analyst expert
- Guides users through time series analysis workflows within the industrial domain

## How It Works

All three agents connect to the RMDM data model (space: `rmdm`, version: `v1`) deployed in your CDF environment. They use various tools including:
- Knowledge graph queries to retrieve data from RMDM views
- Python code execution for complex data processing and analysis
- Document Q&A for analyzing technical documentation
- Image analysis for visual inspection
- Time series data retrieval and computation

These agents work together to provide a complete root cause analysis experience, from identifying equipment and failures, to analyzing historical data, to generating visual cause maps that help identify the root causes of equipment failures. 