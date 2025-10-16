# Out-of-the-Box Agents Module

This module provides pre-configured Atlas AI agents that can be deployed as part of your toolkit to give users immediate access to powerful data exploration capabilities.

## Overview

Adding this folder during toolkit deployment provides your project with ready-to-use agents that demonstrate key capabilities of Atlas AI integrated with Cognite Data Fusion. These agents serve as both functional tools and examples of how to build effective AI assistants for industrial data scenarios.


## Included Agents

### Industrial Data Explorer Agent

The `industrial_data_explorer` agent included in this module is a basic data exploration assistant that showcases how to retrieve and interact with core industrial data types from your Cognite Core data model:

#### Capabilities

- **Assets**: Query and explore asset hierarchies and properties
- **Equipment**: Find and analyze equipment information
- **Time Series**: Discover and examine time series data
- **Files**: Search and access documents and files

#### Use Cases

- Data discovery and exploration for new users
- Quick asset lookups and information retrieval
- Understanding data model structure and relationships
- Educational tool for demonstrating Atlas AI capabilities

### Timeseries Agent

The `timeseries_agent` is a specialized agent focused on time series data analysis. This agent goes beyond basic discovery to provide deep analytical capabilities for time series data:

#### Capabilities
- **Time Series Discovery**: Find and identify time series by name or other attributes
- **Data Point Analysis**: Query actual time series data points and values
- **Data Analysis**: Perform analysis on time series data to identify trends, patterns, and insights

#### Use Cases
- Detailed time series data analysis and trending
- Historical data exploration and pattern identification
- Time series data point retrieval for specific time periods
- Advanced analytics on industrial sensor and measurement data

## Prerequisites

**Atlas AI must be enabled** on the project where you are deploying this toolkit. Without Atlas AI enabled, the agents will not function.

**Alpha flags configuration**: To deploy agents through the toolkit, you must add `agents = true` to the `[alpha_flags]` section in your `cdf.toml` configuration file.

## Deployment

When deploying your toolkit:

1. Include this `ootb_agents` module in your deployment
2. The agents will be automatically configured and available to users
3. Users can immediately start exploring data without additional setup

## Customization

You can customize the included agents or add additional out-of-the-box agents by:

1. Modifying the existing agent configurations
2. Adding new agent YAML files to the `agents/` folder
3. Updating agent descriptions, instructions, or tools to match your specific use cases

## Getting Started

Once deployed, users can interact with the agents:

**Industrial Data Explorer Agent:**

- Ask questions like "Find asset 23-LT-96182-02"
- Explore time series data with queries like "Find time series VAL_23-KA-9101-M01_E_stop_active:VALUE"
- Search for equipment and files using natural language queries

**Timeseries Agent:**
- Find specific time series with queries like "Find time series VAL_23-KA-9101-M01_E_stop_active:VALUE and analyze the data points"
- Analyze historical data patterns and trends
- Retrieve and examine specific data point values over time

This provides an immediate, low-barrier entry point for users to experience the power of Atlas AI with their industrial data. 