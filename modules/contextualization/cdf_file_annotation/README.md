# Cognite Data Model-Based Annotation Module

## Overview

The Annotation template is a framework designed to automate the process of annotating files within Cognite Data Fusion (CDF). It uses a data model-centric approach to manage the annotation lifecycle, from selecting files to processing results and generating reports. This template is configurable, allowing it to adapt to various data models and annotation requirements.

## Key Features

- **Configuration-Driven Workflow:** The entire process is controlled by a single config.yaml file, allowing adaptation to different data models and operational parameters without code changes.
- **Dual Annotation Modes**: Simultaneously runs standard entity matching and pattern-based detection mode:
  - **Standard Mode**: Links files to known entities in your data model with confidence-based approval thresholds.
  - **Pattern Mode**: Automatically generates regex-like patterns from entity aliases and detects all matching text in files, creating a comprehensive searchable catalog of potential entities for review and approval.
- **Intelligent Pattern Generation:** Automatically analyzes entity aliases to generate pattern samples, with support for manual pattern overrides at global, site, or unit levels.
- **Large Document Support (\>50 Pages):** Automatically handles files with more than 50 pages by breaking them into manageable chunks, processing them iteratively, and tracking the overall progress.
- **Parallel Execution Ready:** Designed for concurrent execution with a robust optimistic locking mechanism to prevent race conditions when multiple finalize function instances run in parallel.
- **Comprehensive Reporting:** Annotations stored in three dedicated RAW tables (doc-to-doc links, doc-to-tag links, and pattern detections) plus extraction pipeline logs for full traceability.
- **Local Running and Debugging:** Both the launch and finalize handler can be run locally and have default setups in the 'Run & Debug' tab in VSCode. Requires a .env file to be placed in the directory.

## Getting Started

Ready to deploy? Check out the **[Deployment Guide](DEPLOYMENT.md)** for step-by-step instructions on:

- Prerequisites and data preparation requirements
- CDF Toolkit setup
- Module integration and configuration
- Local development and debugging

For a quick overview, deploying this annotation module into a new Cognite Data Fusion (CDF) project is a streamlined process. Since all necessary resources (Data Sets, Extraction Pipelines, Functions, etc.) are bundled into a single module, you only need to configure one file to get started.

## How It Works

The template operates in three main phases, orchestrated by CDF Workflows. Since the prepare phase is relatively small, it is bundled in with the launch phase. However, conceptually it should be treated as a separate process.

### Prepare Phase

- **Goal**: Identify files that need to be annotated or have their status reset.
- **Process**:
  1.  It queries for files that are marked for re-annotation and resets their status.
  2.  It then queries for new files tagged for annotation (e.g., with a "ToAnnotate" tag).
  3.  For each new file, it creates a corresponding `AnnotationState` instance in the data model, marking it with a "New" status.

### Launch Phase

![LaunchService](https://github.com/user-attachments/assets/3e5ba403-50bb-4f6a-a723-be8947c65ebc)

- **Goal**: Launch the annotation jobs for files that are ready.
- **Process**:
  1.  It queries for `AnnotationState` instances with a "New" or "Retry" status.
  2.  It groups these files by a primary scope (e.g., site, unit) to provide operational context.
  3.  For each group, it fetches the relevant file and target entity information using an intelligent caching system:
      - Checks if a valid cache exists in RAW (based on scope and time limit).
      - If cache is stale or missing, queries the data model for entities within scope.
      - Automatically generates pattern samples from entity aliases (e.g., "FT-101A" → "[FT]-000[A]").
      - Retrieves manual pattern overrides from RAW catalog (GLOBAL, site-level, or unit-level).
      - Merges and deduplicates auto-generated and manual patterns.
      - Stores the combined entity list and pattern samples in RAW cache for reuse.
  4.  It calls the Cognite Diagram Detect API to initiate two async jobs:
      - A `standard annotation` job to find and link known entities with confidence scoring.
      - A `pattern mode` job (if enabled) to detect all text matching the pattern samples, creating a searchable reference catalog.
  5.  It updates the `AnnotationState` instance with both the `diagramDetectJobId` and `patternModeJobId` (if applicable) and sets the overall `annotationStatus` to "Processing".

### Finalize Phase

![FinalizeService](https://github.com/user-attachments/assets/152d9eaf-afdb-46fe-9125-11430ff10bc9)

- **Goal**: Retrieve, process, and store the results of completed annotation jobs.
- **Process**:
  1.  It queries for `AnnotationState` instances with a "Processing" or "Finalizing" status (using optimistic locking to claim jobs).
  2.  It waits until both the standard and pattern mode jobs for a given file are complete.
  3.  It retrieves and processes the results from both jobs:
      - Creates a stable hash for each detection to enable deduplication between standard and pattern results.
      - Filters standard annotations by confidence thresholds (auto-approve vs. suggest).
      - Skips pattern detections that duplicate standard annotations.
  4.  It optionally cleans old annotations first (on first run for multi-page files), then:
      - **Standard annotations**: Creates edges in the data model linking files to specific entities, writes results to RAW tables (`doc_tag` for assets, `doc_doc` for file-to-file links).
      - **Pattern annotations**: Creates edges linking files to a configurable "sink node" for review, writes results to a dedicated `doc_pattern` RAW table for the searchable catalog.
  5.  Updates the file node tag from "AnnotationInProcess" to "Annotated".
  6.  Updates the `AnnotationState` status to "Annotated", "Failed", or back to "New" (if more pages remain), tracking page progress for large files.

## Configuration

The template's behavior is entirely controlled by the `ep_file_annotation.config.yaml` file. This YAML file is parsed by Pydantic models in the code, ensuring a strongly typed and validated configuration.

Key configuration sections include:

- `dataModelViews`: Defines the data model views for files, annotation states, core annotations, and target entities.
- `prepareFunction`: Configures the queries to find files to annotate and optionally reset.
- `launchFunction`: Sets parameters for the annotation job:
  - `batchSize`: Maximum files per diagram detect call (1-50).
  - `patternMode`: Boolean flag to enable pattern-based detection alongside standard matching.
  - `primaryScopeProperty` / `secondaryScopeProperty`: Properties used for batching and cache scoping (e.g., "site", "unit").
  - `cacheService`: Configuration for entity cache storage and time limits.
  - `annotationService`: Diagram detect parameters including `pageRange` for multi-page file processing.
- `finalizeFunction`: Defines how to process and apply the final annotations:
  - `autoApprovalThreshold` / `autoSuggestThreshold`: Confidence thresholds for standard annotations.
  - `cleanOldAnnotations`: Whether to remove existing annotations before applying new ones.
  - `maxRetryAttempts`: Retry limit for failed files.
  - `sinkNode`: Target node for pattern mode annotations pending review.

This file allows for deep customization. For example, you can use a list of query configurations to combine them with `OR` logic, or you can set `primaryScopeProperty` to `None` to process files that are not tied to a specific scope. Manual pattern samples can be added to the RAW catalog at `GLOBAL`, site, or unit levels to override or supplement auto-generated patterns.

## Documentation

This README provides a high-level overview of the template's purpose and architecture. For more detailed information:

### Deployment & Setup

- **[Deployment Guide](DEPLOYMENT.md)**: Step-by-step instructions for deploying to CDF, including prerequisites, configuration, and local debugging setup.

### Configuration & Usage

- **[CONFIG.md](detailed_guides/CONFIG.md)**: Comprehensive guide to the `ep_file_annotation.config.yaml` file and all configuration options.
- **[CONFIG_PATTERNS.md](detailed_guides/CONFIG_PATTERNS.md)**: Recipes for common operational tasks, including processing specific subsets, reprocessing files, and performance tuning.

### Development & Extension

- **[DEVELOPING.md](detailed_guides/DEVELOPING.md)**: Guide for developers extending the template's functionality, including the interface-based architecture and how to create custom service implementations.

### Contributing

- **[CONTRIBUTING.md](CONTRIBUTING.md)**: Guidelines for contributing to this project, including the issue/PR workflow, code standards, and review process.

## Design Philosophy

There were two principles I kept in mind when designing this template.

- **Evolving Needs:** Project requirements evolve. A simple, plug-and-play tool is great to start with, but it can hit limitations when faced with demands for scale, performance, or specialized logic—as was the case with previous annotation templates when applied to projects with tens of thousands of complex files. My belief is that a modern template must be built to be extended.

- **The Balance Between Configuration and Code:** This template is architected to provide two primary modes of adaptation, striking a crucial balance:

  1.  **Quick Start (via Configuration):** For the majority of use cases, a user should only need to edit the `config.yaml` file. By defining their data model views and tuning process parameters, they can get the template running quickly and effectively.
  2.  **Scaling (via Interfaces):** When a project demands unique optimizations—like a non-standard batching strategy or a complex query to fetch entities—the interface-based design provides the necessary "escape hatch." A developer can write a custom Python class to implement their specialized logic, ensuring the template can meet any future requirement.

## Architecture & Optimizations

This section explains some of the core design choices made to ensure the template is robust and scalable.

### Stateful Processing with Data Models

Instead of using a simpler store like a RAW table to track the status of each file, this module uses a dedicated `AnnotationState` Data Model. There is a 1-to-1 relationship between a file being annotated and its corresponding `AnnotationState` instance. This architectural choice is deliberate and crucial for reliability:

- **Concurrency:** Data Model instances have built-in optimistic locking via the `existing_version` field. When multiple parallel functions attempt to "claim" a job, only the first one can succeed in updating the `AnnotationState` instance. All others will receive a version conflict error. This database-level locking is far more reliable and simpler to manage than building a custom locking mechanism on top of RAW.
- **Query Performance:** Finding all files that need processing (e.g., status is "New" or "Retry") is a fast, indexed query against the Data Model. Performing equivalent filtering on potentially millions of rows in a RAW table would be significantly slower and less efficient.
- **Schema Enforcement and Data Integrity:** The `AnnotationState` view enforces a strict schema for state information (`status`, `attemptCount`, `annotatedPageCount`, etc.), ensuring data consistency across the entire process. RAW tables offer no schema guarantees.
- **Discoverability and Governance:** The state of the annotation pipeline is exposed as a first-class entity in the CDF data catalog. This makes it easy to monitor progress, build dashboards, and govern the data lifecycle, which is much more difficult with state hidden away in RAW rows.

### Optimized Batch Processing & Caching

When processing tens of thousands of files, naively fetching context for each file is inefficient. This module implements a significant optimization based on experiences with large-scale projects.

- **Rationale:** For many projects, the entities relevant to a given file are often co-located within the same site or operational unit. By grouping files based on these properties before processing, we can create a highly effective cache.
- **Implementation:** The `launchFunction` configuration allows specifying a `primary_scope_property` and an optional `secondary_scope_property`. The `LaunchService` uses these properties to organize all files into ordered batches. For each unique scope combination:

  1. Check if a valid cache exists in RAW (scoped by primary/secondary values and time limit).
  2. If stale or missing, query the data model for all relevant entities within that scope.
  3. Transform entities into the format required by diagram detect.
  4. Automatically generate pattern samples by analyzing entity alias properties.
  5. Retrieve and merge manual pattern overrides from the RAW catalog.
  6. Store the complete entity list and pattern samples in RAW for reuse.

  This cache is loaded once per scope and reused for all files in that batch, drastically reducing the number of queries to CDF and improving overall throughput. The pattern generation process extracts common naming conventions from aliases, creating regex-like patterns that can match variations (e.g., detecting "FT-102A" even if only "FT-101A" was in the training data).

### Interface-Based Extensibility

The template is designed around a core set of abstract interfaces (e.g., `IDataModelService`, `ILaunchService`). This is a foundational architectural choice that enables scalability and long-term viability.

- **Contract vs. Implementation:** An interface defines a stable "contract" of _what_ a service should do. The provided `General...Service` classes offer a powerful default implementation that is driven by the configuration file.
- **Enabling Customization:** When a project's needs exceed the capabilities of the default implementation or configuration, developers can write their own concrete class that implements the interface with bespoke logic. This custom class can then be "plugged in" via the dependency injection system, without needing to modify the rest of the template's code.

## About Me

Hey everyone\! I'm Jack Zhao, the creator of this template. I want to give a huge shoutout to Thomas Molbach, Noah Karsky, and Darren Downtain for providing invaluable input from a solution architect's point of view. I also want to thank Lucas Guimaraes, Khaled Shaheen and Gayatri Babel for their help in building this.

This code is my attempt to create a standard template that 'breaks' the cycle where projects build simple tools, outgrow them, and are then forced to build a new and often hard-to-reuse solution. My current belief is that it's impossible for a template to have long-term success if it's not built on the fundamental premise of being extended. Customer needs will evolve, and new product features will create new opportunities for optimization.
