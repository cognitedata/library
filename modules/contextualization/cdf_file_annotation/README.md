# Cognite Data Model-Based Annotation Module

## Overview

The Annotation template is a framework designed to automate the process of annotating files within Cognite Data Fusion (CDF). It uses a data model-centric approach to manage the annotation lifecycle, from selecting files to processing results and generating reports. This template is configurable, allowing it to adapt to various data models and annotation requirements.

## Key Features

- **Configuration-Driven**: The entire process is controlled by a single `config.yaml` file, allowing adaptation without code changes.

- **Dual Annotation Modes**: Simultaneously runs standard entity matching and a new pattern-based detection mode to create a comprehensive indexed reference catalog.
- **Large Document Support**: Automatically handles files with more than 50 pages by processing them in chunks and tracking overall progress.

- **Parallel Execution Ready**: Designed for concurrent execution with a robust optimistic locking mechanism to prevent race conditions.

- **Comprehensive Reporting**: Includes a multi-page Streamlit dashboard for monitoring pipeline health, analyzing annotation quality, and managing patterns.

---

<video src="https://github.com/user-attachments/assets/b33c6a5f-0078-46b6-9f5a-dae30713ae5e"></video>
