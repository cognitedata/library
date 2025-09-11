# Architecture Guide

## Configuration Driven

The templates behavior is entirely controlled by the `ep_file_annotation.config.yaml` file. This YAML file is parsed by Pydantic models in the code, ensuring a strongly typed and validated configuration.

Key configuration sections include:

- `dataModelViews`: Defines the data model views for files, annotation states, and target entities.
- `prepareFunction`: Configures the queries to find files to annotate.
- `launchFunction`: Sets parameters for the annotation job, such as batch size, entity matching properties, and a new `patternMode: true` flag to enable the pattern detection feature.
- `finalizeFunction`: Defines how to process and apply the final annotations.

This file allows for deep customization. For example, you can use a list of query configurations to combine them with `OR` logic, or you can set `primaryScopeProperty` to `None` to process files that are not tied to a specific scope.

## How It Works

The template operates in three main phases, orchestrated by a CDF Workflow that calls a `Launch` and multiple `Finalize` functions in parallel.

### Prepare Phase

- **Goal**: Identify files that need to be annotated.
- **Process**: This initial step, handled within the `LaunchService`, queries for new files tagged for annotation (e.g., with a "ToAnnotate" tag). For each new file, it creates a corresponding `AnnotationState` instance in the data model, marking it with a "New" status.

### Launch Phase

- **Goal**: Initiate the annotation jobs for all ready files.
- **Process**: The `LaunchService` queries for `AnnotationState` instances with a "New" or "Retry" status. It groups these files by a configured scope (e.g., `site`) to create a relevant entity cache, avoiding redundant lookups. It then calls the Cognite Diagram Detect API to start two asynchronous jobs: a standard entity matching job and a pattern mode job. Finally, it updates the `AnnotationState` instance with the job IDs and sets the status to "Processing".

### Finalize Phase

- **Goal**: Retrieve, process, and store the results of completed annotation jobs.
- **Process**: The `FinalizeService` queries for `AnnotationState` instances with a "Processing" status. Once both the standard and pattern jobs for a file are complete, it retrieves the results. It applies the standard annotations by creating edges in the data model and logs the results to RAW tables. The pattern mode results are also logged to a dedicated RAW table (`annotation_documents_patterns`). Finally, it updates the `AnnotationState` status to "Annotated" or "Failed".

## Design Philosophy

There were two principles I kept in mind when designing this template.

- **Evolving Needs:** Project requirements evolve. A simple, plug-and-play tool is great to start with, but it can hit limitations when faced with demands for scale, performance, or specialized logic—as was the case with previous annotation templates when applied to projects with tens of thousands of complex files. My belief is that a modern template must be built to be extended.

- **The Balance Between Configuration and Code:** This template is architected to provide two primary modes of adaptation, striking a crucial balance:

  1.  **Quick Start (via Configuration):** For the majority of use cases, a user should only need to edit the `config.yaml` file. By defining their data model views and tuning process parameters, they can get the template running quickly and effectively.
  2.  **Scaling (via Interfaces):** When a project demands unique optimizations—like a non-standard batching strategy or a complex query to fetch entities—the interface-based design provides the necessary "escape hatch." A developer can write a custom Python class to implement their specialized logic, ensuring the template can meet any future requirement.

## Architecture & Optimizations

### Stateful Processing with Data Models

Instead of using RAW tables to track file status, this module uses a dedicated `AnnotationState` Data Model. This is a crucial architectural choice for several reasons:

- **Concurrency**: Data model instances provide built-in optimistic locking via the `existing_version` field. When multiple `Finalize` functions try to claim a job, only the first one succeeds, preventing race conditions.
- **Performance**: Finding files ready for processing is a fast, indexed query against the data model, which is far more efficient than filtering millions of rows in a RAW table.
- **Data Integrity & Governance**: The `AnnotationState` view enforces a strict schema for all status information, ensuring consistency and making the pipeline's state a first-class, governable entity in CDF.

### Interface-Based Extensibility

The template is designed around a core set of abstract interfaces (e.g., `AbstractLaunchService`, `IDataModelService`). This enables scalability and long-term viability.

- **Contract vs. Implementation**: An interface defines a stable contract of _what_ a service should do. The provided `General...Service` classes offer a powerful default implementation driven by the configuration file.
- **Enabling Customization**: When a project's needs exceed the configuration options, developers can write their own class that implements the interface with custom logic. This custom class can then be "plugged in" via the dependency injection system without modifying the rest of the template's code.

For more details on configuration and extending the template, see `detailed_guides/CONFIG.md` and `detailed_guides/DEVELOPING.md`.

### Known Limitation: Scalable Deletion of Pattern Results

A key architectural challenge remains regarding the `cleanOldAnnotations` feature for pattern mode results.

- **The Challenge**: Pattern results are stored in the `annotation_documents_patterns` RAW table with a key format of `f"{tag_text}:{file_id.space}:{file_id.external_id}"`. To delete these rows when reprocessing a file, one would need to know all possible values of `{tag_text}` beforehand.
- **The Impact**: The current implementation cannot scalably delete old pattern results for a specific file because listing all rows to find the relevant keys is not feasible for large tables. This can lead to stale data on the Annotation Quality dashboard if files are frequently re-processed. This is a known issue targeted for future enhancement.
- **Temporary Solution**: Delete the `annotation_documents_patterns` table before re-annotating all files to ensure fresh data.

## Detailed Guides

This README provides a high-level overview of the template's purpose and architecture. To gain a deeper understanding of how to configure and extend the template, I highly recommend exploring the detailed guides located in the `cdf_file_annotation/detailed_guides/` directory:

- **`CONFIG.md`**: A document outlining the `ep_file_annotation.config.yaml` file to control the behavior of the Annotation Function.
- **`CONFIG_PATTERNS.md`**: A guide with recipes for common operational tasks, such as processing specific subsets of data, reprocessing files for debugging, and tuning performance by adjusting the configuration.
- **`DEVELOPING.md`**: A guide for developers who wish to extend the template's functionality. It details the interface-based architecture and provides a step-by-step walkthrough on how to create and integrate your own custom service implementations for specialized logic.

## About Me

Hey everyone\! I'm Jack Zhao, the creator of this template. I want to give a huge shoutout to Thomas Molbach, Noah Karsky, and Darren Downtain for providing invaluable input from a solution architect's point of view. I also want to thank Khaled Shaheen and Gayatri Babel for their help in building this.

This code is my attempt to create a standard template that 'breaks' the cycle where projects build simple tools, outgrow them, and are then forced to build a new and often hard-to-reuse solution. My current belief is that it's impossible for a template to have long-term success if it's not built on the fundamental premise of being extended. Customer needs will evolve, and new product features will create new opportunities for optimization.
