# Deployment Guide

This guide provides step-by-step instructions for deploying the CDF File Annotation Module to your Cognite Data Fusion (CDF) project.

## Prerequisites

Before deploying this module, ensure you have the following:

- **Python 3.11+** installed on your system
- **An active Cognite Data Fusion (CDF) project**
- **CDF Toolkit** installed (see step 1 below)
- **Required Python packages** are listed in:
  - `cdf_file_annotation/functions/fn_file_annotation_launch/requirements.txt`
  - `cdf_file_annotation/functions/fn_file_annotation_finalize/requirements.txt`

### Data Preparation Requirements

Alias and tag generation is abstracted out of the annotation function. You'll need to create a transformation that populates the `aliases` and `tags` properties of your file and target entity views:

#### Aliases Property

- Used to match files with entities
- Should contain a list of alternative names or identifiers that can be found in the file's image
- Examples: `["FT-101A", "Flow Transmitter 101A", "FT101A"]`

#### Tags Property

The `tags` property serves multiple purposes and consists of the following:

- **`DetectInDiagrams`**: Identifies files and assets to include as entities filtered by primary scope and secondary scope (if provided)
- **`ScopeWideDetect`**: Identifies files and assets to include as entities filtered by a primary scope only
- **`ToAnnotate`**: Identifies files that need to be annotated
- **`AnnotationInProcess`**: Identifies files that are in the process of being annotated
- **`Annotated`**: Identifies files that have been annotated
- **`AnnotationFailed`**: Identifies files that have failed the annotation process (either by erroring out or by receiving 0 possible matches)

> **Note**: Don't worry if these concepts don't immediately make sense. Aliases and tags are explained in greater detail in the `detailed_guides/` documentation. The template also includes a jupyter notebook that prepares the files and assets for annotation if using the toolkit's quickstart module.

## Deployment Steps

_**NOTE:** I'm constantly improving this template, thus some parts of the video walkthroughs are from an older version. The video tutorials below are still **relevant**. Any breaking changes will receive a new video tutorial._

_(If videos fail to load, try loading the page in incognito or re-sign into GitHub)_

### Step 1: Create a CDF Project through Toolkit

Follow the [CDF Toolkit guide](https://docs.cognite.com/cdf/deploy/cdf_toolkit/) to set up your project.

Optionally, initialize the quickstart package using toolkit CLI:

```bash
poetry init
poetry add cognite-toolkit
poetry run cdf modules init <project-name>
```

<video src="https://github.com/user-attachments/assets/4dfa8966-a419-47b9-8ee1-4fea331705fd"></video>

<video src="https://github.com/user-attachments/assets/bc165848-5f8c-4eff-9a38-5b2288ec7e23"></video>

### Step 2: Integrate the Module

1. Move the `local_setup/` folder to the root and unpack `.vscode/` and `.env.tmpl`
2. Update the `default.config.yaml` file with project-specific configurations
3. Add the module name to the list of selected modules in your `config.{env}.yaml` file
4. Create a `.env` file with credentials pointing to your CDF project

<video src="https://github.com/user-attachments/assets/78ef2f59-4189-4059-90d6-c480acb3083e"></video>

<video src="https://github.com/user-attachments/assets/32df7e8b-cc27-4675-a813-1a72406704d5"></video>

### Step 3: Build and Deploy the Module

1. (Optional) Build and deploy the quickstart template modules
2. Build and deploy this module:

```bash
poetry run cdf build --env dev
poetry run cdf deploy --dry-run
poetry run cdf deploy
```

#### Example Configuration File

Below is an example `config.<env>.yaml` configuration:

```yaml
# config.<env>.yaml used in examples below
environment:
  name: dev
  project: <insert>
  validation-type: dev
  selected:
    - modules/

variables:
  modules:
    # stuff from quickstart package...
    organization: tx

    # ...

    cdf_ingestion:
      workflow: ingestion
      groupSourceId: <insert>
      ingestionClientId: ${IDP_CLIENT_ID} # Changed from ${INGESTION_CLIENT_ID}
      ingestionClientSecret: ${IDP_CLIENT_SECRET} # Changed from ${INGESTION_CLIENT_SECRET}
      pandidContextualizationFunction: contextualization_p_and_id_annotater
      contextualization_connection_writer: contextualization_connection_writer
      schemaSpace: sp_enterprise_process_industry
      schemaSpace2: cdf_cdm
      schemaSpace3: cdf_idm
      instanceSpaces:
        - springfield_instances
        - cdf_cdm_units
      runWorkflowUserIds:
        - <insert>

    contextualization:
      cdf_file_annotation:
        # used in /data_sets, /data_models, /functions, /extraction_pipelines, and /workflows
        annotationDatasetExternalId: ds_file_annotation

        # used in /data_models and /extraction_pipelines
        annotationStateExternalId: FileAnnotationState
        annotationStateInstanceSpace: sp_dat_cdf_annotation_states
        annotationStateSchemaSpace: sp_hdm #NOTE: stands for space helper data model
        annotationStateVersion: v1.0.1
        fileSchemaSpace: sp_enterprise_process_industry
        fileExternalId: txFile
        fileVersion: v1

        # used in /raw and /extraction_pipelines
        rawDb: db_file_annotation
        rawTableDocTag: annotation_documents_tags
        rawTableDocDoc: annotation_documents_docs
        rawTableCache: annotation_entities_cache

        # used in /extraction_pipelines
        extractionPipelineExternalId: ep_file_annotation
        targetEntitySchemaSpace: sp_enterprise_process_industry
        targetEntityExternalId: txEquipment
        targetEntityVersion: v1

        # used in /functions and /workflows
        launchFunctionExternalId: fn_file_annotation_launch #NOTE: if this is changed, then the folder holding the launch function must be named the same as the new external ID
        launchFunctionVersion: v1.0.0
        finalizeFunctionExternalId: fn_file_annotation_finalize #NOTE: if this is changed, then the folder holding the finalize function must be named the same as the new external ID
        finalizeFunctionVersion: v1.0.0
        functionClientId: ${IDP_CLIENT_ID}
        functionClientSecret: ${IDP_CLIENT_SECRET}

        # used in /workflows
        workflowSchedule: "*/10 * * * *"
        workflowExternalId: wf_file_annotation
        workflowVersion: v1

        # used in /auth
        groupSourceId: <insert> # source ID from Azure AD for the corresponding groups


    # ...
```

<video src="https://github.com/user-attachments/assets/0d85448d-b886-4ff1-96bb-415ef5efad2f"></video>

<video src="https://github.com/user-attachments/assets/0508acce-cb3c-4fbf-a1c2-5c781d9b3de7"></video>

### Step 4: Run the Workflow

After deployment, the annotation process is managed by a workflow that orchestrates the `Launch` and `Finalize` functions. The workflow is automatically triggered based on the schedule defined in the configuration. You can monitor the progress and logs of the functions in the CDF UI.

**Optional preparatory steps:**

1. Run the ingestion workflow from the quickstart package to create instances of `<org>File`, `<org>Asset`, etc.
2. Check out the instantiated files that have been annotated using the annotation function from the quickstart package
3. Run the `local_setup.ipynb` notebook to set up the files for annotation

**Run the File Annotation Workflow** in the CDF UI and monitor its progress.

<video src="https://github.com/user-attachments/assets/1bd1b4fe-42c6-4cd7-9cde-66e51a27c8f8"></video>

<video src="https://github.com/user-attachments/assets/b189910c-6eca-41c3-9f45-dbe83693ea42"></video>

<video src="https://github.com/user-attachments/assets/b5d932c2-4b58-4b04-95cf-dd748aa3e3b1"></video>

<video src="https://github.com/user-attachments/assets/fa267c9f-472d-4ad5-a35b-0102394de7e2"></video>

## Local Development and Debugging

This template is configured for easy local execution and debugging directly within Visual Studio Code.

### Setup Instructions

1. **Create Environment File**: Before running locally, you must create a `.env` file in the root directory. This file will hold the necessary credentials and configuration for connecting to your CDF project. Populate it with the required environment variables for `IDP_CLIENT_ID`, `CDF_CLUSTER`, etc. In the `local_runs/` folder you'll find a `.env` template.

2. **Use the VS Code Debugger**: The repository includes a pre-configured `local_runs/.vscode/launch.json` file. Move the `.vscode/` folder to the top level of your repo.

   - Navigate to the "Run and Debug" view in the VS Code sidebar
   - You will see dropdown options for launching the different functions (e.g., `Launch Function`, `Finalize Function`)
   - Select the function you wish to run and click the green "Start Debugging" arrow
   - This will start the function on your local machine, with the debugger attached, allowing you to set breakpoints and inspect variables
   - Feel free to change/adjust the arguments passed into the function call to point to a test extraction pipeline and/or change the log level

<video src="https://github.com/user-attachments/assets/f8c66306-1502-4e44-ac48-6b24f612900c"></video>

## Troubleshooting

### Common Issues

- **Authentication Errors**: Ensure your `.env` file contains valid credentials and that your service principal has the necessary permissions
- **Module Not Found**: Verify that the module is listed in your `config.{env}.yaml` file under `selected`
- **Function Deployment Fails**: Check that the function folder names match the external IDs defined in your configuration
- **Workflow Not Triggering**: Verify the workflow schedule is valid cron syntax and that the workflow has been deployed successfully

For additional help, please refer to the [detailed guides](detailed_guides/) or [open an issue](../../issues) on GitHub.

## Next Steps

After successful deployment:

1. Review the [Configuration Guide](detailed_guides/CONFIG.md) to understand all available options
2. Check the [Configuration Patterns Guide](detailed_guides/CONFIG_PATTERNS.md) for common use cases
3. Explore the [Development Guide](detailed_guides/DEVELOPING.md) if you need to extend functionality
4. Monitor your workflows and extraction pipelines in the CDF UI

---

Return to [Main README](README.md)
