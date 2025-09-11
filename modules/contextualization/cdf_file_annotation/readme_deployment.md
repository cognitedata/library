# Deployment Guide

## Getting Started

Deploying this annotation module into a new Cognite Data Fusion (CDF) project is a streamlined process. Since all necessary resources (Data Sets, Extraction Pipelines, Functions, etc.) are bundled into a single module, you only need to configure one file to get started.

---

### Prerequisites

- Python 3.11+
- An active Cognite Data Fusion (CDF) project.
- The required Python packages are listed in the `cdf_file_annotation/functions/fn_file_annotation_launch/requirements.txt` and `cdf_file_annotation/functions/fn_file_annotation_finalize/requirements.txt` files.
- Alias and tag generation is abstracted out of the annotation function. Thus, you'll need to create a transformation that populates the `aliases` and `tags` property of your file and target entity view.
  - The `aliases` property is used to match files with entities and should contain a list of alternative names or identifiers that can be found in the files image.
  - The `tags` property serves multiple purposes and consists of the following...
    - (`DetectInDiagrams`) Identifies files and assets to include as entities filtered by primary scope and secondary scope (if provided).
    - (`ScopeWideDetect`) Identifies files and asset to include as entities filtered by a primary scope.
    - (`ToAnnotate`) Identifies files that need to be annotated.
    - (`AnnotationInProcess`) Identifies files that are in the process of being annotated.
    - (`Annotated`) Identifies files that have been annotated.
    - (`AnnotationFailed`) Identifies files that have failed the annotation process. Either by erroring out or by receiving 0 possible matches.
  - Don't worry if these concepts don't immediately make sense. Aliases and tags are explained in greater detail in the detailed_guides/ documentation. The template also includes a jupyter notebook that prepare the files and assets for annotation if using the toolkit's quickstart module.

---

### Deployment Steps with Quickstart Module

The video and deployment steps are with regards to getting things setup on a quickstart module. If you have an existing project, the important step is to insert the correct information in the config.env.yaml file.

_(if videos fail to load, try loading page in incognito or re-sign into github)_

1. **Create a CDF Project through Toolkit**
   - Follow the guide [here](https://docs.cognite.com/cdf/deploy/cdf_toolkit/)
   - (optional) Initialize the quickstart package using toolkit CLI

```bash
poetry init
poetry add cognite-toolkit
poetry run cdf modules init <project-name>
```

<video src="https://github.com/user-attachments/assets/4dfa8966-a419-47b9-8ee1-4fea331705fd"></video>

<video src="https://github.com/user-attachments/assets/bc165848-5f8c-4eff-9a38-5b2288ec7e23"></video>

2. **Integrate the Module**
   - Move the `local_setup/` folder to the root and unpack .vscode/ and .env.tmpl
   - Update the default.config.yaml file with project-specific configurations
   - Add the module name to the list of selected modules in your config.{env}.yaml file
   - Make sure to create a .env file with credentials pointing to your CDF project

<video src="https://github.com/user-attachments/assets/78ef2f59-4189-4059-90d6-c480acb3083e"></video>

<video src="https://github.com/user-attachments/assets/32df7e8b-cc27-4675-a813-1a72406704d5"></video>

3. **Build and Deploy the Module**

   - (optional) Build and deploy the quickstart template modules
   - Build and deploy this module

```bash
poetry run cdf build --env dev
poetry run cdf deploy --dry-run
poetry run cdf deploy
```

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
    organization: <insert>

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
        annotationStateInstanceSpace: sp_dat_cdf_annotation_states # NOTE: can set to fileInstanceSpace if scoping is required - refer to detailed_guides/config_patterns.md
        annotationStateSchemaSpace: sp_hdm #NOTE: stands for space helper data model
        annotationStateVersion: v1.0.0
        fileSchemaSpace: sp_enterprise_process_industry
        fileExternalId: <insert {organization}>File
        fileInstanceSpace: <insert> # Optional - used for scoping - refer to detailed_guides/config_patterns.md
        fileVersion: v1

        # used in /raw and /extraction_pipelines
        rawDb: db_file_annotation
        rawTableDocTag: annotation_documents_tags
        rawTableDocDoc: annotation_documents_docs
        rawTableCache: annotation_entities_cache

        # used in /extraction_pipelines
        extractionPipelineExternalId: ep_file_annotation
        targetEntitySchemaSpace: sp_enterprise_process_industry
        targetEntityExternalId: <insert {organization}>Equipment
        targetEntityInstanceSpace: <insert> # Optional - used for scoping - refer to detailed_guides/config_patterns.md
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

4. **Run the Workflow**

   After deployment, the annotation process is managed by a workflow that orchestrates the `Launch` and `Finalize` functions. The workflow is automatically triggered based on the schedule defined in the configuration. You can monitor the progress and logs of the functions in the CDF UI.

   - (optional) Run the ingestion workflow from the quickstart package to create instances of <org>File, <org>Asset, etc
     - (optional) Checkout the instantiated files that have been annotated using the annotation function from the quickstart package
   - (optional) Run the local_setup.ipynb to setup the files for annotation
   - Run the File Annotation Workflow

<video src="https://github.com/user-attachments/assets/1bd1b4fe-42c6-4cd7-9cde-66e51a27c8f8"></video>

<video src="https://github.com/user-attachments/assets/b189910c-6eca-41c3-9f45-dbe83693ea42"></video>

<video src="https://github.com/user-attachments/assets/b5d932c2-4b58-4b04-95cf-dd748aa3e3b1"></video>

<video src="https://github.com/user-attachments/assets/fa267c9f-472d-4ad5-a35b-0102394de7e2"></video>

---

### Local Development and Debugging

This template is configured for easy local execution and debugging directly within Visual Studio Code.

1.  **Create Environment File**: Before running locally, you must create a `.env` file in the root directory. This file will hold the necessary credentials and configuration for connecting to your CDF project. Populate it with the required environment variables for `IDP_CLIENT_ID`, `CDF_CLUSTER`, etc. In the `local_runs/` folder you'll find a .env template.

2.  **Use the VS Code Debugger**: The repository includes a pre-configured `local_runs/.vscode/launch.json` file. Please move the .vscode/ folder to the top level of your repo.

    - Navigate to the "Run and Debug" view in the VS Code sidebar.
    - You will see dropdown options for launching the different functions (e.g., `Launch Function`, `Finalize Function`).
    - Select the function you wish to run and click the green "Start Debugging" arrow. This will start the function on your local machine, with the debugger attached, allowing you to set breakpoints and inspect variables.
    - Feel free to change/adjust the arguments passed into the function call to point to a test_extraction_pipeline and/or change the log level.

<video src="https://github.com/user-attachments/assets/f8c66306-1502-4e44-ac48-6b24f612900c"></video>
