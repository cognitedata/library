import type { DeploymentPackDefinition } from "./types";
import { QUALITIZER_DEPLOYMENT_PACK } from "./qualitizer-deployment-pack";

const HUB_DEPLOYMENT_PACKS_LIBRARY = "https://hub.cognite.com/deployment-packs-472";
const HUB_ISA_MANUFACTURING_PACK = `${HUB_DEPLOYMENT_PACKS_LIBRARY}/isa-batch-manufacturing-data-model-extension-5821`;

/**
 * Deployment packs: Qualitizer’s own row first, then the Cognite Hub library and `cog-itworks`
 * toolkit modules under `modules/` (default external IDs from `config.dev.yaml` where templated).
 */
export const DEPLOYMENT_PACKS: DeploymentPackDefinition[] = [
  QUALITIZER_DEPLOYMENT_PACK,
  {
    id: "dp:accelerators:cdf_common",
    name: "CDF Common",
    description: `Shared contextualization writer function (toolkit: modules/accelerators/cdf_common). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: ["contextualization_connection_writer"],
    },
  },
  {
    id: "dp:acc:cdf_ingestion",
    name: "Ingestion Workflow",
    description: `Orchestrated population workflow; probe uses the first transformation task from the default workflow graph (pi timeseries). Toolkit: modules/accelerators/cdf_ingestion. Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["pi_timeseries_springfield_aveva_pi"],
    },
  },
  {
    id: "dp:acc:ctx:cdf_connection_sql",
    name: "Contextualization Direct Relation Transformations",
    description: `SQL-based direct-relation population (modules/accelerators/contextualization/cdf_connection_sql). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["maintenance_order_to_asset", "operation_to_asset"],
    },
  },
  {
    id: "dp:acc:ctx:cdf_entity_matching",
    name: "CDF Entity Matching",
    description: `Entity matching functions (modules/accelerators/contextualization/cdf_entity_matching). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: [
        "fn_dm_context_timeseries_entity_matching",
        "fn_dm_context_metadata_update",
      ],
    },
  },
  {
    id: "dp:acc:ctx:cdf_file_annotation",
    name: "File Annotation (contextualization)",
    description: `Annotation pipeline: helper data model plus all four function stages (modules/accelerators/contextualization/cdf_file_annotation). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: [
        "fn_file_annotation_prepare",
        "fn_file_annotation_launch",
        "fn_file_annotation_finalize",
        "fn_file_annotation_promote",
      ],
      dataModels: [
        {
          space: "sp_hdm",
          externalId: "helper_datamodel",
          version: "v1.0.0",
        },
      ],
    },
  },
  {
    id: "dp:acc:industrial_tools:cdf_search",
    name: "Industrial Data Exploration (location filter)",
    description: `Fusion location filter for enterprise data model scope (modules/accelerators/industrial_tools/cdf_search). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      locationFilterExternalIds: ["springfield_location_filter"],
      dataModels: [
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustries",
          version: "v1",
        },
      ],
    },
  },
  {
    id: "dp:acc:open_industrial_data_sync",
    name: "Open Industrial Data Sync",
    description: `OID sync function (modules/accelerators/open_industrial_data_sync). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: ["fn_oid_sync"],
    },
  },
  {
    id: "dp:dashboards:rpt_quality",
    name: "Quality Reports — Contextualization Rate",
    description: `Reporting transformations (modules/dashboards/rpt_quality); probe uses the files annotation rate job. Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["tr_report_files_annotationrate"],
    },
  },
  {
    id: "dp:models:qs_enterprise_dm",
    name: "Quick Start Enterprise Data Model",
    description: `Enterprise and search data models (modules/models/qs_enterprise_dm). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      dataModels: [
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustries",
          version: "v1",
        },
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustriesSearch",
          version: "v1",
        },
      ],
    },
  },
  {
    id: "dp:models:isa_manufacturing_extension",
    name: "ISA Batch Manufacturing Data Model Extension",
    description: `ISA-95 / ISA-88 manufacturing domain model from the Hub pack and cognitedata/library \`isa_manufacturing_extension\`. Detection fingerprints the official template view set (customer copies may rename spaces/models). Hub: ${HUB_ISA_MANUFACTURING_PACK} · Code: https://github.com/cognitedata/library/tree/main/modules/models/isa_manufacturing_extension`,
    signals: {},
    isaManufacturingDerivative: {
      minDistinctiveViewsInOneDataModel: 14,
    },
  },
  {
    id: "dp:models:rmdm_v1",
    name: "Reliability & Maintenance Data Model (RMDM v1)",
    description: `ISO 14224–oriented R&M containers and views (library: modules/models/rmdm_v1). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      dataModels: [{ space: "rmdm", externalId: "rmdm_v1", version: "v1" }],
    },
  },
  {
    id: "dp:dashboards:project_health",
    name: "CDF Project Health Dashboard",
    description: `Project health metrics function for the Streamlit dashboard (library: modules/dashboards/project_health). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: ["project_health_handler"],
    },
  },
  {
    id: "dp:dashboards:context_quality",
    name: "Contextualization Quality Dashboard",
    description: `Contextualization quality metrics function (library: modules/dashboards/context_quality). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: ["context_quality_handler"],
    },
  },
  {
    id: "dp:atlas:ai_property_extractor",
    name: "Atlas AI Property Extractor",
    description: `LLM property extraction function and pipeline (library: modules/atlas_ai/ai_extractor). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      functionExternalIds: ["fn_ai_property_extractor"],
    },
  },
  {
    id: "dp:acc:ctx:cdf_p_and_id_annotation",
    name: "P&ID Annotation",
    description: `PID tagging transformations from cdf_p_and_id_annotation (library: modules/accelerators/contextualization/cdf_p_and_id_annotation); customers may rename these transforms. Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["asset_tagging_tr", "file_tagging_tr"],
    },
  },
  {
    id: "dp:acc:infield_quickstart",
    name: "InField QuickStart",
    description: `APM / InField quickstart config data model (library: modules/accelerators/infield_quickstart/cdf_apm_base). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      dataModels: [{ space: "APM_Config", externalId: "APM_Config", version: "1" }],
    },
  },
  {
    id: "cdf_pi",
    name: "AVEVA PI / PI System",
    description: `PI timeseries population (modules/sourcesystem/cdf_pi). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["pi_timeseries_springfield_aveva_pi"],
      dataModels: [
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustries",
          version: "v1",
        },
      ],
    },
  },
  {
    id: "cdf_sap_assets",
    name: "SAP S/4HANA — Assets",
    description: `SAP asset & equipment population (modules/sourcesystem/cdf_sap_assets). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["sap_assets_springfield_s4hana"],
      dataModels: [
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustries",
          version: "v1",
        },
      ],
    },
  },
  {
    id: "cdf_sap_events",
    name: "SAP S/4HANA — Activities",
    description: `SAP maintenance orders and operations (modules/sourcesystem/cdf_sap_events). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["sap_maintenance_orders_springfield_s4hana"],
      dataModels: [
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustries",
          version: "v1",
        },
      ],
    },
  },
  {
    id: "cdf_sharepoint",
    name: "SharePoint Files",
    description: `SharePoint file metadata ingestion (modules/sourcesystem/cdf_sharepoint). Hub: ${HUB_DEPLOYMENT_PACKS_LIBRARY}`,
    signals: {
      transformationExternalIds: ["files_metadata_springfield"],
      dataModels: [
        {
          space: "sp_enterprise_process_industry",
          externalId: "ORGProcessIndustries",
          version: "v1",
        },
      ],
    },
  },
];
