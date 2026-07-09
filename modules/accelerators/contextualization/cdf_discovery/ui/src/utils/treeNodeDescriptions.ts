import type { MessageKey } from "../i18n";
import type { TreeNode } from "../types/discoveryNodes";
import {
  DATA_ROOT,
  DATA_SAVED_QUERIES,
  EXTRACT_ROOT,
  FUSION_ADMIN,
  FUSION_GROUPS,
  FUSION_ROOT,
  FUSION_SPACES,
  GOVERNANCE_GROUPS,
  GOVERNANCE_ROOT,
  GOVERNANCE_SPACES,
  INDEX_ROOT,
  MONITOR_ROOT,
  TRANSFORM_PIPELINES,
  TRANSFORM_ROOT,
  TRANSFORM_TEMPLATES,
} from "./treeNodeIds";

const TREE_DESC_KEYS: Record<string, MessageKey> = {
  [DATA_ROOT]: "tree.desc.data",
  [FUSION_ROOT]: "tree.desc.fusion",
  [GOVERNANCE_ROOT]: "tree.desc.gov",
  [EXTRACT_ROOT]: "tree.desc.extract",
  [TRANSFORM_ROOT]: "tree.desc.transform",
  [INDEX_ROOT]: "invertedIndex.nav.desc.indexing",
  [MONITOR_ROOT]: "tree.desc.monitor",
  "data:raw": "tree.desc.dataRaw",
  "data:dm": "tree.desc.dataDm",
  "data:classic": "tree.desc.dataClassic",
  "data:records": "tree.desc.dataRecords",
  [DATA_SAVED_QUERIES]: "tree.desc.dataSavedQueries",
  [FUSION_SPACES]: "tree.desc.fusionSpaces",
  [FUSION_ADMIN]: "tree.desc.fusionAdmin",
  [FUSION_GROUPS]: "tree.desc.fusionGroups",
  [GOVERNANCE_SPACES]: "tree.desc.govSpaces",
  [GOVERNANCE_GROUPS]: "tree.desc.govGroups",
  [TRANSFORM_PIPELINES]: "tree.desc.transformPipelines",
  [TRANSFORM_TEMPLATES]: "tree.desc.transformTemplates",
  "fusion:dm": "tree.desc.fusionDm",
  "fusion:integration": "tree.desc.fusionIntegration",
  "index:dashboard": "invertedIndex.nav.desc.dashboard",
  "index:config": "invertedIndex.nav.desc.config",
  "index:ops": "invertedIndex.nav.desc.ops",
  "index:query": "invertedIndex.nav.desc.query",
  "index:file": "invertedIndex.nav.desc.fileContext",
  "index:tag-reuse": "invertedIndex.nav.desc.tagReuse",
  "index:ops:build-metadata": "invertedIndex.nav.desc.buildMetadata",
  "index:ops:build-annotations": "invertedIndex.nav.desc.buildAnnotations",
  "index:ops:target-driven": "invertedIndex.nav.desc.targetDriven",
};

const KIND_DESC_KEYS: Record<string, MessageKey> = {
  folder: "tree.desc.folder",
  connection: "tree.desc.connection",
  workflow: "tree.desc.kindWorkflow",
  transformation: "tree.desc.kindTransformation",
  function: "tree.desc.kindFunction",
  saved_query: "tree.desc.kindSavedQuery",
  dm_data_model: "tree.desc.kindDmDataModel",
  dm_view: "tree.desc.kindDmView",
  fusion_dm_view: "tree.desc.kindDmView",
  raw_database: "tree.desc.kindRawDatabase",
  raw_table: "tree.desc.kindRawTable",
  classic_resource: "tree.desc.kindClassicResource",
  etl_pipeline: "tree.desc.kindEtlPipeline",
  etl_template: "tree.desc.kindEtlTemplate",
  etl_workflow_yaml: "tree.desc.kindEtlPipeline",
  record_stream: "tree.desc.kindRecordStream",
  gov_artifact_file: "tree.desc.kindGovArtifact",
  gov_space: "tree.desc.kindGovArtifact",
  gov_group: "tree.desc.kindGovArtifact",
  extract: "tree.desc.extract",
  monitor: "tree.desc.monitor",
  inverted_index_dashboard: "invertedIndex.nav.desc.dashboard",
  inverted_index_configuration: "invertedIndex.nav.desc.config",
  inverted_index_build_metadata: "invertedIndex.nav.desc.buildMetadata",
  inverted_index_build_annotations: "invertedIndex.nav.desc.buildAnnotations",
  inverted_index_target_driven: "invertedIndex.nav.desc.targetDriven",
  inverted_index_query: "invertedIndex.nav.desc.query",
  inverted_index_file_context: "invertedIndex.nav.desc.fileContext",
  inverted_index_tag_reuse: "invertedIndex.nav.desc.tagReuse",
};

export function treeNodeDescription(
  node: Pick<TreeNode, "id" | "kind" | "has_children">,
  t: (key: MessageKey) => string
): string {
  const byId = TREE_DESC_KEYS[node.id];
  if (byId) return t(byId);
  const byKind = KIND_DESC_KEYS[node.kind];
  if (byKind) return t(byKind);
  if (node.has_children) return t("tree.desc.folder");
  return t("tree.desc.default");
}
