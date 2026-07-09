import type { ReactNode } from "react";
import type { DocumentTab, TreeNode } from "../types/discoveryNodes";
import type { MessageKey } from "../i18n";

/** Context passed to module open-node handlers (tab creation). */
export type OpenNodeContext = {
  setTabs: React.Dispatch<React.SetStateAction<DocumentTab[]>>;
  setActiveTabId: React.Dispatch<React.SetStateAction<string | null>>;
  setRowDetail: React.Dispatch<React.SetStateAction<unknown>>;
  t: (key: MessageKey, params?: Record<string, string | number>) => string;
  openSavedQuery: (query: import("../types/discoveryNodes").SavedQuery) => void;
  openGovernanceWorkspaceTab: (
    workspace: "spaces" | "groups",
    subTab: import("../types/discoveryNodes").GovernanceSubTab,
    artifactRel?: string | null
  ) => void;
  openRecordsStreamTab: (streamExternalId: string, label: string) => void;
  openSqlForOpenTarget: (
    target: import("../types/discoveryNodes").OpenTarget,
    label: string
  ) => void;
};

/** Context passed to module tab render handlers. */
export type RenderTabContext = {
  setTabs: React.Dispatch<React.SetStateAction<DocumentTab[]>>;
  setRowDetail: React.Dispatch<React.SetStateAction<unknown>>;
  t: (key: MessageKey, params?: Record<string, string | number>) => string;
  updateDataModelTab: (tab: import("../types/discoveryNodes").DataModelDocumentTab) => void;
  updateSqlTab: (tab: import("../types/discoveryNodes").SqlDocumentTab) => void;
  updateRecordsStreamTab: (tab: import("../types/discoveryNodes").RecordsStreamDocumentTab) => void;
  updateTransformationTab: (tab: import("../types/discoveryNodes").TransformationDocumentTab) => void;
  updateFunctionTab: (tab: import("../types/discoveryNodes").FunctionDocumentTab) => void;
  updateWorkflowTab: (tab: import("../types/discoveryNodes").WorkflowDocumentTab) => void;
  updateEtlDocumentTab: (
    tab:
      | import("../types/discoveryNodes").EtlPipelineDocumentTab
      | import("../types/discoveryNodes").EtlTemplateDocumentTab
  ) => void;
  patchEtlTabRunSession: (
    tabId: string,
    patch: import("../types/transformTabRun").TransformTabRunSessionPatch
  ) => void;
  queryDmView: (view: import("../types/discoveryNodes").DataModelGraphView) => void;
  openWorkflowInTransform: (ref: import("../types/discoveryNodes").WorkflowRef) => Promise<void>;
  openInTransformBusyId: string | null;
  openInTransformError: string | null;
  setTransformPipelinesRevision: React.Dispatch<React.SetStateAction<number>>;
  setGovernanceArtifactsRevision: React.Dispatch<
    React.SetStateAction<{ token: number; workspace: "spaces" | "groups" }>
  >;
  bumpTransformPipelinesTree: () => void;
  deletePipeline: (pipelineId: string, label: string) => Promise<void>;
  deleteTemplate: (templateId: string, label: string) => Promise<void>;
  openRenamePipeline: (pipelineId: string, label: string) => void;
  openRenameTemplate: (templateId: string, label: string) => void;
  onTransformCopyCreated: (
    result:
      | { kind: "pipeline"; pipelineId: string; label: string }
      | { kind: "template"; templateId: string; label: string }
  ) => void;
  openNodePreviewQuery: (
    pipelineOrTemplateId: string,
    previewNodeId: string,
    parameters: import("../types/transformCanvas").TransformWorkflowParameters | null | undefined,
    nodeConfig: Record<string, unknown> | undefined,
    runId: string | undefined
  ) => void;
  openFileContentQueryFromRow: (row: Record<string, unknown>) => void;
  downloadFileFromRow: (row: Record<string, unknown>) => void;
  saveSqlTab?: (tab: import("../types/discoveryNodes").SqlDocumentTab, mode: "save" | "saveAs") => void;
};

export type DiscoveryModule = {
  id: string;
  treeRootId: string;
  labelKey: MessageKey;
  /** Return true when this module should handle the tree node open action. */
  ownsNode: (node: TreeNode) => boolean;
  tryOpenNode: (node: TreeNode, ctx: OpenNodeContext) => boolean;
  /** Return true when this module renders the tab. */
  ownsTab: (tab: DocumentTab) => boolean;
  renderTab: (tab: DocumentTab, ctx: RenderTabContext) => ReactNode | null;
};
