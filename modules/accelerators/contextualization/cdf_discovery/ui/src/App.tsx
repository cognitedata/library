import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  deleteTransformPipeline,
  deleteTransformTemplate,
  fetchConnection,
  fetchTransformPipelineByWorkflow,
  type ConnectionInfo,
} from "./api";
import { DocumentTabBar } from "./components/DocumentTabBar";
import { DocumentTabFullscreenOverlay } from "./components/DocumentTabFullscreenOverlay";
import { DataModelFlowPane } from "./components/DataModelFlowPane";
import { ObjectDiscovery } from "./components/ObjectDiscovery";
import { AccessibleResizeHandle } from "./components/AccessibleResizeHandle";
import { TreePanelDockMenu } from "./components/PanelDockToggleButtons";
import { PanelHeaderActions, panelHeaderMenuTriggerId } from "./components/PanelHeaderActions";
import { documentTabButtonId, documentTabPanelIdForTab } from "./components/documentTabIds";
import { PanelDragHandle } from "./components/PanelDragHandle";
import { PanelDropOverlay } from "./components/PanelDropOverlay";
import { PropertiesPanel, type PropertiesPanelLayout } from "./components/PropertiesPanel";
import {
  useDiscoveryPanelLayout,
  type TreePanelSide,
} from "./hooks/useDiscoveryPanelLayout";
import { useDocumentTabFullscreen } from "./hooks/useDocumentTabFullscreen";
import { SqlQueryPane } from "./components/SqlQueryPane";
import { RecordsStreamDocumentTab as RecordsStreamPane } from "./components/RecordsStreamDocumentTab";
import { FunctionPane } from "./components/FunctionPane";
import { TransformationPane } from "./components/TransformationPane";
import { TransformPipelinePane } from "./components/transform/TransformPipelinePane";
import { TransformFusionWorkflowPane } from "./components/transform/TransformFusionWorkflowPane";
import { CreatePipelineDialog } from "./components/transform/CreatePipelineDialog";
import { RenameTransformLabelDialog } from "./components/transform/RenameTransformLabelDialog";
import { SavePipelineAsTemplateDialog } from "./components/transform/SavePipelineAsTemplateDialog";
import { GovernanceScopePane } from "./components/governance/GovernanceScopePane";
import { TransformScopePane } from "./components/transform/TransformScopePane";
import { TransformWorkflowYamlPane } from "./components/transform/TransformWorkflowYamlPane";
import { GovernanceSpacesPane } from "./components/governance/GovernanceSpacesPane";
import { GovernanceGroupsPane } from "./components/governance/GovernanceGroupsPane";
import { GovernanceCdfSpacePane } from "./components/governance/GovernanceCdfSpacePane";
import { GovernanceCdfGroupPane } from "./components/governance/GovernanceCdfGroupPane";
import { CreateGovernanceArtifactDialog } from "./components/governance/CreateGovernanceArtifactDialog";
import { CogniteLogo } from "./components/CogniteLogo";
import { ComingSoonPane } from "./components/ComingSoonPane";
import { useAppSettings } from "./context/AppSettingsContext";
import { useDiscoveryConfig } from "./context/DiscoveryConfigContext";
import { openTargetFromSqlTabId } from "./utils/workspacePersistence";
import { dmInstanceKindFromOpenTarget } from "./utils/dmInstanceFromRow";
import { LOCALES } from "./i18n";
import {
  isDataModelTab,
  isFunctionTab,
  isSqlTab,
  isRecordsStreamTab,
  isTransformationTab,
  isWorkflowTab,
  isGovernanceScopeTab,
  isGovernanceSpacesTab,
  isGovernanceGroupsTab,
  isGovernanceCdfSpaceTab,
  isGovernanceCdfGroupTab,
  isEtlPipelineTab,
  isEtlTemplateTab,
  isEtlScopeTab,
  isEtlWorkflowYamlTab,
  isExtractTab,
  isMonitorTab,
  type DataModelDocumentTab,
  type GovernanceSubTab,
  type GovernanceSpacesDocumentTab,
  type GovernanceGroupsDocumentTab,
  type GovernanceCdfSpaceDocumentTab,
  type GovernanceCdfGroupDocumentTab,
  type DataModelGraphView,
  type DocumentTab,
  type FunctionDocumentTab,
  type WorkflowDocumentTab,
  type TransformationDocumentTab,
  type EtlPipelineDocumentTab,
  type EtlTemplateDocumentTab,
  type OpenTarget,
  type SqlDocumentTab,
  type RecordsStreamDocumentTab,
  type TreeNode,
} from "./types/discoveryNodes";
import { dataModelTabKey, dataModelTabLabel, dataModelRefFromNode } from "./utils/dataModelTabs";
import {
  workflowRefFromNode,
  workflowTabKey,
  workflowTabLabel,
} from "./utils/workflowTabs";
import {
  createFunctionTab,
  functionIdFromNode,
  functionLabelFromMeta,
  functionTabKey,
} from "./utils/functionTabs";
import {
  createTransformationTab,
  transformationIdFromNode,
  transformationLabelFromMeta,
  transformationTabKey,
} from "./utils/transformationTabs";
import {
  createSqlTab,
  createSqlTabForOpenTarget,
  createFileContentSqlTab,
  SQL_WORKSPACE_TAB_ID,
} from "./utils/sqlTabs";
import { createRecordsStreamTab } from "./utils/recordsStreamTabs";
import { canQueryTreeNode, labelForDmView, openTargetForDmView } from "./utils/sqlQuerySeed";
import { fileContentRefFromRow } from "./utils/queryableFileFromRow";
import { downloadCdfFileWithConfirm } from "./utils/downloadCdfFile";
import {
  createSqlTabFromSavedQuery,
  savedQueryEntryFromSqlTab,
  savedQueryFromNode,
  slugifySavedQueryId,
  sqlSavedQueryTabId,
  uniqueSavedQueryId,
} from "./utils/savedQueries";
import {
  createEtlPipelineTab,
  createEtlTemplateTab,
  createEtlWorkflowYamlTab,
  etlPipelineTabKey,
  etlScopeTabKey,
  etlTemplateTabKey,
  pipelineIdFromNode,
  scopeSuffixFromNode,
  pipelineLabelFromMeta,
  templateIdFromNode,
  templateLabelFromMeta,
  workflowYamlRelPathFromNode,
  workflowYamlTabKey,
  opensTransformTab,
} from "./utils/transformTabs";
import { TRANSFORM_ROOT } from "./utils/treeNodeIds";
import {
  type TransformTabRunSessionPatch,
  withTransformTabRunSession,
} from "./types/transformTabRun";
import { createExtractTab, createMonitorTab, opensExtractTab, opensMonitorTab } from "./utils/workspaceTabs";
import { opensGovernanceCdfDetailTab, opensGovernanceTab } from "./utils/governanceTabs";
import type { GovernanceArtifactCreateContext } from "./utils/governanceTreeNew";
import { restoreWorkspaceTabs, serializeWorkspace } from "./utils/workspacePersistence";
import type { SavedQuery } from "./types/discoveryNodes";

export function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();
  const {
    workspace,
    persistWorkspace,
    loading: configLoading,
    savedQueries,
    savedQueriesRevision,
    persistSavedQueries,
  } = useDiscoveryConfig();
  const [connection, setConnection] = useState<ConnectionInfo | null>(null);
  const [connError, setConnError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [governanceArtifactsRevision, setGovernanceArtifactsRevision] = useState<{
    token: number;
    workspace: "spaces" | "groups";
  }>({ token: 0, workspace: "spaces" });
  const [transformPipelinesRevision, setTransformPipelinesRevision] = useState(0);
  const [transformTemplatesRevision, setTransformTemplatesRevision] = useState(0);
  const [createGovArtifact, setCreateGovArtifact] = useState<GovernanceArtifactCreateContext | null>(
    null
  );
  const [createPipelineOpen, setCreatePipelineOpen] = useState(false);
  const [createPipelineInitialTemplateId, setCreatePipelineInitialTemplateId] = useState<
    string | undefined
  >(undefined);
  const [saveAsTemplateState, setSaveAsTemplateState] = useState<{
    pipelineId: string;
    pipelineLabel: string;
  } | null>(null);
  const [renameTransformState, setRenameTransformState] = useState<
    | { kind: "pipeline"; id: string; label: string }
    | { kind: "template"; id: string; label: string }
    | null
  >(null);
  const [selectedNode, setSelectedNode] = useState<TreeNode | null>(null);
  const [tabs, setTabs] = useState<DocumentTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [rowDetail, setRowDetail] = useState<unknown | null>(null);
  const panel = useDiscoveryPanelLayout();
  const workspaceRestored = useRef(false);

  const openEtlScopeTab = useCallback(() => {
    const id = etlScopeTabKey();
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        setActiveTabId(id);
        return prev;
      }
      setActiveTabId(id);
      return [
        ...prev,
        { kind: "etl_scope", id: "transform:scope" as const, label: t("transform.tree.scope") },
      ];
    });
    setRowDetail(null);
  }, [t]);

  const openCreatedPipeline = useCallback((pipelineId: string, label: string) => {
    setTransformPipelinesRevision((n) => n + 1);
    const id = etlPipelineTabKey(pipelineId);
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        setActiveTabId(id);
        return prev;
      }
      const tab = createEtlPipelineTab(pipelineId, label);
      setActiveTabId(id);
      return [...prev, tab];
    });
  }, []);

  const openCreatedTemplate = useCallback((templateId: string, label: string) => {
    setTransformTemplatesRevision((n) => n + 1);
    const id = etlTemplateTabKey(templateId);
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        setActiveTabId(id);
        return prev;
      }
      const tab = createEtlTemplateTab(templateId, label);
      setActiveTabId(id);
      return [...prev, tab];
    });
  }, []);

  const onTransformCopyCreated = useCallback(
    (result: { kind: "pipeline"; pipelineId: string; label: string } | { kind: "template"; templateId: string; label: string }) => {
      if (result.kind === "pipeline") {
        openCreatedPipeline(result.pipelineId, result.label);
        return;
      }
      openCreatedTemplate(result.templateId, result.label);
    },
    [openCreatedPipeline, openCreatedTemplate]
  );

  const openSqlWorkspace = useCallback(() => {
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === SQL_WORKSPACE_TAB_ID);
      if (existing) {
        setActiveTabId(SQL_WORKSPACE_TAB_ID);
        return prev;
      }
      const tab = createSqlTab({ id: SQL_WORKSPACE_TAB_ID, label: t("sql.title") });
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, [t]);

  useEffect(() => {
    if (configLoading || workspaceRestored.current) return;
    workspaceRestored.current = true;
    if (workspace.tabs.length) {
      const restored = restoreWorkspaceTabs(workspace, t("sql.title"));
      if (restored.tabs.length) {
        setTabs(restored.tabs);
        setActiveTabId(restored.activeTabId);
        return;
      }
    }
    openSqlWorkspace();
  }, [configLoading, workspace, t, openSqlWorkspace]);

  useEffect(() => {
    if (!workspaceRestored.current) return;
    const timer = window.setTimeout(() => {
      void persistWorkspace(serializeWorkspace(tabs, activeTabId)).catch(() => {
        /* best-effort; config errors surface in DiscoveryConfigContext */
      });
    }, 500);
    return () => window.clearTimeout(timer);
  }, [tabs, activeTabId, persistWorkspace]);

  const openSavedQuery = useCallback((query: SavedQuery) => {
    const tab = createSqlTabFromSavedQuery(query);
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isSqlTab(existing)) {
        setActiveTabId(tab.id);
        return prev.map((t) =>
          t.id === tab.id
            ? {
                ...existing,
                query: tab.query,
                label: tab.label,
                limit: tab.limit,
                convertToString: tab.convertToString,
                savedQueryId: tab.savedQueryId,
              }
            : t
        );
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const saveSqlTab = useCallback(
    async (tab: SqlDocumentTab, mode: "save" | "saveAs") => {
      const existingIds = new Set(savedQueries.map((q) => q.id));
      let id = tab.savedQueryId;
      let name = tab.label.trim() || t("sql.title");

      if (mode === "saveAs" || !id) {
        const prompted = window.prompt(t("sql.saveAsPrompt"), name);
        if (prompted == null) return;
        name = prompted.trim();
        if (!name) return;
        const base = slugifySavedQueryId(name);
        id = uniqueSavedQueryId(base, existingIds);
      }

      const entry = savedQueryEntryFromSqlTab(tab, name, id!);
      const next =
        tab.savedQueryId && id === tab.savedQueryId
          ? savedQueries.map((q) => (q.id === id ? entry : q))
          : [...savedQueries.filter((q) => q.id !== id), entry];
      await persistSavedQueries(next);

      const newTabId = sqlSavedQueryTabId(id);
      setTabs((prev) =>
        prev.map((t) =>
          t.id === tab.id
            ? {
                ...(t as SqlDocumentTab),
                id: newTabId,
                label: name,
                savedQueryId: id,
              }
            : t
        )
      );
      setActiveTabId((current) => (current === tab.id ? newTabId : current));
    },
    [persistSavedQueries, savedQueries, t]
  );

  const deleteSavedQuery = useCallback(
    async (query: SavedQuery) => {
      if (!window.confirm(t("discovery.deleteSavedQueryConfirm", { name: query.name }))) {
        return;
      }
      const tabId = sqlSavedQueryTabId(query.id);
      await persistSavedQueries(savedQueries.filter((q) => q.id !== query.id));
      setTabs((prev) => {
        const next = prev.filter(
          (t) => t.id !== tabId && !(isSqlTab(t) && t.savedQueryId === query.id)
        );
        setActiveTabId((cur) => {
          if (!cur || next.some((tab) => tab.id === cur)) return cur;
          return next[next.length - 1]?.id ?? null;
        });
        return next;
      });
      setSelectedNode((current) => {
        if (current?.kind !== "saved_query") return current;
        const selected = savedQueryFromNode(current);
        return selected?.id === query.id ? null : current;
      });
    },
    [persistSavedQueries, savedQueries, t]
  );

  const deletePipeline = useCallback(
    async (pipelineId: string, label: string) => {
      if (!window.confirm(t("transform.pipelines.deleteConfirm", { name: label }))) {
        return;
      }
      try {
        await deleteTransformPipeline(pipelineId);
      } catch (e) {
        window.alert(`${t("transform.pipelines.deleteFailed")}: ${String(e)}`);
        return;
      }
      setTransformPipelinesRevision((n) => n + 1);
      const tabId = etlPipelineTabKey(pipelineId);
      setTabs((prev) => {
        const next = prev.filter((tab) => tab.id !== tabId);
        setActiveTabId((cur) => {
          if (!cur || next.some((tab) => tab.id === cur)) return cur;
          return next[next.length - 1]?.id ?? null;
        });
        return next;
      });
      setSelectedNode((current) => {
        if (current?.kind !== "etl_pipeline") return current;
        const selectedId = pipelineIdFromNode(current);
        return selectedId === pipelineId ? null : current;
      });
    },
    [t]
  );

  const deleteTemplate = useCallback(
    async (templateId: string, label: string) => {
      if (!window.confirm(t("transform.templates.deleteConfirm", { name: label }))) {
        return;
      }
      try {
        await deleteTransformTemplate(templateId);
      } catch (e) {
        window.alert(`${t("transform.templates.deleteFailed")}: ${String(e)}`);
        return;
      }
      setTransformTemplatesRevision((n) => n + 1);
      const tabId = etlTemplateTabKey(templateId);
      setTabs((prev) => {
        const next = prev.filter((tab) => tab.id !== tabId);
        setActiveTabId((cur) => {
          if (!cur || next.some((tab) => tab.id === cur)) return cur;
          return next[next.length - 1]?.id ?? null;
        });
        return next;
      });
      setSelectedNode((current) => {
        if (current?.kind !== "etl_template") return current;
        const selectedId = templateIdFromNode(current);
        return selectedId === templateId ? null : current;
      });
    },
    [t]
  );

  const applyPipelineRename = useCallback((pipelineId: string, newLabel: string) => {
    setTransformPipelinesRevision((n) => n + 1);
    const tabId = etlPipelineTabKey(pipelineId);
    setTabs((prev) =>
      prev.map((tab) => {
        if (tab.id !== tabId || !isEtlPipelineTab(tab)) return tab;
        return {
          ...tab,
          label: newLabel,
          document: tab.document ? { ...tab.document, label: newLabel } : tab.document,
        };
      })
    );
  }, []);

  const applyTemplateRename = useCallback((templateId: string, newLabel: string) => {
    setTransformTemplatesRevision((n) => n + 1);
    const tabId = etlTemplateTabKey(templateId);
    setTabs((prev) =>
      prev.map((tab) => {
        if (tab.id !== tabId || !isEtlTemplateTab(tab)) return tab;
        const document =
          tab.document && typeof tab.document === "object"
            ? { ...tab.document, label: newLabel }
            : tab.document;
        return { ...tab, label: newLabel, document };
      })
    );
  }, []);

  const openRenamePipeline = useCallback((pipelineId: string, label: string) => {
    setRenameTransformState({ kind: "pipeline", id: pipelineId, label });
  }, []);

  const openRenameTemplate = useCallback((templateId: string, label: string) => {
    setRenameTransformState({ kind: "template", id: templateId, label });
  }, []);

  const openRecordsStreamTab = useCallback((streamExternalId: string, label: string) => {
    const tab = createRecordsStreamTab(streamExternalId, label);
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isRecordsStreamTab(existing)) {
        setActiveTabId(tab.id);
        return prev;
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const openSqlForOpenTarget = useCallback((target: OpenTarget, label: string) => {
    const tab = createSqlTabForOpenTarget(target, label);
    if (!tab) return;
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isSqlTab(existing)) {
        setActiveTabId(tab.id);
        return prev.map((t) => (t.id === tab.id ? { ...existing, query: tab.query, label: tab.label } : t));
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const openFileContentQueryFromRow = useCallback((row: Record<string, unknown>) => {
    const ref = fileContentRefFromRow(row);
    if (!ref) return;
    const tab = createFileContentSqlTab(ref);
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isSqlTab(existing)) {
        setActiveTabId(tab.id);
        return prev;
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const downloadFileFromRow = useCallback(
    async (row: Record<string, unknown>) => {
      try {
        await downloadCdfFileWithConfirm(row, t);
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        throw new Error(t("sql.downloadFileFailed", { detail }));
      }
    },
    [t]
  );

  const queryDmView = useCallback(
    (view: DataModelGraphView) => {
      openSqlForOpenTarget(openTargetForDmView(view), labelForDmView(view));
    },
    [openSqlForOpenTarget]
  );

  const openGovernanceWorkspaceTab = useCallback(
    (which: "spaces" | "groups", subTab: GovernanceSubTab, artifactRel?: string) => {
      const id = which === "spaces" ? "gov:spaces" : "gov:groups";
      const label = which === "spaces" ? "Spaces" : "Groups";
      setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing && isGovernanceSpacesTab(existing) && which === "spaces") {
          setActiveTabId(id);
          return prev.map((tab) =>
            tab.id === id
              ? {
                  ...existing,
                  activeSubTab: subTab,
                  artifactRel: artifactRel ?? existing.artifactRel,
                }
              : tab
          );
        }
        if (existing && isGovernanceGroupsTab(existing) && which === "groups") {
          setActiveTabId(id);
          return prev.map((tab) =>
            tab.id === id
              ? {
                  ...existing,
                  activeSubTab: subTab,
                  artifactRel: artifactRel ?? existing.artifactRel,
                }
              : tab
          );
        }
        if (existing) {
          setActiveTabId(id);
          return prev;
        }
        if (which === "spaces") {
          setActiveTabId("gov:spaces");
          const tab: GovernanceSpacesDocumentTab = {
            kind: "governance_spaces",
            id: "gov:spaces",
            label,
            activeSubTab: subTab,
            artifactRel: artifactRel ?? null,
          };
          return [...prev, tab];
        }
        setActiveTabId("gov:groups");
        const tab: GovernanceGroupsDocumentTab = {
          kind: "governance_groups",
          id: "gov:groups",
          label,
          activeSubTab: subTab,
          artifactRel: artifactRel ?? null,
        };
        return [...prev, tab];
      });
    },
    []
  );

  const openDiscoveryNode = useCallback(
    (node: TreeNode) => {
      if (node.kind === "saved_query") {
        const query = savedQueryFromNode(node);
        if (query) openSavedQuery(query);
        return;
      }
      if (node.kind === "workflow") {
        const ref = workflowRefFromNode(node);
        if (!ref) return;
        void (async () => {
          try {
            const found = await fetchTransformPipelineByWorkflow(ref.external_id);
            const scopeSuffix = found.scope_suffix?.trim() || "all";
            const pipelineId = found.pipeline_id;
            const label =
              (typeof found.pipeline.label === "string" && found.pipeline.label.trim()) ||
              pipelineId;
            const id = etlPipelineTabKey(pipelineId, scopeSuffix);
            setTabs((prev) => {
              const existing = prev.find((tab) => tab.id === id);
              if (existing) {
                setActiveTabId(id);
                return prev;
              }
              const tab = createEtlPipelineTab(pipelineId, label, null, scopeSuffix);
              setActiveTabId(id);
              return [...prev, tab];
            });
            setRowDetail(null);
          } catch {
            const id = workflowTabKey(ref);
            setTabs((prev) => {
              const existing = prev.find((tab) => tab.id === id);
              if (existing) {
                setActiveTabId(id);
                return prev;
              }
              const tab: WorkflowDocumentTab = {
                kind: "workflow",
                id,
                label: workflowTabLabel(ref),
                workflow: ref,
                graph: null,
                loading: true,
                error: null,
              };
              setActiveTabId(id);
              return [...prev, tab];
            });
            setRowDetail(null);
          }
        })();
        return;
      }
      if (node.kind === "function") {
        const fnId = functionIdFromNode(node);
        if (fnId == null) return;
        const id = functionTabKey(fnId);
        const label = functionLabelFromMeta(node.meta);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createFunctionTab(fnId, label);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "etl_pipeline") {
        const pipelineId = pipelineIdFromNode(node);
        if (!pipelineId) return;
        const scopeSuffix = scopeSuffixFromNode(node);
        const id = etlPipelineTabKey(pipelineId, scopeSuffix);
        const label = pipelineLabelFromMeta(node.meta);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createEtlPipelineTab(pipelineId, label, null, scopeSuffix);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "etl_template") {
        const templateId = templateIdFromNode(node);
        if (!templateId) return;
        const id = etlTemplateTabKey(templateId);
        const label = templateLabelFromMeta(node.meta);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createEtlTemplateTab(templateId, label);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "etl_workflow_yaml") {
        const relPath = workflowYamlRelPathFromNode(node);
        if (!relPath) return;
        const id = workflowYamlTabKey(relPath);
        const label = node.label?.trim() || relPath.split("/").pop() || relPath;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createEtlWorkflowYamlTab(relPath, label);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "etl_scope" || node.id === "transform:scope") {
        openEtlScopeTab();
        return;
      }
      if (opensExtractTab(node)) {
        const id = "extract" as const;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createExtractTab(t("tree.extract"));
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (opensMonitorTab(node)) {
        const id = "monitor" as const;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createMonitorTab(t("tree.monitor"));
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "transformation") {
        const txId = transformationIdFromNode(node);
        if (txId == null) return;
        const id = transformationTabKey(txId);
        const label = transformationLabelFromMeta(node.meta);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createTransformationTab(txId, label);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "dm_data_model") {
        const ref = dataModelRefFromNode(node);
        if (!ref) return;
        const id = dataModelTabKey(ref);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab: DocumentTab = {
            kind: "data_model",
            id,
            label: dataModelTabLabel(ref),
            dataModel: ref,
            graph: null,
            loading: true,
            error: null,
          };
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      const ws = node.meta?.governance_workspace as string | undefined;
      if (ws === "scope" || node.id === "gov") {
        const id = "gov:scope";
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          setActiveTabId(id);
          return [
            ...prev,
            { kind: "governance_scope", id, label: t("governance.subtab.scope") },
          ];
        });
        setRowDetail(null);
        return;
      }
      if (ws === "spaces" || node.id === "gov:spaces") {
        openGovernanceWorkspaceTab("spaces", "configure");
        setRowDetail(null);
        return;
      }
      if (ws === "groups" || node.id === "gov:groups") {
        openGovernanceWorkspaceTab("groups", "configure");
        setRowDetail(null);
        return;
      }
      if (node.kind === "gov_artifact_file") {
        const rel = String(node.meta?.artifact_rel ?? "");
        const workspace = (node.meta?.governance_workspace as "spaces" | "groups") ?? "spaces";
        openGovernanceWorkspaceTab(workspace, "artifacts", rel);
        setRowDetail(null);
        return;
      }
      if (node.kind === "gov_space") {
        const space = String(node.meta?.space ?? "");
        if (!space) return;
        const id = `gov:space:tab:${space}`;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab: GovernanceCdfSpaceDocumentTab = {
            kind: "governance_cdf_space",
            id,
            label: node.label,
            space,
            detail: null,
            loading: true,
            error: null,
          };
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "gov_group") {
        const gid = node.meta?.id;
        const groupId = typeof gid === "number" ? gid : Number(gid);
        if (!Number.isFinite(groupId)) return;
        const id = `gov:group:tab:${groupId}`;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab: GovernanceCdfGroupDocumentTab = {
            kind: "governance_cdf_group",
            id,
            label: node.label,
            groupId,
            detail: null,
            loading: true,
            error: null,
          };
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (
        node.kind === "record_stream" &&
        node.open_target?.type === "record_stream"
      ) {
        openRecordsStreamTab(node.open_target.stream_external_id, node.label);
        return;
      }
      if (canQueryTreeNode(node) && node.open_target) {
        openSqlForOpenTarget(node.open_target, node.label);
      }
    },
    [openEtlScopeTab, openGovernanceWorkspaceTab, openRecordsStreamTab, openSavedQuery, openSqlForOpenTarget, t]
  );

  const loadConnection = useCallback(async () => {
    setConnError(null);
    try {
      const info = await fetchConnection();
      setConnection(info);
    } catch (e) {
      setConnection(null);
      setConnError(String(e));
    }
  }, []);

  useEffect(() => {
    void loadConnection();
  }, [loadConnection, refreshKey]);

  const activeTab = useMemo(
    () => tabs.find((tab) => tab.id === activeTabId) ?? null,
    [tabs, activeTabId]
  );

  const { fullscreenOpen, toggleFullscreen, closeFullscreen } = useDocumentTabFullscreen(
    activeTab != null
  );

  const dmInstanceKind = useMemo(() => {
    if (!activeTab || !isSqlTab(activeTab)) return null;
    const target = openTargetFromSqlTabId(activeTab.id);
    if (!target) return null;
    return dmInstanceKindFromOpenTarget(target);
  }, [activeTab]);

  const updateDataModelTab = useCallback((updated: DataModelDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateSqlTab = useCallback((updated: SqlDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateRecordsStreamTab = useCallback((updated: RecordsStreamDocumentTab) => {
      setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
    },
    []
  );

  const updateTransformationTab = useCallback((updated: TransformationDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateFunctionTab = useCallback((updated: FunctionDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateWorkflowTab = useCallback((updated: WorkflowDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateEtlDocumentTab = useCallback(
    (updated: EtlPipelineDocumentTab | EtlTemplateDocumentTab) => {
      setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
    },
    []
  );

  const patchEtlTabRunSession = useCallback((tabId: string, patch: TransformTabRunSessionPatch) => {
    setTabs((prev) =>
      prev.map((tab) => {
        if (tab.id !== tabId || (tab.kind !== "etl_pipeline" && tab.kind !== "etl_template")) {
          return tab;
        }
        return withTransformTabRunSession(tab, patch);
      })
    );
  }, []);

  const reorderTabs = useCallback((fromIndex: number, toIndex: number) => {
    if (fromIndex === toIndex || fromIndex < 0 || toIndex < 0) return;
    setTabs((prev) => {
      if (fromIndex >= prev.length || toIndex >= prev.length) return prev;
      const next = [...prev];
      const [moved] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, moved);
      return next;
    });
  }, []);

  const closeTab = useCallback(
    (id: string) => {
      const nextTabs = tabs.filter((tab) => tab.id !== id);
      const nextActiveId =
        activeTabId === id
          ? nextTabs.length
            ? nextTabs[nextTabs.length - 1].id
            : null
          : activeTabId;
      setTabs(nextTabs);
      setActiveTabId(nextActiveId);
      if (workspaceRestored.current) {
        void persistWorkspace(serializeWorkspace(nextTabs, nextActiveId)).catch(() => {
          /* best-effort; config errors surface in DiscoveryConfigContext */
        });
      }
    },
    [tabs, activeTabId, persistWorkspace]
  );

  const connectionLabel = connection
    ? `${connection.project} @ ${connection.base_url || "CDF"}`
    : connError
      ? t("connection.failed", { detail: connError })
      : t("connection.loading");

  const shouldRenderSideColumn = (side: TreePanelSide): boolean => {
    if (panel.treeSide === side) return true;
    if (panel.propertiesDock === "left-bottom" && panel.treeSide === side) return true;
    if (panel.propertiesDock === "right" && side === "right") return true;
    return false;
  };

  const showLeftColumn = shouldRenderSideColumn("left");
  const showRightColumn = shouldRenderSideColumn("right");

  const columnWidthForSide = (side: TreePanelSide): number => {
    const hasTree = panel.treeSide === side;
    const treeExpanded = hasTree && !panel.treeCollapsed;
    if (treeExpanded) return panel.treeWidth;
    if (hasTree && panel.treeCollapsed) {
      const hasStackedProps =
        (panel.propertiesDock === "left-bottom" && panel.treeSide === side) ||
        (panel.propertiesDock === "right" && side === "right");
      if (hasStackedProps) return panel.treeWidth;
      return panel.sideColumnWidth;
    }
    if (panel.propertiesDock === "right" && side === "right") return panel.propertiesSize;
    return panel.sideColumnWidth;
  };

  const resizeHandlerForSide = (side: TreePanelSide) => {
    const hasTree = panel.treeSide === side;
    const treeExpanded = hasTree && !panel.treeCollapsed;
    if (treeExpanded) return panel.onResizeTreeStart;
    if (panel.propertiesDock === "right" && side === "right") return panel.onResizePropertiesSideStart;
    return panel.onResizeTreeStart;
  };

  const renderActiveTabContent = (tab: DocumentTab) => {
    if (isDataModelTab(tab)) {
      return <DataModelFlowPane tab={tab} onTabUpdate={updateDataModelTab} onQueryView={queryDmView} />;
    }
    if (isWorkflowTab(tab)) {
      return <TransformFusionWorkflowPane key={tab.id} tab={tab} onTabUpdate={updateWorkflowTab} />;
    }
    if (isTransformationTab(tab)) {
      return (
        <TransformationPane
          tab={tab}
          onTabUpdate={updateTransformationTab}
          onSelectRow={(row) => setRowDetail(row)}
          onQueryFile={openFileContentQueryFromRow}
          onDownloadFile={downloadFileFromRow}
        />
      );
    }
    if (isFunctionTab(tab)) {
      return <FunctionPane tab={tab} onTabUpdate={updateFunctionTab} />;
    }
    if (isGovernanceScopeTab(tab)) {
      return <GovernanceScopePane />;
    }
    if (isGovernanceSpacesTab(tab)) {
      return (
        <GovernanceSpacesPane
          initialSubTab={tab.activeSubTab}
          initialArtifactRel={tab.artifactRel}
          onArtifactsChanged={(workspace) =>
            setGovernanceArtifactsRevision((prev) => ({
              token: prev.token + 1,
              workspace,
            }))
          }
        />
      );
    }
    if (isGovernanceGroupsTab(tab)) {
      return (
        <GovernanceGroupsPane
          initialSubTab={tab.activeSubTab}
          initialArtifactRel={tab.artifactRel}
          onArtifactsChanged={(workspace) =>
            setGovernanceArtifactsRevision((prev) => ({
              token: prev.token + 1,
              workspace,
            }))
          }
        />
      );
    }
    if (isGovernanceCdfSpaceTab(tab)) {
      return (
        <GovernanceCdfSpacePane
          tab={tab}
          onTabUpdate={(updated) => setTabs((prev) => prev.map((row) => (row.id === updated.id ? updated : row)))}
        />
      );
    }
    if (isGovernanceCdfGroupTab(tab)) {
      return (
        <GovernanceCdfGroupPane
          tab={tab}
          onTabUpdate={(updated) => setTabs((prev) => prev.map((row) => (row.id === updated.id ? updated : row)))}
        />
      );
    }
    if (isEtlPipelineTab(tab)) {
      return (
        <TransformPipelinePane
          key={tab.id}
          tab={tab}
          onTabUpdate={updateEtlDocumentTab}
          onRunSessionPatch={patchEtlTabRunSession}
          onCopyCreated={onTransformCopyCreated}
          onDelete={() => void deletePipeline(tab.pipelineId, tab.label)}
          onRename={() => openRenamePipeline(tab.pipelineId, tab.label)}
        />
      );
    }
    if (isEtlTemplateTab(tab)) {
      return (
        <TransformPipelinePane
          key={tab.id}
          editorKind="template"
          tab={tab}
          onTabUpdate={updateEtlDocumentTab}
          onRunSessionPatch={patchEtlTabRunSession}
          onCopyCreated={onTransformCopyCreated}
          onDelete={() => void deleteTemplate(tab.templateId, tab.label)}
          onRename={() => openRenameTemplate(tab.templateId, tab.label)}
        />
      );
    }
    if (isEtlScopeTab(tab)) {
      return <TransformScopePane />;
    }
    if (isEtlWorkflowYamlTab(tab)) {
      return (
        <TransformWorkflowYamlPane
          key={tab.id}
          tab={tab}
          onTabUpdate={(updated) =>
            setTabs((prev) => prev.map((row) => (row.id === updated.id ? updated : row)))
          }
        />
      );
    }
    if (isExtractTab(tab)) {
      return <ComingSoonPane workspace="extract" />;
    }
    if (isMonitorTab(tab)) {
      return <ComingSoonPane workspace="monitor" />;
    }
    if (isRecordsStreamTab(tab)) {
      return (
        <RecordsStreamPane
          tab={tab}
          onTabUpdate={updateRecordsStreamTab}
          onSelectRow={(row) => setRowDetail(row)}
        />
      );
    }
    if (isSqlTab(tab)) {
      return (
        <SqlQueryPane
          tab={tab}
          onTabUpdate={updateSqlTab}
          onSelectRow={(row) => setRowDetail(row)}
          onQueryFile={openFileContentQueryFromRow}
          onDownloadFile={downloadFileFromRow}
          onSave={tab.engine === "file_content" ? undefined : () => void saveSqlTab(tab, "save")}
          onSaveAs={tab.engine === "file_content" ? undefined : () => void saveSqlTab(tab, "saveAs")}
        />
      );
    }
    return (
      <div
        className="disc-empty-hint"
        style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}
      >
        {t("tabs.empty")}
      </div>
    );
  };

  const renderDocumentPane = () => (
    <>
      <div className="disc-doc-pane" style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
        <DocumentTabBar
          tabs={tabs}
          activeId={activeTabId}
          onSelect={setActiveTabId}
          onClose={closeTab}
          onReorder={reorderTabs}
          fullscreen={{
            open: fullscreenOpen,
            onToggle: toggleFullscreen,
            disabled: !activeTab,
          }}
        />
        {activeTab ? (
          fullscreenOpen ? (
            <div className="disc-doc-pane__fullscreen-placeholder" role="status">
              {t("tabs.fullscreenActiveHint")}
            </div>
          ) : (
            <div
              role="tabpanel"
              id={documentTabPanelIdForTab(activeTab.id)}
              aria-labelledby={documentTabButtonId(activeTab.id)}
              className="disc-doc-tabpanel"
            >
              {renderActiveTabContent(activeTab)}
            </div>
          )
        ) : (
          <div
            className="disc-empty-hint"
            style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}
          >
            {t("tabs.empty")}
          </div>
        )}
      </div>
      {fullscreenOpen && activeTab ? (
        <DocumentTabFullscreenOverlay t={t} title={activeTab.label} onClose={closeFullscreen}>
          {renderActiveTabContent(activeTab)}
        </DocumentTabFullscreenOverlay>
      ) : null}
    </>
  );

  const renderPropertiesPanel = (layout: PropertiesPanelLayout) => (
    <PropertiesPanel
      collapsed={panel.propertiesCollapsed}
      onToggleCollapse={panel.togglePropertiesCollapsed}
      selectedNode={selectedNode}
      rowDetail={rowDetail}
      dmInstanceKind={dmInstanceKind}
      paneSize={panel.propertiesSize}
      layout={layout}
      isDragging={panel.draggingPanel === "properties"}
      onPanelDragStart={() => panel.beginPanelDrag("properties")}
      onPanelDragEnd={panel.endPanelDrag}
      onQueryFile={openFileContentQueryFromRow}
      onDownloadFile={downloadFileFromRow}
      propertiesDock={panel.propertiesDock}
      onDockProperties={panel.dropPropertiesDock}
    />
  );

  const renderTreePane = () => (
    <aside
      className={`disc-tree-pane${panel.treeCollapsed ? " disc-tree-pane--collapsed" : ""}${panel.draggingPanel === "tree" ? " disc-panel--dragging" : ""}`}
    >
      <div className="disc-tree-pane-header">
        <PanelDragHandle
          panel="tree"
          labelKey="layout.dragHandle.tree"
          dockMenuTriggerId={panelHeaderMenuTriggerId("disc-tree-panel-menu")}
          onDragStart={() => panel.beginPanelDrag("tree")}
          onDragEnd={panel.endPanelDrag}
        />
        <span className="disc-tree-pane-header__title">{t("discovery.title")}</span>
        <div className="disc-tree-pane-header__actions">
          <PanelHeaderActions
            menuId="disc-tree-panel-menu"
            menuLabelKey="layout.panelMenu.tree"
            collapsed={panel.treeCollapsed}
            collapseLabelKey="discovery.collapse"
            expandLabelKey="discovery.show"
            onToggleCollapse={panel.toggleTreeCollapsed}
          >
            <TreePanelDockMenu treeSide={panel.treeSide} onDockTree={panel.dropTreeSide} />
          </PanelHeaderActions>
        </div>
      </div>
      {!panel.treeCollapsed && (
        <ObjectDiscovery
          refreshKey={refreshKey}
          savedQueriesRevision={savedQueriesRevision}
          governanceArtifactsRevision={governanceArtifactsRevision}
          transformPipelinesRevision={transformPipelinesRevision}
          transformTemplatesRevision={transformTemplatesRevision}
          connectionLabel={connection ? `${connection.project}` : undefined}
          selectedId={selectedNode?.id ?? null}
          onSelectNode={(node) => {
            setSelectedNode(node);
            setRowDetail(null);
            if (
              node &&
              (opensGovernanceTab(node) ||
                opensGovernanceCdfDetailTab(node) ||
                opensTransformTab(node) ||
                opensExtractTab(node) ||
                opensMonitorTab(node))
            ) {
              openDiscoveryNode(node);
            }
          }}
          onOpenNode={openDiscoveryNode}
          onDeleteSavedQuery={deleteSavedQuery}
          onTreeNew={(action) => {
            if (action.kind === "governance_space_artifact") {
              setCreateGovArtifact({ kind: "spaces", parentRel: action.parentRel });
              return;
            }
            if (action.kind === "governance_group_artifact") {
              setCreateGovArtifact({ kind: "groups", parentRel: action.parentRel });
              return;
            }
            if (action.kind === "saved_query") {
              openSqlWorkspace();
              return;
            }
            if (action.kind === "transform_pipeline_from_template") {
              if (
                !window.confirm(
                  t("transform.treeDrag.confirmCreateFromTemplate", {
                    name: action.templateId,
                  })
                )
              ) {
                return;
              }
              setCreatePipelineInitialTemplateId(action.templateId);
              setCreatePipelineOpen(true);
              return;
            }
            setCreatePipelineInitialTemplateId(undefined);
            setCreatePipelineOpen(true);
          }}
          onDeletePipeline={deletePipeline}
          onDeleteTemplate={deleteTemplate}
          onRenamePipeline={openRenamePipeline}
          onRenameTemplate={openRenameTemplate}
          onPipelineDropOnTemplates={(pipelineId, pipelineLabel) => {
            setSaveAsTemplateState({ pipelineId, pipelineLabel });
          }}
          onTemplateDropOnPipelines={(templateId, templateLabel) => {
            if (
              !window.confirm(
                t("transform.treeDrag.confirmCreateFromTemplate", { name: templateLabel })
              )
            ) {
              return;
            }
            setCreatePipelineInitialTemplateId(templateId);
            setCreatePipelineOpen(true);
          }}
          dataTreeDragEnabled={
            activeTab != null && (isEtlPipelineTab(activeTab) || isEtlTemplateTab(activeTab))
          }
        />
      )}
    </aside>
  );

  const renderSideColumn = (side: TreePanelSide) => {
    const showTree = panel.treeSide === side;
    const showStackedProps = panel.propertiesDock === "left-bottom" && panel.treeSide === side;
    const showSideProps = panel.propertiesDock === "right" && side === "right";
    const columnWidth = columnWidthForSide(side);

    return (
      <div
        className={`disc-side-column disc-side-column--${side}`}
        style={{ width: columnWidth, minWidth: columnWidth, maxWidth: columnWidth }}
      >
        {showTree && renderTreePane()}
        {showStackedProps && (
          <>
            {showTree && !panel.treeCollapsed && !panel.propertiesCollapsed && (
              <AccessibleResizeHandle
                className="disc-resize-handle-v"
                orientation="horizontal"
                value={panel.propertiesSize}
                min={panel.propsMin}
                max={panel.propsMaxHeight()}
                labelKey="layout.resize.propertiesSize"
                onMouseDown={panel.onResizePropertiesStackedStart}
                onValueChange={panel.setPropertiesSizeClamped}
              />
            )}
            {renderPropertiesPanel("stacked")}
          </>
        )}
        {showSideProps && (
          <>
            {showTree && !panel.treeCollapsed && !panel.propertiesCollapsed && (
              <AccessibleResizeHandle
                className="disc-resize-handle-v"
                orientation="horizontal"
                value={panel.propertiesSize}
                min={panel.propsMin}
                max={panel.propsMaxHeight()}
                labelKey="layout.resize.propertiesSize"
                onMouseDown={panel.onResizePropertiesStackedStart}
                onValueChange={panel.setPropertiesSizeClamped}
              />
            )}
            {renderPropertiesPanel(showTree ? "stacked" : "side")}
          </>
        )}
      </div>
    );
  };

  const renderHorizontalResize = (side: TreePanelSide) => (
    <AccessibleResizeHandle
      className="disc-resize-handle-h"
      orientation="vertical"
      value={columnWidthForSide(side)}
      min={panel.treeMin}
      max={panel.treeMax}
      labelKey="layout.resize.treeWidth"
      onMouseDown={resizeHandlerForSide(side)}
      onValueChange={panel.setTreeWidthClamped}
    />
  );

  return (
    <div className="disc-app">
      <a href="#disc-main" className="disc-skip-link">
        {t("a11y.skipToMain")}
      </a>
      <header className="disc-toolbar">
        <CogniteLogo />
        <h1 className="disc-toolbar__title">{t("app.title")}</h1>
        <span className="disc-connection-badge">{connectionLabel}</span>
        <button type="button" className="disc-btn" onClick={() => setRefreshKey((k) => k + 1)}>
          {t("toolbar.refresh")}
        </button>
        <button type="button" className="disc-btn" onClick={openSqlWorkspace}>
          {t("toolbar.sqlQuery")}
        </button>
        <div className="disc-toolbar__controls">
          <label className="disc-toolbar__control" title={t("controls.theme.tooltip")}>
            <span className="disc-toolbar__control-label">{t("controls.theme")}</span>
            <span className="disc-theme-toggle" role="group">
              <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                {t("controls.themeLight")}
              </button>
              <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                {t("controls.themeDark")}
              </button>
            </span>
          </label>
          <label className="disc-toolbar__control" title={t("controls.language.tooltip")}>
            <span className="disc-toolbar__control-label">{t("controls.language")}</span>
            <select value={locale} onChange={(e) => setLocale(e.target.value as typeof locale)}>
              {LOCALES.map(({ code, label }) => (
                <option key={code} value={code}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>
      {connError ? (
        <div className="disc-banner--error" role="alert">
          {t("connection.failed", { detail: connError })}
        </div>
      ) : null}
      <div className={`disc-main${panel.draggingPanel ? " disc-main--panel-drag" : ""}`}>
        <div className="disc-split-h">
          {showLeftColumn && renderSideColumn("left")}
          {showLeftColumn && renderHorizontalResize("left")}
          <main id="disc-main" className="disc-workspace">
            {panel.propertiesDock === "bottom" ? (
              <div className="disc-split-v">
                {renderDocumentPane()}
                {!panel.propertiesCollapsed && (
                  <AccessibleResizeHandle
                    className="disc-resize-handle-v"
                    orientation="horizontal"
                    value={panel.propertiesSize}
                    min={panel.propsMin}
                    max={panel.propsMaxHeight()}
                    labelKey="layout.resize.propertiesSize"
                    onMouseDown={panel.onResizePropertiesBottomStart}
                    onValueChange={panel.setPropertiesSizeClamped}
                  />
                )}
                {renderPropertiesPanel("bottom")}
              </div>
            ) : (
              renderDocumentPane()
            )}
          </main>
          {showRightColumn && renderHorizontalResize("right")}
          {showRightColumn && renderSideColumn("right")}
        </div>
        <PanelDropOverlay
          dragging={panel.draggingPanel}
          treeSide={panel.treeSide}
          treeWidth={panel.treeWidth}
          onDropTree={panel.dropTreeSide}
          onDropProperties={panel.dropPropertiesDock}
        />
      </div>
      <CreatePipelineDialog
        open={createPipelineOpen}
        initialTemplateId={createPipelineInitialTemplateId}
        onClose={() => {
          setCreatePipelineOpen(false);
          setCreatePipelineInitialTemplateId(undefined);
        }}
        onCreated={openCreatedPipeline}
      />
      {createGovArtifact ? (
        <CreateGovernanceArtifactDialog
          open
          context={createGovArtifact}
          onClose={() => setCreateGovArtifact(null)}
          onCreated={(rel) => {
            const workspace = createGovArtifact.kind;
            setGovernanceArtifactsRevision((prev) => ({
              token: prev.token + 1,
              workspace,
            }));
            setRefreshKey((k) => k + 1);
            openGovernanceWorkspaceTab(workspace, "artifacts", rel);
            setCreateGovArtifact(null);
          }}
        />
      ) : null}
      {saveAsTemplateState ? (
        <SavePipelineAsTemplateDialog
          open
          pipelineId={saveAsTemplateState.pipelineId}
          pipelineLabel={saveAsTemplateState.pipelineLabel}
          onClose={() => setSaveAsTemplateState(null)}
          onSaved={() => {
            setTransformTemplatesRevision((n) => n + 1);
            setSaveAsTemplateState(null);
          }}
        />
      ) : null}
      {renameTransformState ? (
        <RenameTransformLabelDialog
          open
          kind={renameTransformState.kind}
          resourceId={renameTransformState.id}
          currentLabel={renameTransformState.label}
          onClose={() => setRenameTransformState(null)}
          onRenamed={(newLabel) => {
            if (renameTransformState.kind === "pipeline") {
              applyPipelineRename(renameTransformState.id, newLabel);
            } else {
              applyTemplateRename(renameTransformState.id, newLabel);
            }
            setRenameTransformState(null);
          }}
        />
      ) : null}
    </div>
  );
}
