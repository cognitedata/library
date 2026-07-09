import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  deleteTransformWorkflow,
  deleteTransformTemplate,
  fetchConnection,
  fetchTransformWorkflowByWorkflow,
  importTransformWorkflowFromWorkflow,
  type ConnectionInfo,
} from "./api";
import { DocumentTabBar } from "./components/DocumentTabBar";
import { DocumentTabFullscreenOverlay } from "./components/DocumentTabFullscreenOverlay";
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
import { CreatePipelineDialog } from "./components/transform/CreatePipelineDialog";
import { RenameTransformLabelDialog } from "./components/transform/RenameTransformLabelDialog";
import { SavePipelineAsTemplateDialog } from "./components/transform/SavePipelineAsTemplateDialog";
import { CreateGovernanceArtifactDialog } from "./components/governance/CreateGovernanceArtifactDialog";
import { CogniteLogo } from "./components/CogniteLogo";
import { useAppSettings } from "./context/AppSettingsContext";
import { useDiscoveryConfig } from "./context/DiscoveryConfigContext";
import { openTargetFromSqlTabId } from "./utils/workspacePersistence";
import { dmInstanceKindFromOpenTarget } from "./utils/dmInstanceFromRow";
import { LOCALES } from "./i18n";
import {
  isSqlTab,
  isRecordsStreamTab,
  isGovernanceSpacesTab,
  isGovernanceGroupsTab,
  isEtlPipelineTab,
  isEtlTemplateTab,
  isEtlWorkflowYamlTab,
  type DataModelDocumentTab,
  type GovernanceSubTab,
  type GovernanceSpacesDocumentTab,
  type GovernanceGroupsDocumentTab,
  type DataModelGraphView,
  type DocumentTab,
  type FunctionDocumentTab,
  type WorkflowDocumentTab,
  type WorkflowRef,
  type TransformationDocumentTab,
  type EtlPipelineDocumentTab,
  type EtlTemplateDocumentTab,
  type OpenTarget,
  type SqlDocumentTab,
  type RecordsStreamDocumentTab,
  type TreeNode,
} from "./types/discoveryNodes";
import {
  createSqlTab,
  createSqlTabForOpenTarget,
  createFileContentSqlTab,
  SQL_WORKSPACE_TAB_ID,
  sqlTabKeyForOpenTarget,
} from "./utils/sqlTabs";
import { createRecordsStreamTab } from "./utils/recordsStreamTabs";
import { labelForDmView, openTargetForDmView } from "./utils/sqlQuerySeed";
import {
  nodePreviewOpenTarget,
  resolvePreviewRawSink,
  sqlQueryForPreviewNode,
} from "./utils/nodePreviewQuery";
import type { TransformWorkflowParameters } from "./types/transformCanvas";
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
  etlPipelineTabKey,
  etlTemplateTabKey,
  opensTransformTab,
  pipelineIdFromNode,
  templateIdFromNode,
} from "./utils/transformTabs";
import {
  type TransformTabRunSessionPatch,
  withTransformTabRunSession,
} from "./types/transformTabRun";
import {
  createSettingsTab,
  opensExtractTab,
  opensMonitorTab,
} from "./utils/workspaceTabs";
import { opensGovernanceCdfDetailTab, opensGovernanceTab } from "./utils/governanceTabs";
import type { GovernanceArtifactCreateContext } from "./utils/governanceTreeNew";
import { restoreWorkspaceTabs, serializeWorkspace } from "./utils/workspacePersistence";
import type { SavedQuery } from "./types/discoveryNodes";
import { DISCOVERY_MODULES } from "./modules/discoveryModules";
import {
  openModuleNode,
  renderModuleTab,
} from "./shell/discoveryShellDispatch";
import type { OpenNodeContext, RenderTabContext } from "./shell/discoveryShell";
import { invertedIndexKindFromNode } from "./modules/invertedIndex/utils/indexTabs";

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

  const openCreatedPipeline = useCallback(
    (pipelineId: string, label: string, scopeSuffix = "") => {
      setTransformPipelinesRevision((n) => n + 1);
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
    },
    []
  );

  const [openInTransformBusyId, setOpenInTransformBusyId] = useState<string | null>(null);
  const [openInTransformError, setOpenInTransformError] = useState<string | null>(null);

  const openWorkflowInTransform = useCallback(
    async (ref: WorkflowRef) => {
      const wfId = ref.external_id.trim();
      if (!wfId) return;
      setOpenInTransformError(null);
      setOpenInTransformBusyId(wfId);
      try {
        const result = await importTransformWorkflowFromWorkflow({
          workflow_external_id: wfId,
          version: ref.version,
        });
        const scopeSuffix = result.scope_suffix?.trim() ?? "";
        const label =
          (typeof result.workflow.label === "string" && result.workflow.label.trim()) ||
          result.workflow_id;
        openCreatedPipeline(result.workflow_id, label, scopeSuffix);
        setRowDetail(null);
      } catch (e) {
        setOpenInTransformError(String(e));
      } finally {
        setOpenInTransformBusyId(null);
      }
    },
    [openCreatedPipeline]
  );

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

  const openSettingsTab = useCallback(() => {
    const id = "settings";
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        setActiveTabId(id);
        return prev;
      }
      const tab = createSettingsTab(t("settings.title"));
      setActiveTabId(id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, [t]);

  useEffect(() => {
    if (configLoading || workspaceRestored.current) return;
    workspaceRestored.current = true;
    if (workspace.tabs.length) {
      const restored = restoreWorkspaceTabs(
        workspace,
        t("sql.title"),
        t("governance.tree.instanceSpaces")
      );
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
        await deleteTransformWorkflow(pipelineId);
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

  const deleteWorkflowInTransform = useCallback(
    async (ref: WorkflowRef, fallbackLabel: string) => {
      const wfId = ref.external_id.trim();
      if (!wfId) return;
      try {
        const found = await fetchTransformWorkflowByWorkflow(wfId);
        const label =
          (typeof found.workflow.label === "string" && found.workflow.label.trim()) ||
          fallbackLabel ||
          found.workflow_id;
        await deletePipeline(found.workflow_id, label);
      } catch (e) {
        window.alert(`${t("transform.pipelines.deleteFailed")}: ${String(e)}`);
      }
    },
    [deletePipeline, t]
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

  const bumpTransformPipelinesTree = useCallback(() => {
    setTransformPipelinesRevision((n) => n + 1);
  }, []);

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

  const openNodePreviewQuery = useCallback(
    (
      pipelineId: string,
      previewNodeId: string,
      parameters: TransformWorkflowParameters | null | undefined,
      previewNodeConfig: Record<string, unknown> | null | undefined,
      runId: string | null | undefined
    ) => {
      const { rawDb, previewTable } = resolvePreviewRawSink(parameters, previewNodeConfig);
      const rid = String(runId ?? "").trim();
      const target = nodePreviewOpenTarget({
        pipelineId,
        previewNodeId,
        rawDb,
        previewTable,
        runId: rid,
      });
      const query = sqlQueryForPreviewNode({
        rawDb,
        previewTable,
        runId: rid,
        previewNodeId,
        noRunComment: t("transform.nodePreview.noRunSqlComment"),
      });
      const tabId = sqlTabKeyForOpenTarget(target);
      const label = `Preview: ${previewNodeId}`;
      const tab = createSqlTab({ id: tabId, label, query });
      setTabs((prev) => {
        const existing = prev.find((row) => row.id === tab.id);
        if (existing && isSqlTab(existing)) {
          setActiveTabId(tab.id);
          return prev.map((row) =>
            row.id === tab.id ? { ...existing, query: tab.query, label: tab.label } : row
          );
        }
        setActiveTabId(tab.id);
        return [...prev, tab];
      });
      setRowDetail(null);
    },
    [t]
  );

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
    (which: "spaces" | "groups", subTab: GovernanceSubTab, artifactRel?: string | null) => {
      const id = which === "spaces" ? "gov:spaces" : "gov:groups";
      const label =
        which === "spaces" ? t("governance.tree.instanceSpaces") : t("fusion.tree.groups");
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
    [t]
  );

  const openDiscoveryNode = useCallback(
    (node: TreeNode) => {
      const ctx: OpenNodeContext = {
        setTabs,
        setActiveTabId,
        setRowDetail,
        t,
        openSavedQuery,
        openGovernanceWorkspaceTab,
        openRecordsStreamTab,
        openSqlForOpenTarget,
      };
      openModuleNode(node, ctx, DISCOVERY_MODULES);
    },
    [openGovernanceWorkspaceTab, openRecordsStreamTab, openSavedQuery, openSqlForOpenTarget, t]
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

  const selectTab = useCallback(
    (id: string) => {
      if (id === activeTabId) return;
      if (
        activeTab &&
        (isEtlPipelineTab(activeTab) || isEtlTemplateTab(activeTab) || isEtlWorkflowYamlTab(activeTab)) &&
        activeTab.dirty
      ) {
        window.alert(t("tabs.saveBeforeSwitch"));
        return;
      }
      setActiveTabId(id);
    },
    [activeTab, activeTabId, t]
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
    const ctx: RenderTabContext = {
      setTabs,
      setRowDetail,
      t,
      updateDataModelTab,
      updateSqlTab,
      updateRecordsStreamTab,
      updateTransformationTab,
      updateFunctionTab,
      updateWorkflowTab,
      updateEtlDocumentTab,
      patchEtlTabRunSession,
      queryDmView,
      openWorkflowInTransform,
      openInTransformBusyId,
      openInTransformError,
      setTransformPipelinesRevision,
      setGovernanceArtifactsRevision,
      bumpTransformPipelinesTree,
      deletePipeline,
      deleteTemplate,
      openRenamePipeline,
      openRenameTemplate,
      onTransformCopyCreated,
      openNodePreviewQuery,
      openFileContentQueryFromRow,
      downloadFileFromRow,
      saveSqlTab: (sqlTab, mode) => void saveSqlTab(sqlTab, mode),
    };
    const rendered = renderModuleTab(tab, ctx, DISCOVERY_MODULES);
    if (rendered != null) {
      return rendered;
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
          onSelect={selectTab}
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
                opensMonitorTab(node) ||
                invertedIndexKindFromNode(node) != null)
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
          onOpenWorkflowInTransform={(ref) => void openWorkflowInTransform(ref)}
          onDeleteWorkflowInTransform={(ref, label) =>
            void deleteWorkflowInTransform(ref, label)
          }
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
            <span className="disc-theme-toggle" role="group" aria-label={t("controls.theme")}>
              <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                {t("controls.themeLight")}
              </button>
              <button type="button" data-active={theme === "system"} onClick={() => setTheme("system")}>
                {t("controls.themeSystem")}
              </button>
              <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                {t("controls.themeDark")}
              </button>
            </span>
          </label>
          <label className="disc-toolbar__control" title={t("controls.language.tooltip")}>
            <span className="disc-toolbar__control-label">{t("controls.language")}</span>
            <span className="disc-toolbar__locale-settings">
              <select value={locale} onChange={(e) => setLocale(e.target.value as typeof locale)}>
                {LOCALES.map(({ code, label }) => (
                  <option key={code} value={code}>
                    {label}
                  </option>
                ))}
              </select>
              <button
                type="button"
                className="disc-btn disc-toolbar__settings-btn"
                title={t("controls.settings.tooltip")}
                aria-label={t("controls.settings")}
                onClick={openSettingsTab}
              >
                ⚙
              </button>
            </span>
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
