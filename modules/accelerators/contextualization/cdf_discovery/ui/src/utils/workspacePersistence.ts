import type {
  DataModelRef,
  DocumentTab,
  SavedWorkspace,
  SavedWorkspaceTab,
  SqlDocumentTab,
  WorkflowRef,
} from "../types/discoveryNodes";
import { dataModelTabKey, dataModelTabLabel } from "./dataModelTabs";
import { createFunctionTab } from "./functionTabs";
import { createTransformationTab } from "./transformationTabs";
import { createSqlTabFromSavedQuery, savedQueryIdFromTabId } from "./savedQueries";
import { createSqlTab, createSqlTabForOpenTarget, createFileContentSqlTab, SQL_WORKSPACE_TAB_ID } from "./sqlTabs";
import { workflowTabKey, workflowTabLabel } from "./workflowTabs";

function openTargetFromSqlTabId(tabId: string) {
  if (!tabId.startsWith("sql:") || tabId === SQL_WORKSPACE_TAB_ID) return null;
  const key = tabId.slice(4);
  if (key.startsWith("classic:")) {
    return { type: "classic_list" as const, resource_type: key.slice("classic:".length) };
  }
  if (key.startsWith("raw:")) {
    const body = key.slice("raw:".length);
    const colon = body.indexOf(":");
    if (colon < 0) return null;
    return {
      type: "raw_rows" as const,
      database: body.slice(0, colon),
      table: body.slice(colon + 1),
    };
  }
  if (key.startsWith("dm:")) {
    const body = key.slice("dm:".length);
    const parts = body.split(":");
    if (parts.length >= 3) {
      return {
        type: "dm_instances" as const,
        view_space: parts[0],
        view_external_id: parts[1],
        view_version: parts.slice(2).join(":"),
      };
    }
  }
  if (key.startsWith("fusion:seq:")) {
    const externalId = decodeURIComponent(key.slice("fusion:seq:".length));
    if (externalId) {
      return { type: "fusion_sequence" as const, sequence_external_id: externalId };
    }
  }
  if (key === "fusion:dm:nodes") {
    return { type: "fusion_dm_all" as const, entity: "nodes" as const };
  }
  if (key === "fusion:dm:edges") {
    return { type: "fusion_dm_all" as const, entity: "edges" as const };
  }
  if (key.startsWith("fusion:type:")) {
    const body = key.slice("fusion:type:".length);
    const parts = body.split(":").map((p) => decodeURIComponent(p));
    if (parts.length >= 4) {
      return {
        type: "fusion_data_model" as const,
        model_space: parts[0],
        model_external_id: parts[1],
        model_version: parts[2],
        type_external_id: parts.slice(3).join(":"),
      };
    }
  }
  if (key.startsWith("fusion:")) {
    const resource = key.slice("fusion:".length);
    if (resource && resource !== "seq" && !resource.startsWith("type:")) {
      return { type: "fusion_cdf" as const, resource };
    }
  }
  return null;
}

export function serializeWorkspace(
  tabs: DocumentTab[],
  activeTabId: string | null
): SavedWorkspace {
  const saved: SavedWorkspaceTab[] = [];

  for (const tab of tabs) {
    if (tab.kind === "sql") {
      const entry: Extract<SavedWorkspaceTab, { kind: "sql" }> = {
        kind: "sql",
        id: tab.id,
        label: tab.label,
        query: tab.query,
        limit: tab.limit,
        convert_to_string: tab.convertToString,
      };
      if (tab.sourceLimit != null) entry.source_limit = tab.sourceLimit;
      if (tab.timeoutSec != null) entry.timeout = tab.timeoutSec;
      if (tab.savedQueryId) entry.saved_query_id = tab.savedQueryId;
      if (tab.engine === "file_content" && tab.fileContent) {
        entry.engine = "file_content";
        entry.file_content = tab.fileContent;
      }
      saved.push(entry);
    } else if (tab.kind === "data_model") {
      saved.push({
        kind: "data_model",
        id: tab.id,
        label: tab.label,
        space: tab.dataModel.space,
        external_id: tab.dataModel.external_id,
        version: tab.dataModel.version,
        name: tab.dataModel.name,
      });
    } else if (tab.kind === "transformation") {
      saved.push({
        kind: "transformation",
        id: tab.id,
        label: tab.label,
        transformation_id: tab.transformationId,
      });
    } else if (tab.kind === "function") {
      saved.push({
        kind: "function",
        id: tab.id,
        label: tab.label,
        function_id: tab.functionId,
      });
    } else if (tab.kind === "workflow") {
      saved.push({
        kind: "workflow",
        id: tab.id,
        label: tab.label,
        external_id: tab.workflow.external_id,
        version: tab.workflow.version,
        name: tab.workflow.name,
      });
    } else if (tab.kind === "governance_scope") {
      saved.push({ kind: "governance_scope", id: tab.id, label: tab.label });
    } else if (tab.kind === "governance_spaces") {
      saved.push({
        kind: "governance_spaces",
        id: tab.id,
        label: tab.label,
        active_sub_tab: tab.activeSubTab,
        artifact_rel: tab.artifactRel,
      });
    } else if (tab.kind === "governance_groups") {
      saved.push({
        kind: "governance_groups",
        id: tab.id,
        label: tab.label,
        active_sub_tab: tab.activeSubTab,
        artifact_rel: tab.artifactRel,
      });
    } else if (tab.kind === "governance_cdf_space") {
      saved.push({
        kind: "governance_cdf_space",
        id: tab.id,
        label: tab.label,
        space: tab.space,
      });
    } else if (tab.kind === "governance_cdf_group") {
      saved.push({
        kind: "governance_cdf_group",
        id: tab.id,
        label: tab.label,
        group_id: tab.groupId,
      });
    }
  }

  const active =
    activeTabId && saved.some((t) => t.id === activeTabId) ? activeTabId : saved[0]?.id ?? null;

  return { active_tab_id: active, tabs: saved };
}

function restoreSqlTab(
  saved: Extract<SavedWorkspaceTab, { kind: "sql" }>,
  sqlWorkspaceLabel: string
): SqlDocumentTab {
  const savedId = saved.saved_query_id ?? savedQueryIdFromTabId(saved.id);
  if (savedId) {
    const tab = createSqlTabFromSavedQuery({
      id: savedId,
      name: saved.label?.trim() || savedId,
      query: saved.query,
      limit: saved.limit ?? 100,
      convert_to_string: saved.convert_to_string ?? true,
      source_limit: saved.source_limit,
      timeout: saved.timeout,
    });
    return tab;
  }

  let tab: SqlDocumentTab;
  if (saved.id === SQL_WORKSPACE_TAB_ID) {
    tab = createSqlTab({
      id: saved.id,
      label: saved.label?.trim() || sqlWorkspaceLabel,
      query: saved.query,
    });
  } else {
    const target = openTargetFromSqlTabId(saved.id);
    const fromTarget = target ? createSqlTabForOpenTarget(target, saved.label) : null;
    if (fromTarget) {
      tab = { ...fromTarget, query: saved.query || fromTarget.query };
    } else {
      tab = createSqlTab({
        id: saved.id,
        label: saved.label?.trim() || "SQL Query",
        query: saved.query,
      });
    }
  }
  tab.limit = saved.limit ?? tab.limit;
  tab.convertToString = saved.convert_to_string ?? tab.convertToString;
  if (saved.source_limit != null) tab.sourceLimit = saved.source_limit;
  if (saved.timeout != null) tab.timeoutSec = saved.timeout;
  if (saved.label?.trim()) tab.label = saved.label.trim();
  if (saved.engine === "file_content" && saved.file_content) {
    const fileTab = createFileContentSqlTab(saved.file_content, tab.label);
    fileTab.id = saved.id;
    fileTab.query = saved.query || fileTab.query;
    fileTab.limit = saved.limit ?? fileTab.limit;
    fileTab.convertToString = saved.convert_to_string ?? fileTab.convertToString;
    if (saved.source_limit != null) fileTab.sourceLimit = saved.source_limit;
    if (saved.timeout != null) fileTab.timeoutSec = saved.timeout;
    return fileTab;
  }
  return tab;
}

export function restoreWorkspaceTabs(
  workspace: SavedWorkspace,
  sqlWorkspaceLabel: string
): { tabs: DocumentTab[]; activeTabId: string | null } {
  const tabs: DocumentTab[] = [];

  for (const saved of workspace.tabs) {
    if (saved.kind === "sql") {
      tabs.push(restoreSqlTab(saved, sqlWorkspaceLabel));
    } else if (saved.kind === "data_model") {
      const ref: DataModelRef = {
        space: saved.space,
        external_id: saved.external_id,
        version: saved.version,
        name: saved.name,
      };
      tabs.push({
        kind: "data_model",
        id: saved.id || dataModelTabKey(ref),
        label: saved.label?.trim() || dataModelTabLabel(ref),
        dataModel: ref,
        graph: null,
        loading: true,
        error: null,
      });
    } else if (saved.kind === "transformation") {
      tabs.push(
        createTransformationTab(
          saved.transformation_id,
          saved.label?.trim() || `Transformation ${saved.transformation_id}`
        )
      );
      const last = tabs[tabs.length - 1];
      if (last.kind === "transformation") last.id = saved.id;
    } else if (saved.kind === "function") {
      tabs.push(createFunctionTab(saved.function_id, saved.label?.trim() || "Function"));
      const last = tabs[tabs.length - 1];
      if (last.kind === "function") last.id = saved.id;
    } else if (saved.kind === "workflow") {
      const ref: WorkflowRef = {
        external_id: saved.external_id,
        version: saved.version,
        name: saved.name,
      };
      tabs.push({
        kind: "workflow",
        id: saved.id || workflowTabKey(ref),
        label: saved.label?.trim() || workflowTabLabel(ref),
        workflow: ref,
        graph: null,
        loading: true,
        error: null,
      });
    } else if (saved.kind === "governance_scope") {
      tabs.push({
        kind: "governance_scope",
        id: "gov:scope",
        label: saved.label?.trim() || "Governance",
      });
    } else if (saved.kind === "governance_spaces") {
      tabs.push({
        kind: "governance_spaces",
        id: "gov:spaces",
        label: saved.label?.trim() || "Spaces",
        activeSubTab: saved.active_sub_tab ?? "configure",
        artifactRel: saved.artifact_rel ?? null,
      });
    } else if (saved.kind === "governance_groups") {
      tabs.push({
        kind: "governance_groups",
        id: "gov:groups",
        label: saved.label?.trim() || "Groups",
        activeSubTab: saved.active_sub_tab ?? "configure",
        artifactRel: saved.artifact_rel ?? null,
      });
    } else if (saved.kind === "governance_cdf_space") {
      tabs.push({
        kind: "governance_cdf_space",
        id: saved.id,
        label: saved.label?.trim() || saved.space,
        space: saved.space,
        detail: null,
        loading: true,
        error: null,
      });
    } else if (saved.kind === "governance_cdf_group") {
      tabs.push({
        kind: "governance_cdf_group",
        id: saved.id,
        label: saved.label?.trim() || `Group ${saved.group_id}`,
        groupId: saved.group_id,
        detail: null,
        loading: true,
        error: null,
      });
    }
  }

  const active =
    workspace.active_tab_id && tabs.some((t) => t.id === workspace.active_tab_id)
      ? workspace.active_tab_id
      : tabs[0]?.id ?? null;

  return { tabs, activeTabId: active };
}
