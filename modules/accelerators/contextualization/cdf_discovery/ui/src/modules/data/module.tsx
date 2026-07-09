import type { ReactNode } from "react";
import { DataModelFlowPane } from "../../components/DataModelFlowPane";
import { RecordsStreamDocumentTab as RecordsStreamPane } from "../../components/RecordsStreamDocumentTab";
import { SqlQueryPane } from "../../components/SqlQueryPane";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import type { DataModelGraphView, DocumentTab, TreeNode } from "../../types/discoveryNodes";
import { dataModelTabKey, dataModelTabLabel, dataModelRefFromNode } from "../../utils/dataModelTabs";
import { savedQueryFromNode } from "../../utils/savedQueries";
import { canQueryTreeNode } from "../../utils/sqlQuerySeed";
import { DATA_ROOT } from "../../utils/treeNodeIds";
import {
  isDataModelTab,
  isRecordsStreamTab,
  isSqlTab,
} from "../../types/discoveryNodes";

function ownsDataNode(node: TreeNode): boolean {
  if (node.kind === "saved_query" || node.kind === "dm_data_model") return true;
  if (node.kind === "record_stream" && node.open_target?.type === "record_stream") return true;
  return canQueryTreeNode(node);
}

function tryOpenDataNode(node: TreeNode, ctx: OpenNodeContext): boolean {
  if (node.kind === "saved_query") {
    const query = savedQueryFromNode(node);
    if (query) {
      ctx.openSavedQuery(query);
      return true;
    }
    return false;
  }
  if (node.kind === "dm_data_model") {
    const ref = dataModelRefFromNode(node);
    if (!ref) return false;
    const id = dataModelTabKey(ref);
    ctx.setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        ctx.setActiveTabId(id);
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
      ctx.setActiveTabId(id);
      return [...prev, tab];
    });
    ctx.setRowDetail(null);
    return true;
  }
  if (node.kind === "record_stream" && node.open_target?.type === "record_stream") {
    ctx.openRecordsStreamTab(node.open_target.stream_external_id, node.label);
    return true;
  }
  if (canQueryTreeNode(node) && node.open_target) {
    ctx.openSqlForOpenTarget(node.open_target, node.label);
    return true;
  }
  return false;
}

function renderDataTab(tab: DocumentTab, ctx: RenderTabContext): ReactNode | null {
  if (isDataModelTab(tab)) {
    return (
      <DataModelFlowPane
        tab={tab}
        onTabUpdate={ctx.updateDataModelTab}
        onQueryView={(view: DataModelGraphView) => ctx.queryDmView(view)}
      />
    );
  }
  if (isRecordsStreamTab(tab)) {
    return (
      <RecordsStreamPane
        tab={tab}
        onTabUpdate={ctx.updateRecordsStreamTab}
        onSelectRow={(row) => ctx.setRowDetail(row)}
      />
    );
  }
  if (isSqlTab(tab)) {
    return (
      <SqlQueryPane
        tab={tab}
        onTabUpdate={ctx.updateSqlTab}
        onSelectRow={(row) => ctx.setRowDetail(row)}
        onQueryFile={ctx.openFileContentQueryFromRow}
        onDownloadFile={ctx.downloadFileFromRow}
        onSave={tab.engine === "file_content" ? undefined : ctx.saveSqlTab ? () => ctx.saveSqlTab!(tab, "save") : undefined}
        onSaveAs={tab.engine === "file_content" ? undefined : ctx.saveSqlTab ? () => ctx.saveSqlTab!(tab, "saveAs") : undefined}
      />
    );
  }
  return null;
}

export const dataModule: DiscoveryModule = {
  id: "data",
  treeRootId: DATA_ROOT,
  labelKey: "tree.desc.data",
  ownsNode: ownsDataNode,
  tryOpenNode: tryOpenDataNode,
  ownsTab: (tab) => isDataModelTab(tab) || isRecordsStreamTab(tab) || isSqlTab(tab),
  renderTab: renderDataTab,
};
