import type { ReactNode } from "react";
import { fetchTransformWorkflowByWorkflow } from "../../api";
import { FunctionPane } from "../../components/FunctionPane";
import { TransformationPane } from "../../components/TransformationPane";
import { TransformFusionWorkflowPane } from "../../components/transform/TransformFusionWorkflowPane";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import type { DocumentTab, TreeNode, WorkflowDocumentTab } from "../../types/discoveryNodes";
import {
  createFunctionTab,
  functionIdFromNode,
  functionLabelFromMeta,
  functionTabKey,
} from "../../utils/functionTabs";
import {
  createTransformationTab,
  transformationIdFromNode,
  transformationLabelFromMeta,
  transformationTabKey,
} from "../../utils/transformationTabs";
import {
  createEtlPipelineTab,
  etlPipelineTabKey,
  normalizePipelineScopeSuffix,
} from "../../utils/transformTabs";
import { workflowRefFromNode, workflowTabKey, workflowTabLabel } from "../../utils/workflowTabs";
import { FUSION_ROOT } from "../../utils/treeNodeIds";
import { isFunctionTab, isTransformationTab, isWorkflowTab } from "../../types/discoveryNodes";

export const fusionModule: DiscoveryModule = {
  id: "fusion",
  treeRootId: FUSION_ROOT,
  labelKey: "tree.desc.fusion",

  ownsNode(node: TreeNode): boolean {
    return node.kind === "workflow" || node.kind === "function" || node.kind === "transformation";
  },

  tryOpenNode(node: TreeNode, ctx: OpenNodeContext): boolean {
    if (node.kind === "workflow") {
      const ref = workflowRefFromNode(node);
      if (!ref) return false;
      void (async () => {
        try {
          const found = await fetchTransformWorkflowByWorkflow(ref.external_id);
          const scopeSuffix = normalizePipelineScopeSuffix(found.scope_suffix);
          const pipelineId = found.workflow_id;
          const label =
            (typeof found.workflow.label === "string" && found.workflow.label.trim()) ||
            pipelineId;
          const id = etlPipelineTabKey(pipelineId, scopeSuffix);
          ctx.setTabs((prev) => {
            const existing = prev.find((tab) => tab.id === id);
            if (existing) {
              ctx.setActiveTabId(id);
              return prev;
            }
            const tab = createEtlPipelineTab(pipelineId, label, null, scopeSuffix);
            ctx.setActiveTabId(id);
            return [...prev, tab];
          });
          ctx.setRowDetail(null);
        } catch {
          const id = workflowTabKey(ref);
          ctx.setTabs((prev) => {
            const existing = prev.find((tab) => tab.id === id);
            if (existing) {
              ctx.setActiveTabId(id);
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
            ctx.setActiveTabId(id);
            return [...prev, tab];
          });
          ctx.setRowDetail(null);
        }
      })();
      return true;
    }
    if (node.kind === "function") {
      const fnId = functionIdFromNode(node);
      if (fnId == null) return false;
      const id = functionTabKey(fnId);
      const label = functionLabelFromMeta(node.meta);
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        const tab = createFunctionTab(fnId, label);
        ctx.setActiveTabId(id);
        return [...prev, tab];
      });
      ctx.setRowDetail(null);
      return true;
    }
    if (node.kind === "transformation") {
      const txId = transformationIdFromNode(node);
      if (txId == null) return false;
      const id = transformationTabKey(txId);
      const label = transformationLabelFromMeta(node.meta);
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        const tab = createTransformationTab(txId, label);
        ctx.setActiveTabId(id);
        return [...prev, tab];
      });
      ctx.setRowDetail(null);
      return true;
    }
    return false;
  },

  ownsTab(tab: DocumentTab): boolean {
    return isWorkflowTab(tab) || isTransformationTab(tab) || isFunctionTab(tab);
  },

  renderTab(tab: DocumentTab, ctx: RenderTabContext): ReactNode | null {
    if (isWorkflowTab(tab)) {
      return (
        <TransformFusionWorkflowPane
          key={tab.id}
          tab={tab}
          onTabUpdate={ctx.updateWorkflowTab}
          onOpenInTransform={() => void ctx.openWorkflowInTransform(tab.workflow)}
          onDeleteInTransform={() => ctx.setTransformPipelinesRevision((n) => n + 1)}
          openInTransformBusy={ctx.openInTransformBusyId === tab.workflow.external_id}
          openInTransformError={ctx.openInTransformError}
        />
      );
    }
    if (isTransformationTab(tab)) {
      return (
        <TransformationPane
          tab={tab}
          onTabUpdate={ctx.updateTransformationTab}
          onSelectRow={(row) => ctx.setRowDetail(row)}
          onQueryFile={ctx.openFileContentQueryFromRow}
          onDownloadFile={ctx.downloadFileFromRow}
        />
      );
    }
    if (isFunctionTab(tab)) {
      return <FunctionPane tab={tab} onTabUpdate={ctx.updateFunctionTab} />;
    }
    return null;
  },
};
