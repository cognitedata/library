import type { ReactNode } from "react";
import { TransformPipelinePane } from "../../components/transform/TransformPipelinePane";
import { TransformWorkflowYamlPane } from "../../components/transform/TransformWorkflowYamlPane";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import type { DocumentTab, TreeNode } from "../../types/discoveryNodes";
import type { TransformWorkflowParameters } from "../../types/transformCanvas";
import {
  createEtlPipelineTab,
  createEtlTemplateTab,
  createEtlWorkflowYamlTab,
  etlPipelineTabKey,
  etlTemplateTabKey,
  opensTransformTab,
  pipelineIdFromNode,
  pipelineLabelFromMeta,
  scopeSuffixFromNode,
  templateIdFromNode,
  templateLabelFromMeta,
  workflowYamlRelPathFromNode,
  workflowYamlTabKey,
} from "../../utils/transformTabs";
import { TRANSFORM_ROOT } from "../../utils/treeNodeIds";
import {
  isEtlPipelineTab,
  isEtlTemplateTab,
  isEtlWorkflowYamlTab,
} from "../../types/discoveryNodes";

export const transformModule: DiscoveryModule = {
  id: "transform",
  treeRootId: TRANSFORM_ROOT,
  labelKey: "tree.desc.transform",

  ownsNode(node: TreeNode): boolean {
    return opensTransformTab(node);
  },

  tryOpenNode(node: TreeNode, ctx: OpenNodeContext): boolean {
    if (node.kind === "etl_pipeline") {
      const pipelineId = pipelineIdFromNode(node);
      if (!pipelineId) return false;
      const scopeSuffix = scopeSuffixFromNode(node);
      const id = etlPipelineTabKey(pipelineId, scopeSuffix);
      const label = pipelineLabelFromMeta(node.meta);
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
      return true;
    }
    if (node.kind === "etl_template") {
      const templateId = templateIdFromNode(node);
      if (!templateId) return false;
      const id = etlTemplateTabKey(templateId);
      const label = templateLabelFromMeta(node.meta);
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        const tab = createEtlTemplateTab(templateId, label);
        ctx.setActiveTabId(id);
        return [...prev, tab];
      });
      ctx.setRowDetail(null);
      return true;
    }
    if (node.kind === "etl_workflow_yaml") {
      const relPath = workflowYamlRelPathFromNode(node);
      if (!relPath) return false;
      const id = workflowYamlTabKey(relPath);
      const label = node.label?.trim() || relPath.split("/").pop() || relPath;
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        const tab = createEtlWorkflowYamlTab(relPath, label);
        ctx.setActiveTabId(id);
        return [...prev, tab];
      });
      ctx.setRowDetail(null);
      return true;
    }
    return false;
  },

  ownsTab(tab: DocumentTab): boolean {
    return isEtlPipelineTab(tab) || isEtlTemplateTab(tab) || isEtlWorkflowYamlTab(tab);
  },

  renderTab(tab: DocumentTab, ctx: RenderTabContext): ReactNode | null {
    if (isEtlPipelineTab(tab)) {
      return (
        <TransformPipelinePane
          key={tab.id}
          tab={tab}
          onTabUpdate={ctx.updateEtlDocumentTab}
          onRunSessionPatch={ctx.patchEtlTabRunSession}
          onCopyCreated={ctx.onTransformCopyCreated}
          onBuildComplete={(result) => {
            if (result.ok) ctx.bumpTransformPipelinesTree();
          }}
          onDelete={() => void ctx.deletePipeline(tab.pipelineId, tab.label)}
          onRename={() => ctx.openRenamePipeline(tab.pipelineId, tab.label)}
          onOpenNodePreviewQuery={(node) =>
            ctx.openNodePreviewQuery(
              tab.pipelineId,
              node.id,
              tab.document?.parameters as TransformWorkflowParameters | null | undefined,
              (node.data as { config?: Record<string, unknown> } | undefined)?.config,
              tab.runSession?.lastRun?.run_id
            )
          }
        />
      );
    }
    if (isEtlTemplateTab(tab)) {
      return (
        <TransformPipelinePane
          key={tab.id}
          editorKind="template"
          tab={tab}
          onTabUpdate={ctx.updateEtlDocumentTab}
          onRunSessionPatch={ctx.patchEtlTabRunSession}
          onCopyCreated={ctx.onTransformCopyCreated}
          onBuildComplete={(result) => {
            if (result.ok) ctx.bumpTransformPipelinesTree();
          }}
          onDelete={() => void ctx.deleteTemplate(tab.templateId, tab.label)}
          onRename={() => ctx.openRenameTemplate(tab.templateId, tab.label)}
          onOpenNodePreviewQuery={(node) =>
            ctx.openNodePreviewQuery(
              tab.templateId,
              node.id,
              tab.document?.parameters as TransformWorkflowParameters | null | undefined,
              (node.data as { config?: Record<string, unknown> } | undefined)?.config,
              tab.runSession?.lastRun?.run_id
            )
          }
        />
      );
    }
    if (isEtlWorkflowYamlTab(tab)) {
      return (
        <TransformWorkflowYamlPane
          key={tab.id}
          tab={tab}
          onTabUpdate={(updated) =>
            ctx.setTabs((prev) => prev.map((row) => (row.id === updated.id ? updated : row)))
          }
        />
      );
    }
    return null;
  },
};
