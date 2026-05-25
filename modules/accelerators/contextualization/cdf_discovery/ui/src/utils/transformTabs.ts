import type { EtlPipelineDocumentTab, EtlTemplateDocumentTab } from "../types/discoveryNodes";
import type { TransformCanvasDocument, TransformPipelineDocument } from "../types/transformCanvas";
import { TRANSFORM_PIPELINE_PREFIX, TRANSFORM_TEMPLATE_PREFIX } from "./treeNodeIds";
import type { EtlWorkflowYamlDocumentTab } from "../types/discoveryNodes";

const LEGACY_PIPELINE_ITEM_PREFIX = "transform:pipelines:item:";
const LEGACY_TEMPLATE_ITEM_PREFIX = "transform:pipelines:templates:item:";

export function etlPipelineTabKey(pipelineId: string, scopeSuffix = "all"): string {
  return `etl:pipeline:${scopeSuffix}:${pipelineId}`;
}

export function etlTemplateTabKey(templateId: string): string {
  return `etl:template:${templateId}`;
}

export function etlScopeTabKey(): string {
  return "transform:scope";
}

function decodeNodeSegment(seg: string): string {
  try {
    return decodeURIComponent(seg);
  } catch {
    return seg;
  }
}

function pipelineSegmentsFromNodeId(nodeId: string): { scopeSuffix: string; pipelineId: string } | null {
  if (!nodeId.startsWith(TRANSFORM_PIPELINE_PREFIX)) return null;
  const rest = nodeId.slice(TRANSFORM_PIPELINE_PREFIX.length);
  if (!rest) return null;
  const parts = rest.split(":").map(decodeNodeSegment);
  if (parts.length >= 2) {
    const pipelineId = parts[parts.length - 1];
    const scopeSuffix = parts.slice(0, -1).join(":");
    return pipelineId ? { scopeSuffix: scopeSuffix || "all", pipelineId } : null;
  }
  if (parts.length === 1) {
    return { scopeSuffix: "all", pipelineId: parts[0] };
  }
  return null;
}

function idAfterPrefix(nodeId: string, prefix: string, legacyPrefix: string): string | null {
  if (nodeId.startsWith(prefix)) {
    const raw = nodeId.slice(prefix.length);
    if (!raw) return null;
    try {
      return decodeURIComponent(raw);
    } catch {
      return raw;
    }
  }
  if (nodeId.startsWith(legacyPrefix)) {
    const raw = nodeId.slice(legacyPrefix.length);
    if (!raw) return null;
    try {
      return decodeURIComponent(raw);
    } catch {
      return raw;
    }
  }
  return null;
}

export function scopeSuffixFromNode(node: { id?: string; meta?: Record<string, unknown> }): string {
  const fromMeta = node.meta?.scope_suffix;
  if (typeof fromMeta === "string" && fromMeta.trim()) return fromMeta.trim();
  const parsed = pipelineSegmentsFromNodeId(node.id ?? "");
  return parsed?.scopeSuffix ?? "all";
}

export function pipelineIdFromNode(node: { id?: string; meta?: Record<string, unknown> }): string | null {
  const fromMeta = node.meta?.id;
  if (typeof fromMeta === "string" && fromMeta.trim()) return fromMeta.trim();
  const parsed = pipelineSegmentsFromNodeId(node.id ?? "");
  if (parsed) return parsed.pipelineId;
  const id = node.id ?? "";
  return idAfterPrefix(id, TRANSFORM_PIPELINE_PREFIX, LEGACY_PIPELINE_ITEM_PREFIX);
}

export function templateIdFromNode(node: { id?: string; meta?: Record<string, unknown> }): string | null {
  const fromMeta = node.meta?.id;
  if (typeof fromMeta === "string" && fromMeta.trim()) return fromMeta.trim();
  const id = node.id ?? "";
  return idAfterPrefix(id, TRANSFORM_TEMPLATE_PREFIX, LEGACY_TEMPLATE_ITEM_PREFIX);
}

export function pipelineLabelFromMeta(meta: Record<string, unknown> | undefined): string {
  const label = meta?.label;
  if (typeof label === "string" && label.trim()) return label.trim();
  const id = meta?.id;
  if (typeof id === "string" && id.trim()) return id.trim();
  return "Pipeline";
}

export function templateLabelFromMeta(meta: Record<string, unknown> | undefined): string {
  const label = meta?.label;
  if (typeof label === "string" && label.trim()) return label.trim();
  const id = meta?.id;
  if (typeof id === "string" && id.trim()) return id.trim();
  return "Template";
}

export function workflowYamlRelPathFromNode(node: {
  meta?: Record<string, unknown>;
}): string | null {
  const rel = node.meta?.rel_path;
  return typeof rel === "string" && rel.trim() ? rel.trim() : null;
}

export function workflowYamlTabKey(relPath: string): string {
  return `etl:workflow-yaml:${relPath}`;
}

export function createEtlWorkflowYamlTab(relPath: string, label: string): EtlWorkflowYamlDocumentTab {
  return {
    kind: "etl_workflow_yaml",
    id: workflowYamlTabKey(relPath),
    label,
    relPath,
    loading: true,
    error: null,
    dirty: false,
  };
}

/** Tree nodes that open a Transform document tab (scope, canvas, template, workflow YAML). */
export function opensTransformTab(node: { kind?: string; id?: string }): boolean {
  return (
    node.kind === "etl_scope" ||
    node.kind === "etl_pipeline" ||
    node.kind === "etl_template" ||
    node.kind === "etl_workflow_yaml"
  );
}

export function isTransformPipelineTreeNode(node: { id?: string; kind?: string }): boolean {
  return node.kind === "etl_pipeline" || (node.id?.startsWith(TRANSFORM_PIPELINE_PREFIX) ?? false);
}

export function isTransformTemplateTreeNode(node: { id?: string; kind?: string }): boolean {
  return node.kind === "etl_template" || (node.id?.startsWith(TRANSFORM_TEMPLATE_PREFIX) ?? false);
}

export function createEtlPipelineTab(
  pipelineId: string,
  label: string,
  canvas: TransformCanvasDocument | null = null,
  scopeSuffix = "all"
): EtlPipelineDocumentTab {
  return {
    kind: "etl_pipeline",
    id: etlPipelineTabKey(pipelineId, scopeSuffix),
    label,
    pipelineId,
    scopeSuffix,
    document: null,
    canvas,
    loading: true,
    error: null,
    dirty: false,
  };
}

export function pipelineDocumentToTab(
  doc: TransformPipelineDocument,
  tab: EtlPipelineDocumentTab
): EtlPipelineDocumentTab {
  return {
    ...tab,
    label: doc.label || tab.label,
    document: doc,
    canvas: doc.canvas ?? tab.canvas,
    loading: false,
    error: null,
    dirty: false,
  };
}

export function createEtlTemplateTab(
  templateId: string,
  label: string,
  canvas: TransformCanvasDocument | null = null
): EtlTemplateDocumentTab {
  return {
    kind: "etl_template",
    id: etlTemplateTabKey(templateId),
    label,
    templateId,
    document: null,
    canvas,
    loading: true,
    error: null,
    dirty: false,
  };
}

export function templateDocumentToTab(
  doc: Record<string, unknown>,
  tab: EtlTemplateDocumentTab
): EtlTemplateDocumentTab {
  const canvas = doc.canvas;
  const label = typeof doc.label === "string" ? doc.label : tab.label;
  return {
    ...tab,
    label: label || tab.label,
    document: doc,
    canvas:
      canvas && typeof canvas === "object" && !Array.isArray(canvas)
        ? (canvas as TransformCanvasDocument)
        : tab.canvas,
    loading: false,
    error: null,
    dirty: false,
  };
}
