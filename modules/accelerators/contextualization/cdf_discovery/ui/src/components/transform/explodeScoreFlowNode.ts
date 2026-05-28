import type { Edge, Node } from "@xyflow/react";
import type { JsonObject } from "../../types/jsonConfig";
import type { TransformCanvasHandleOrientation } from "../../types/transformCanvas";
import { rfTypeToKind } from "../../types/transformCanvas";
import { readScoreFields, readScoringRules } from "../../utils/scoreNodeConfigModel";
import { wireEdgesForExplodedNodes } from "./explodeFlowNodeEdges";
import { nextEtlNodeId } from "./flowNodeRegistry";
import { withEtlNodeDimensions } from "./etlFlowNodeSizing";

const NODE_SPACING_LR = 220;
const NODE_SPACING_TB = 120;

export type ScoreExplodeMode = "rules_chain" | "fields_parallel";

function readScoreNodeConfig(node: Node): JsonObject | null {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const cfg = data.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) return null;
  return cfg as JsonObject;
}

export function scoreExplodeMode(node: Node): ScoreExplodeMode | null {
  if (rfTypeToKind(node.type) !== "score") return null;
  const cfg = readScoreNodeConfig(node);
  if (!cfg) return null;
  const rules = readScoringRules(cfg);
  const fields = readScoreFields(cfg);
  if (rules.length >= 2) return "rules_chain";
  if (fields.length >= 2) return "fields_parallel";
  return null;
}

export function isExplodableScoreFlowNode(node: Node): boolean {
  return scoreExplodeMode(node) != null;
}

function copyScoreSharedConfig(target: JsonObject, parent: JsonObject): void {
  if (parent.initial_score != null) target.initial_score = parent.initial_score;
  if (parent.min_score != null) target.min_score = parent.min_score;
  if (parent.min_threshold_filter_enabled === true) {
    target.min_threshold_filter_enabled = true;
    if (parent.min_threshold != null) target.min_threshold = parent.min_threshold;
  }
}

function scoreConfigForRule(rule: unknown, parent: JsonObject, fields: string[]): JsonObject {
  const r = rule !== null && typeof rule === "object" && !Array.isArray(rule) ? (rule as JsonObject) : {};
  const name = String(r.name ?? "").trim();
  const out: JsonObject = {
    description: name || String(parent.description ?? "").trim(),
    score_fields: fields,
    scoring_rules: [rule],
  };
  copyScoreSharedConfig(out, parent);
  return out;
}

function scoreConfigForField(field: string, parent: JsonObject): JsonObject {
  const out: JsonObject = {
    description: `${String(parent.description ?? "").trim() || "score"} — ${field}`.trim(),
    score_fields: [field],
    scoring_rules: readScoringRules(parent),
  };
  copyScoreSharedConfig(out, parent);
  return out;
}

function layoutPositions(
  source: Node,
  count: number,
  orientation: TransformCanvasHandleOrientation
): { x: number; y: number }[] {
  const out: { x: number; y: number }[] = [];
  for (let i = 0; i < count; i++) {
    if (orientation === "tb") {
      out.push({ x: source.position.x, y: source.position.y + i * NODE_SPACING_TB });
    } else {
      out.push({ x: source.position.x + i * NODE_SPACING_LR, y: source.position.y });
    }
  }
  return out;
}

function nodeLabel(base: string, suffix: string): string {
  const b = base.trim();
  return b ? `${b} — ${suffix}` : suffix;
}

export type ExplodeScoreFlowNodeResult = {
  nodes: Node[];
  edges: Edge[];
  newNodeIds: string[];
  mode: ScoreExplodeMode;
};

export function explodeScoreFlowNode(
  nodes: Node[],
  edges: Edge[],
  sourceNodeId: string,
  opts?: { handleOrientation?: TransformCanvasHandleOrientation }
): ExplodeScoreFlowNodeResult | null {
  const source = nodes.find((n) => n.id === sourceNodeId);
  if (!source) return null;
  const mode = scoreExplodeMode(source);
  if (!mode) return null;

  const parentCfg = readScoreNodeConfig(source)!;
  const parentLabel = String((source.data as Record<string, unknown>).label ?? "").trim();
  const existingIds = new Set(nodes.map((n) => n.id));

  const specs: { label: string; config: JsonObject }[] = [];
  if (mode === "rules_chain") {
    const fields = readScoreFields(parentCfg);
    for (const rule of readScoringRules(parentCfg)) {
      const r = rule as JsonObject;
      const name = String(r.name ?? "").trim();
      specs.push({
        label: nodeLabel(parentLabel, name || "rule"),
        config: scoreConfigForRule(rule, parentCfg, fields),
      });
    }
  } else {
    for (const field of readScoreFields(parentCfg)) {
      specs.push({
        label: nodeLabel(parentLabel, field),
        config: scoreConfigForField(field, parentCfg),
      });
    }
  }

  if (specs.length < 2) return null;

  const positions = layoutPositions(source, specs.length, opts?.handleOrientation ?? "lr");
  const newNodes: Node[] = [];
  const newNodeIds: string[] = [];
  const sourceData = source.data as Record<string, unknown>;

  for (let i = 0; i < specs.length; i++) {
    const id = nextEtlNodeId("score", existingIds);
    existingIds.add(id);
    newNodeIds.push(id);
    const nodeData: Record<string, unknown> = {
      kind: "score",
      label: specs[i]!.label,
      config: specs[i]!.config,
    };
    if (sourceData.notes != null) nodeData.notes = sourceData.notes;
    if (sourceData.node_color != null) nodeData.node_color = sourceData.node_color;
    if (sourceData.node_bg_color != null) nodeData.node_bg_color = sourceData.node_bg_color;
    if (sourceData.canvas_node_enabled === false) nodeData.canvas_node_enabled = false;

    newNodes.push(
      withEtlNodeDimensions(
        {
          id,
          type: "etlScore",
          position: positions[i]!,
          data: nodeData,
          parentId: source.parentId,
        },
        "score"
      )
    );
  }

  const withoutSource = nodes.filter((n) => n.id !== sourceNodeId);
  const allNodes = [...withoutSource, ...newNodes];
  const getNode = (id: string) => allNodes.find((n) => n.id === id);
  const wiring = mode === "rules_chain" ? "ordered" : "parallel";
  const nextEdges = wireEdgesForExplodedNodes(edges, sourceNodeId, newNodeIds, wiring, getNode);

  return { nodes: allNodes, edges: nextEdges, newNodeIds, mode };
}
