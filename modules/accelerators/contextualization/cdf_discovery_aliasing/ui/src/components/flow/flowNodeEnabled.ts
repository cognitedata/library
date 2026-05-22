import type { MessageKey } from "../../i18n";
import {
  isWorkflowCanvasNodeCascadeDisabled,
  isWorkflowCanvasNodeEnabled,
  type CanvasNodeKind,
  type WorkflowCanvasDocument,
  type WorkflowCanvasEdge,
  type WorkflowCanvasNode,
} from "../../types/workflowCanvas";

const STRUCTURAL_KINDS = new Set<CanvasNodeKind>([
  "start",
  "end",
  "source_view",
  "subflow_graph_in",
  "subflow_graph_out",
]);

const PASS_THROUGH_KINDS = new Set<CanvasNodeKind>(["match_validation_source_view"]);

const EXECUTABLE_PIPELINE_KINDS = new Set<CanvasNodeKind>([
  "save_view",
  "save_raw",
  "save_classic",
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "transform",
  "join",
  "validation",
  "instance_filter",
  "confidence_filter",
  "inverted_index",
]);

const QUERY_KINDS = new Set<CanvasNodeKind>(["query_view", "query_raw", "query_classic", "query_sql"]);

const DISABLEABLE_KINDS = new Set<CanvasNodeKind>([...EXECUTABLE_PIPELINE_KINDS, "subgraph"]);

const JOIN_INPUT_SOURCE_KINDS = new Set<CanvasNodeKind>([
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "transform",
  "validation",
  "instance_filter",
  "confidence_filter",
  "join",
]);

const JOIN_TARGET_HANDLE_LEFT = "in__left";
const JOIN_TARGET_HANDLE_RIGHT = "in__right";

type Adjacency = {
  byId: Map<string, WorkflowCanvasNode>;
  revAdj: Map<string, string[]>;
  fwdAdj: Map<string, string[]>;
};

function kindOf(n: WorkflowCanvasNode): CanvasNodeKind {
  return n.kind;
}

function buildCanvasAdjacency(nodes: WorkflowCanvasNode[], edges: WorkflowCanvasEdge[]): Adjacency {
  const byId = new Map<string, WorkflowCanvasNode>();
  const revAdj = new Map<string, string[]>();
  const fwdAdj = new Map<string, string[]>();
  for (const n of nodes) {
    if (n.id) {
      byId.set(n.id, n);
      revAdj.set(n.id, []);
      fwdAdj.set(n.id, []);
    }
  }
  for (const e of edges) {
    if (!e.source || !e.target || !byId.has(e.source) || !byId.has(e.target)) continue;
    revAdj.get(e.target)!.push(e.source);
    fwdAdj.get(e.source)!.push(e.target);
  }
  return { byId, revAdj, fwdAdj };
}

function isEffectivelyEnabled(
  nid: string,
  n: WorkflowCanvasNode,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): boolean {
  if (treatAsEnabled.has(nid)) return true;
  if (disabledIds.has(nid)) return false;
  return isWorkflowCanvasNodeEnabled(n);
}

function executableIdsFromCanvas(
  byId: Map<string, WorkflowCanvasNode>,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): Set<string> {
  const out = new Set<string>();
  for (const [nid, n] of byId) {
    if (!isEffectivelyEnabled(nid, n, disabledIds, treatAsEnabled)) continue;
    if (EXECUTABLE_PIPELINE_KINDS.has(kindOf(n))) out.add(nid);
  }
  return out;
}

function isDisabledPassThroughNode(n: WorkflowCanvasNode, disabledIds: Set<string>): boolean {
  if (disabledIds.has(n.id)) return true;
  const k = kindOf(n);
  if (k === "subgraph" && !isWorkflowCanvasNodeEnabled(n)) return true;
  if (EXECUTABLE_PIPELINE_KINDS.has(k) && !isWorkflowCanvasNodeEnabled(n)) return true;
  return false;
}

function collectExecutableAncestors(
  startId: string,
  byId: Map<string, WorkflowCanvasNode>,
  revAdj: Map<string, string[]>,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): Set<string> {
  const executableIds = executableIdsFromCanvas(byId, disabledIds, treatAsEnabled);
  const out = new Set<string>();
  const stack = [...(revAdj.get(startId) ?? [])];
  const visited = new Set<string>();
  while (stack.length) {
    const cur = stack.pop()!;
    if (visited.has(cur)) continue;
    visited.add(cur);
    if (executableIds.has(cur)) {
      out.add(cur);
      continue;
    }
    const n = byId.get(cur);
    if (!n) continue;
    const k = kindOf(n);
    if (PASS_THROUGH_KINDS.has(k) || STRUCTURAL_KINDS.has(k)) {
      stack.push(...(revAdj.get(cur) ?? []));
      continue;
    }
    if (k === "subgraph" && isWorkflowCanvasNodeEnabled(n)) {
      stack.push(...(revAdj.get(cur) ?? []));
      continue;
    }
    if (isDisabledPassThroughNode(n, disabledIds)) {
      stack.push(...(revAdj.get(cur) ?? []));
    }
  }
  return out;
}

function resolveEnabledExecutableCanvasId(
  canvasId: string,
  byId: Map<string, WorkflowCanvasNode>,
  revAdj: Map<string, string[]>,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): string | null {
  const executableIds = executableIdsFromCanvas(byId, disabledIds, treatAsEnabled);
  const stack = [canvasId];
  const visited = new Set<string>();
  while (stack.length) {
    const cur = stack.pop()!;
    if (visited.has(cur)) continue;
    visited.add(cur);
    if (executableIds.has(cur)) return cur;
    const n = byId.get(cur);
    if (!n) continue;
    const k = kindOf(n);
    if (PASS_THROUGH_KINDS.has(k) || STRUCTURAL_KINDS.has(k)) {
      stack.push(...(revAdj.get(cur) ?? []));
      continue;
    }
    if (k === "subgraph" && isWorkflowCanvasNodeEnabled(n)) {
      stack.push(...(revAdj.get(cur) ?? []));
      continue;
    }
    if (isDisabledPassThroughNode(n, disabledIds)) {
      stack.push(...(revAdj.get(cur) ?? []));
    }
  }
  return null;
}

function isEntryExecutableNode(
  nodeId: string,
  byId: Map<string, WorkflowCanvasNode>,
  revAdj: Map<string, string[]>,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): boolean {
  const n = byId.get(nodeId);
  if (!n || !QUERY_KINDS.has(kindOf(n))) return false;
  if (
    collectExecutableAncestors(nodeId, byId, revAdj, disabledIds, treatAsEnabled).size > 0
  ) {
    return false;
  }
  const stack = [...(revAdj.get(nodeId) ?? [])];
  const visited = new Set<string>();
  while (stack.length) {
    const cur = stack.pop()!;
    if (visited.has(cur)) continue;
    visited.add(cur);
    const curN = byId.get(cur);
    if (!curN) continue;
    const k = kindOf(curN);
    if (EXECUTABLE_PIPELINE_KINDS.has(k) && isEffectivelyEnabled(cur, curN, disabledIds, treatAsEnabled)) {
      return false;
    }
    if (
      PASS_THROUGH_KINDS.has(k) ||
      STRUCTURAL_KINDS.has(k) ||
      k === "subgraph"
    ) {
      stack.push(...(revAdj.get(cur) ?? []));
      continue;
    }
    if (isDisabledPassThroughNode(curN, disabledIds)) {
      stack.push(...(revAdj.get(cur) ?? []));
      continue;
    }
    return false;
  }
  return true;
}

function joinHasExecutableInput(
  joinId: string,
  edges: WorkflowCanvasEdge[],
  byId: Map<string, WorkflowCanvasNode>,
  revAdj: Map<string, string[]>,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): boolean {
  let leftOk = false;
  let rightOk = false;
  for (const e of edges) {
    if (e.target !== joinId) continue;
    const src = e.source;
    if (!src) continue;
    const pred = byId.get(src);
    if (!pred || !JOIN_INPUT_SOURCE_KINDS.has(kindOf(pred))) continue;
    if (
      !resolveEnabledExecutableCanvasId(src, byId, revAdj, disabledIds, treatAsEnabled)
    ) {
      continue;
    }
    const th = e.target_handle ?? "";
    if (th === JOIN_TARGET_HANDLE_LEFT) leftOk = true;
    else if (th === JOIN_TARGET_HANDLE_RIGHT) rightOk = true;
  }
  return leftOk || rightOk;
}

function hasValidExecutableUpstream(
  nodeId: string,
  edges: WorkflowCanvasEdge[],
  byId: Map<string, WorkflowCanvasNode>,
  revAdj: Map<string, string[]>,
  disabledIds: Set<string>,
  treatAsEnabled: Set<string>
): boolean {
  const n = byId.get(nodeId);
  if (!n) return true;
  const k = kindOf(n);
  if (!DISABLEABLE_KINDS.has(k)) return true;
  if (k === "join") {
    return joinHasExecutableInput(nodeId, edges, byId, revAdj, disabledIds, treatAsEnabled);
  }
  const ancestors = collectExecutableAncestors(
    nodeId,
    byId,
    revAdj,
    disabledIds,
    treatAsEnabled
  );
  if (ancestors.size > 0) return true;
  return isEntryExecutableNode(nodeId, byId, revAdj, disabledIds, treatAsEnabled);
}

function shouldCascadeDisable(
  nodeId: string,
  edges: WorkflowCanvasEdge[],
  byId: Map<string, WorkflowCanvasNode>,
  revAdj: Map<string, string[]>,
  disabledIds: Set<string>
): boolean {
  const n = byId.get(nodeId);
  if (!n || !DISABLEABLE_KINDS.has(kindOf(n))) return false;
  if (disabledIds.has(nodeId)) return false;
  if (!isWorkflowCanvasNodeEnabled(n)) return false;
  return !hasValidExecutableUpstream(nodeId, edges, byId, revAdj, disabledIds, new Set());
}

export function cascadeDisableIds(doc: WorkflowCanvasDocument, rootIds: Set<string>): Set<string> {
  const { byId, revAdj, fwdAdj } = buildCanvasAdjacency(doc.nodes, doc.edges);
  const disabled = new Set(rootIds);
  const queue = [...rootIds].filter((id) => byId.has(id));
  while (queue.length) {
    const nid = queue.shift()!;
    for (const tgt of fwdAdj.get(nid) ?? []) {
      if (disabled.has(tgt)) continue;
      if (shouldCascadeDisable(tgt, doc.edges, byId, revAdj, disabled)) {
        disabled.add(tgt);
        queue.push(tgt);
      }
    }
  }
  return disabled;
}

function effectiveDisabledIds(
  byId: Map<string, WorkflowCanvasNode>,
  treatAsEnabled: Set<string>
): Set<string> {
  const out = new Set<string>();
  for (const [nid, n] of byId) {
    if (treatAsEnabled.has(nid)) continue;
    if (!isWorkflowCanvasNodeEnabled(n)) out.add(nid);
  }
  return out;
}

export function cascadeEnableIds(
  doc: WorkflowCanvasDocument,
  rootIds: Set<string>,
  opts?: { pendingEnable?: Set<string> }
): Set<string> {
  const pending = opts?.pendingEnable ?? new Set<string>();
  const { byId, revAdj, fwdAdj } = buildCanvasAdjacency(doc.nodes, doc.edges);
  const toEnable = new Set<string>();
  const extraEnabled = new Set<string>();
  for (const nid of rootIds) {
    if (!byId.has(nid)) continue;
    const n = byId.get(nid)!;
    if (pending.has(nid) || isWorkflowCanvasNodeEnabled(n)) extraEnabled.add(nid);
  }
  const queue = [...extraEnabled];
  while (queue.length) {
    const nid = queue.shift()!;
    for (const tgt of fwdAdj.get(nid) ?? []) {
      if (toEnable.has(tgt) || rootIds.has(tgt)) continue;
      const n = byId.get(tgt);
      if (!n || isWorkflowCanvasNodeEnabled(n)) continue;
      if (!isWorkflowCanvasNodeCascadeDisabled(n)) continue;
      const dis = effectiveDisabledIds(byId, extraEnabled);
      if (
        hasValidExecutableUpstream(tgt, doc.edges, byId, revAdj, dis, extraEnabled)
      ) {
        toEnable.add(tgt);
        extraEnabled.add(tgt);
        queue.push(tgt);
      }
    }
  }
  return toEnable;
}

export type WorkflowCanvasEnablementPatch = {
  document: WorkflowCanvasDocument;
  cascadeAffectedIds: string[];
};

function innerCanvasFromSubgraph(node: WorkflowCanvasNode): WorkflowCanvasDocument | undefined {
  return node.data?.inner_canvas;
}

/** Cascade-disable inner steps when the subgraph frame is off; reconcile when it is on. */
function syncSubgraphInnerEnablement(node: WorkflowCanvasNode): WorkflowCanvasNode {
  if (node.kind !== "subgraph") return node;
  const inner = innerCanvasFromSubgraph(node);
  if (!inner?.nodes?.length) return node;

  const nextInner = isWorkflowCanvasNodeEnabled(node)
    ? reconcileInnerCanvasEnablement(inner)
    : disableInnerCanvasForDisabledFrame(inner);

  return {
    ...node,
    data: {
      ...node.data,
      inner_canvas: nextInner,
    },
  };
}

function disableInnerCanvasForDisabledFrame(
  inner: WorkflowCanvasDocument
): WorkflowCanvasDocument {
  return syncAllSubgraphInnerEnablement({
    ...inner,
    nodes: inner.nodes.map((n) => {
      if (!DISABLEABLE_KINDS.has(kindOf(n))) return n;
      if (!isWorkflowCanvasNodeEnabled(n) && !isWorkflowCanvasNodeCascadeDisabled(n)) {
        return n;
      }
      return { ...n, enabled: false, cascade_disabled: true };
    }),
  });
}

function reconcileInnerCanvasEnablement(
  inner: WorkflowCanvasDocument
): WorkflowCanvasDocument {
  let nodes = inner.nodes.map((n) => {
    if (!isWorkflowCanvasNodeCascadeDisabled(n) || isWorkflowCanvasNodeEnabled(n)) {
      return n;
    }
    const { enabled: _e, cascade_disabled: _c, ...rest } = n;
    return rest;
  });
  const edges = inner.edges;

  let changed = true;
  while (changed) {
    changed = false;
    const { byId, revAdj } = buildCanvasAdjacency(nodes, edges);
    const disabledIds = new Set(
      [...byId.entries()]
        .filter(([, n]) => !isWorkflowCanvasNodeEnabled(n))
        .map(([id]) => id)
    );
    for (const [nid, n] of byId) {
      if (!isWorkflowCanvasNodeEnabled(n)) continue;
      if (!DISABLEABLE_KINDS.has(kindOf(n))) continue;
      if (
        shouldCascadeDisable(nid, edges, byId, revAdj, disabledIds)
      ) {
        nodes = nodes.map((x) =>
          x.id === nid ? { ...x, enabled: false, cascade_disabled: true } : x
        );
        changed = true;
      }
    }
  }
  return syncAllSubgraphInnerEnablement({ ...inner, nodes });
}

function syncAllSubgraphInnerEnablement(
  doc: WorkflowCanvasDocument
): WorkflowCanvasDocument {
  return {
    ...doc,
    nodes: doc.nodes.map((n) => (n.kind === "subgraph" ? syncSubgraphInnerEnablement(n) : n)),
  };
}

function patchNodeEnablement(
  n: WorkflowCanvasNode,
  rootId: string,
  turnOn: boolean,
  manualDisableIds: Set<string>,
  manualEnableIds: Set<string>,
  cascadeEnableOnlyIds: Set<string>
): WorkflowCanvasNode {
  const nid = n.id;
  if (turnOn && (manualEnableIds.has(nid) || cascadeEnableOnlyIds.has(nid))) {
    const { enabled: _e, cascade_disabled: _c, ...rest } = n;
    return rest;
  }
  if (manualDisableIds.has(nid)) {
    if (nid === rootId) {
      const { cascade_disabled: _c, enabled: _e, ...rest } = n;
      return { ...rest, enabled: false };
    }
    return { ...n, enabled: false, cascade_disabled: true };
  }
  return n;
}

/** Toggle or set canvas node ``enabled`` with cascade disable/enable for orphaned downstream nodes. */
export function patchWorkflowCanvasNodeEnabled(
  doc: WorkflowCanvasDocument,
  nodeId: string,
  enabled?: boolean
): WorkflowCanvasEnablementPatch {
  const root = doc.nodes.find((n) => n.id === nodeId);
  if (!root) {
    return { document: doc, cascadeAffectedIds: [] };
  }
  const turnOn = enabled ?? !isWorkflowCanvasNodeEnabled(root);

  let manualDisableIds = new Set<string>();
  const manualEnableIds = new Set<string>();
  let cascadeEnableOnlyIds = new Set<string>();
  let cascadeHintIds: string[] = [];

  if (turnOn) {
    manualEnableIds.add(nodeId);
    cascadeEnableOnlyIds = cascadeEnableIds(doc, new Set([nodeId]), {
      pendingEnable: new Set([nodeId]),
    });
    cascadeHintIds = [...cascadeEnableOnlyIds];
  } else {
    manualDisableIds = cascadeDisableIds(doc, new Set([nodeId]));
    cascadeHintIds = [...manualDisableIds].filter((id) => id !== nodeId);
  }

  const patched: WorkflowCanvasDocument = {
    ...doc,
    nodes: doc.nodes.map((n) =>
      patchNodeEnablement(
        n,
        nodeId,
        turnOn,
        manualDisableIds,
        manualEnableIds,
        cascadeEnableOnlyIds
      )
    ),
  };

  return {
    document: syncAllSubgraphInnerEnablement(patched),
    cascadeAffectedIds: cascadeHintIds,
  };
}

export function formatCascadeEnablementHint(
  t: (key: MessageKey, vars?: Record<string, string | number>) => string,
  cascadeAffectedIds: string[],
  turnOn: boolean
): string | null {
  if (cascadeAffectedIds.length === 0) return null;
  const count = cascadeAffectedIds.length;
  return turnOn
    ? t("flow.cascadeEnabledNodes", { count })
    : t("flow.cascadeDisabledNodes", { count });
}

/** Apply enablement patch and optionally report a cascade summary hint. */
export function applyWorkflowCanvasEnablementPatch(
  prev: WorkflowCanvasDocument,
  nodeId: string,
  enabled?: boolean,
  opts?: {
    t?: (key: MessageKey, vars?: Record<string, string | number>) => string;
    onHint?: (message: string) => void;
  }
): WorkflowCanvasDocument {
  const root = prev.nodes.find((n) => n.id === nodeId);
  const turnOn = enabled ?? (root ? !isWorkflowCanvasNodeEnabled(root) : true);
  const result = patchWorkflowCanvasNodeEnabled(prev, nodeId, enabled);
  if (opts?.t && opts.onHint) {
    const hint = formatCascadeEnablementHint(opts.t, result.cascadeAffectedIds, turnOn);
    if (hint) opts.onHint(hint);
  }
  return result.document;
}
