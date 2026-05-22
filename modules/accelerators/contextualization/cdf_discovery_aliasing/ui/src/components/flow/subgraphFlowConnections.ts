import type { Connection, Edge, Node } from "@xyflow/react";
import type { SubflowPortEntry, WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import {
  isSubflowGraphHubRfType,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
} from "../../types/workflowCanvas";
import type { CompileWorkflowDagMode } from "../../utils/workflowCompileMode";
import {
  discoveryQueryRfTypes,
  discoveryStageRfTypes,
  discoveryPersistenceOutboundToEndOnlyRfTypes,
  discoveryValidationRuleLayoutRfTypes,
} from "./flowConstants";

/** Cohort-producing discovery nodes that may feed a subgraph frame input or graph-out from the inner canvas. */
function isDiscoveryCohortSourceRfType(t: string | undefined): boolean {
  if (!t) return false;
  return (
    discoveryQueryRfTypes.has(t) ||
    t === "discoveryTransform" ||
    t === "discoveryJoin" ||
    t === "discoveryValidate" ||
    t === "discoveryInstanceFilter" ||
    t === "discoveryConfidenceFilter" ||
    t === "discoveryInvertedIndex"
  );
}

/** Layout node before workflow end (typically save / cleanup). */
function isAliasPersistenceLayoutRfType(t: string | undefined): boolean {
  return t === "discoveryAliasPersistence";
}

type GetNode = (id: string) => Node | undefined;

function readPorts(n: Node | undefined) {
  return ((n?.data ?? {}) as WorkflowCanvasNodeData).subflow_ports;
}

function subflowInputPortEntry(sf: Node | undefined, portId: string): SubflowPortEntry | undefined {
  return readPorts(sf)?.inputs?.find((p) => p.id === portId);
}

function subflowOutputPortEntry(sf: Node | undefined, portId: string): SubflowPortEntry | undefined {
  return readPorts(sf)?.outputs?.find((p) => p.id === portId);
}

/**
 * True when a primary data edge from ``sourceRfType`` to ``targetRfType`` is allowed (target uses
 * ``in``, source uses main ``out`` / match-rule heads). Used for subgraph frame ports that record
 * an inner peer type, and kept aligned with ``isValidDiscoveryFlowConnection`` for non-subgraph pairs.
 */
export function isValidDirectRfDataEdgeSourceToTarget(
  sourceRfType: string,
  targetRfType: string,
  validationRuleLayoutRfTypes: Set<string>,
  compileDagMode: CompileWorkflowDagMode = "canvas"
): boolean {
  const st = sourceRfType;
  const tt = targetRfType;

  if (st === "discoveryEnd") return false;

  /**
   * Persistence nodes (saves, alias persistence, inverted index): primary data ``out`` may target **only** ``discoveryEnd``.
   * Disallows wiring into queries, transforms, validation, join, other saves, extraction, match-rule layouts, etc.
   */
  if (discoveryPersistenceOutboundToEndOnlyRfTypes.has(st)) {
    return tt === "discoveryEnd";
  }

  /** Query nodes may only receive the primary data edge from Start (not source views, transforms, etc.). */
  if (discoveryQueryRfTypes.has(tt)) {
    return st === "discoveryStart";
  }

  if (tt === "discoveryJoin") {
    return (
      st === "discoveryViewQuery" ||
      st === "discoveryRawQuery" ||
      st === "discoveryClassicQuery" ||
      st === "discoverySqlQuery" ||
      st === "discoveryTransform" ||
      st === "discoveryValidate" ||
      st === "discoveryInstanceFilter" ||
      st === "discoveryConfidenceFilter" ||
      st === "discoveryJoin"
    );
  }

  if (tt === "discoveryEnd") {
    return (
      st === "discoveryValidate" ||
      st === "discoveryInstanceFilter" ||
      st === "discoveryConfidenceFilter" ||
      discoveryStageRfTypes.has(st) ||
      validationRuleLayoutRfTypes.has(st) ||
      isAliasPersistenceLayoutRfType(st) ||
      st === "discoveryInvertedIndex"
    );
  }

  if (tt === "discoveryInvertedIndex") {
    return (
      st === "discoveryViewQuery" ||
      st === "discoveryRawQuery" ||
      st === "discoveryClassicQuery" ||
      st === "discoverySqlQuery" ||
      st === "discoveryTransform" ||
      st === "discoveryJoin" ||
      st === "discoveryValidate" ||
      st === "discoveryInstanceFilter" ||
      st === "discoveryConfidenceFilter"
    );
  }

  if (isAliasPersistenceLayoutRfType(tt)) {
    return (
      st === "discoveryValidate" ||
      st === "discoveryInstanceFilter" ||
      st === "discoveryConfidenceFilter" ||
      st === "discoveryTransform"
    );
  }

  if (st === "discoveryStart") {
    if (compileDagMode === "canvas") {
      return tt === "discoveryViewQuery" || tt === "discoveryRawQuery" || tt === "discoveryClassicQuery" || tt === "discoverySqlQuery";
    }
    return (
      tt === "discoverySourceView" ||
      tt === "discoveryViewQuery" ||
      tt === "discoveryRawQuery" ||
      tt === "discoveryClassicQuery" ||
      tt === "discoverySqlQuery"
    );
  }

  if (tt === "discoverySourceView") {
    if (compileDagMode === "canvas") {
      return false;
    }
    return st === "discoveryStart";
  }

  if (st === "discoverySourceView") {
    return false;
  }

  if (validationRuleLayoutRfTypes.has(tt)) {
    if (
      st === "discoveryValidate" ||
      st === "discoveryInstanceFilter" ||
      st === "discoveryConfidenceFilter"
    ) {
      return true;
    }
    if (
      st === "discoveryTransform" ||
      discoveryStageRfTypes.has(st) ||
      isAliasPersistenceLayoutRfType(st) ||
      st === "discoveryInvertedIndex"
    ) {
      return tt === "discoveryMatchValidationRuleExtraction" || tt === "discoveryMatchValidationRuleAliasing";
    }
    return validationRuleLayoutRfTypes.has(st);
  }

  if (validationRuleLayoutRfTypes.has(st)) {
    return tt === "discoveryEnd" || validationRuleLayoutRfTypes.has(tt);
  }

  /** Discovery validate output is not a cohort input for transform (query / join / transform chain only). */
  if (
    tt === "discoveryTransform" &&
    (st === "discoveryValidate" ||
      st === "discoveryInstanceFilter" ||
      st === "discoveryConfidenceFilter")
  ) {
    return false;
  }

  return true;
}

export function subflowDeclaresInputPort(sf: Node | undefined, portId: string | null): boolean {
  if (portId == null || !portId) return false;
  return Boolean(readPorts(sf)?.inputs?.some((p) => p.id === portId));
}

export function subflowDeclaresOutputPort(sf: Node | undefined, portId: string | null): boolean {
  if (portId == null || !portId) return false;
  return Boolean(readPorts(sf)?.outputs?.some((p) => p.id === portId));
}

function parentSubflowOf(_getNode: GetNode, _childId: string): Node | undefined {
  return undefined;
}

/** Node whose ``data.subflow_ports`` should be used to validate graph-in / graph-out handles. */
function portFrameForGraphHub(getNode: GetNode, hubId: string): Node | undefined {
  const hub = getNode(hubId);
  if (!hub) return undefined;
  const d = (hub.data ?? {}) as WorkflowCanvasNodeData;
  if (d.subflow_ports?.inputs?.length || d.subflow_ports?.outputs?.length) {
    return hub;
  }
  return parentSubflowOf(getNode, hubId);
}

function sameSubflowInterior(getNode: GetNode, a: string, b: string): boolean {
  const na = getNode(a);
  const nb = getNode(b);
  const pa = na?.parentId != null && String(na.parentId).trim() ? String(na.parentId).trim() : "";
  const pb = nb?.parentId != null && String(nb.parentId).trim() ? String(nb.parentId).trim() : "";
  return Boolean(pa && pa === pb);
}

/** Whether ``st → tt`` is allowed for wiring into a subgraph input port (parent → subgraph frame). */
function allowedExternalSourceToSubflowInput(st: string): boolean {
  if (st === "discoveryEnd") return false;
  if (st === "discoverySubgraph" || isSubflowGraphHubRfType(st)) return false;
  if (st === "discoveryStart") return true;
  if (st === "discoverySourceView") return true;
  if (st === "discoveryTransform") return true;
  if (isAliasPersistenceLayoutRfType(st) || st === "discoveryInvertedIndex") return true;
  if (isDiscoveryCohortSourceRfType(st)) return true;
  if (discoveryValidationRuleLayoutRfTypes.has(st)) return true;
  return false;
}

/** Whether ``st → tt`` is allowed for wiring from a subgraph output port (subgraph frame → external). */
function allowedSubflowOutputToExternalTarget(tt: string, validationRuleLayoutRfTypes: Set<string>): boolean {
  if (tt === "discoveryStart") return false;
  if (tt === "discoverySubgraph" || isSubflowGraphHubRfType(tt)) return false;
  if (tt === "discoveryEnd") return true;
  if (tt === "discoverySourceView") return false;
  if (tt === "discoveryInvertedIndex" || isAliasPersistenceLayoutRfType(tt)) return true;
  if (tt === "discoveryTransform" || discoveryStageRfTypes.has(tt) || tt === "discoveryValidate")
    return true;
  if (validationRuleLayoutRfTypes.has(tt)) return true;
  return true;
}

function validInteriorToGraphOut(
  getNode: GetNode,
  c: Connection | Edge,
  validationRuleLayoutRfTypes: Set<string>
): boolean {
  const st = getNode(c.source)?.type;
  const tt = getNode(c.target)?.type;
  if (!st || !tt || tt !== "discoverySubflowGraphOut") return false;
  const portId = parsePortIdFromSubflowTargetHandle(c.targetHandle ?? undefined);
  if (portId == null) return false;
  const frame = portFrameForGraphHub(getNode, c.target);
  if (!subflowDeclaresOutputPort(frame, portId)) return false;
  const outEntry = subflowOutputPortEntry(frame, portId);
  if (outEntry?.inner_source_rf_type && st !== outEntry.inner_source_rf_type)
    return false;
  const parentSf = parentSubflowOf(getNode, c.target);
  if (parentSf) {
    if (!sameSubflowInterior(getNode, c.source, c.target)) return false;
  }
  if (st === "discoveryEnd" || st === "discoveryStart" || st === "discoverySubgraph" || isSubflowGraphHubRfType(st)) return false;
  if (st === "discoverySourceView") return false;
  if (st === "discoveryTransform") return true;
  if (isAliasPersistenceLayoutRfType(st) || st === "discoveryInvertedIndex") return true;
  if (isDiscoveryCohortSourceRfType(st)) return true;
  if (validationRuleLayoutRfTypes.has(st)) return true;
  return false;
}

function validGraphInToInterior(getNode: GetNode, c: Connection | Edge, validationRuleLayoutRfTypes: Set<string>): boolean {
  const st = getNode(c.source)?.type;
  const tt = getNode(c.target)?.type;
  if (!st || !tt || st !== "discoverySubflowGraphIn") return false;
  const portId = parsePortIdFromSubflowSourceHandle(c.sourceHandle ?? undefined);
  if (portId == null) return false;
  const frame = portFrameForGraphHub(getNode, c.source);
  if (!subflowDeclaresInputPort(frame, portId)) return false;
  const inEntry = subflowInputPortEntry(frame, portId);
  if (inEntry?.inner_target_rf_type && tt !== inEntry.inner_target_rf_type)
    return false;
  const parentSf = parentSubflowOf(getNode, c.source);
  if (parentSf) {
    if (!sameSubflowInterior(getNode, c.source, c.target)) return false;
  }
  if (tt === "discoveryEnd" || tt === "discoveryStart" || tt === "discoverySubgraph" || isSubflowGraphHubRfType(tt)) return false;
  if (tt === "discoverySourceView") return false;
  if (isAliasPersistenceLayoutRfType(tt) || tt === "discoveryInvertedIndex") return false;
  if (tt === "discoveryTransform" || discoveryStageRfTypes.has(tt) || tt === "discoveryValidate")
    return true;
  if (validationRuleLayoutRfTypes.has(tt)) return true;
  return false;
}

/**
 * Full connection validity including ``discoverySubgraph`` boundary ports and inner graph-in/out hubs.
 */
export function isValidDiscoveryFlowConnection(
  getNode: GetNode,
  c: Connection | Edge,
  validationRuleLayoutRfTypes: Set<string> = discoveryValidationRuleLayoutRfTypes,
  compileDagMode: CompileWorkflowDagMode = "canvas"
): boolean {
  const st = getNode(c.source)?.type;
  const tt = getNode(c.target)?.type;
  if (!st || !tt) return false;
  const srcH = c.sourceHandle ?? undefined;

  if (st === "discoverySubflowGraphIn") {
    return validGraphInToInterior(getNode, c, validationRuleLayoutRfTypes);
  }
  if (tt === "discoverySubflowGraphOut") {
    return validInteriorToGraphOut(getNode, c, validationRuleLayoutRfTypes);
  }
  if (isSubflowGraphHubRfType(st) || isSubflowGraphHubRfType(tt)) {
    return false;
  }

  if (tt === "discoverySubgraph") {
    const portId = parsePortIdFromSubflowTargetHandle(c.targetHandle ?? undefined);
    if (portId == null) return false;
    const sg = getNode(c.target);
    if (!subflowDeclaresInputPort(sg, portId)) return false;
    const innerT = subflowInputPortEntry(sg, portId)?.inner_target_rf_type;
    if (innerT) {
      return (
        allowedExternalSourceToSubflowInput(st) &&
        isValidDirectRfDataEdgeSourceToTarget(st, innerT, validationRuleLayoutRfTypes, compileDagMode)
      );
    }
    return allowedExternalSourceToSubflowInput(st);
  }

  if (st === "discoverySubgraph") {
    const portId = parsePortIdFromSubflowSourceHandle(c.sourceHandle ?? undefined);
    if (portId == null) return false;
    const sg = getNode(c.source);
    if (!subflowDeclaresOutputPort(sg, portId)) return false;
    const innerS = subflowOutputPortEntry(sg, portId)?.inner_source_rf_type;
    if (innerS) {
      return (
        allowedSubflowOutputToExternalTarget(tt, validationRuleLayoutRfTypes) &&
        isValidDirectRfDataEdgeSourceToTarget(innerS, tt, validationRuleLayoutRfTypes, compileDagMode)
      );
    }
    return allowedSubflowOutputToExternalTarget(tt, validationRuleLayoutRfTypes);
  }

  if (st === "discoveryEnd") return false;

  /** Dedicated validation branch (``validation`` source handle → match-definition layout nodes). */
  if (validationRuleLayoutRfTypes.has(tt)) {
    if (st === "discoveryValidate" && tt === "discoveryMatchValidationRuleExtraction") {
      return srcH == null || srcH === "out";
    }
    /** Source views feed match-rule layout nodes on the main ``out`` handle (no ``validation`` branch). */
    if (st === "discoverySourceView") {
      return srcH == null || srcH === "out";
    }
    if (
      st === "discoveryTransform" ||
      discoveryStageRfTypes.has(st) ||
      isAliasPersistenceLayoutRfType(st) ||
      st === "discoveryInvertedIndex"
    ) {
      return srcH === "validation";
    }
    return validationRuleLayoutRfTypes.has(st);
  }

  if (validationRuleLayoutRfTypes.has(st)) {
    return tt === "discoveryEnd" || validationRuleLayoutRfTypes.has(tt);
  }

  return isValidDirectRfDataEdgeSourceToTarget(st, tt, validationRuleLayoutRfTypes, compileDagMode);
}
