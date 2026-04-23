import type { Connection, Edge, Node } from "@xyflow/react";
import type { SubflowPortEntry, WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import {
  isSubflowGraphHubRfType,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
} from "../../types/workflowCanvas";
import { keaValidationRuleLayoutRfTypes } from "./flowConstants";

/** Pipeline writeback sinks (RAW or Data Modeling layout cards, plus fn_dm_alias_persistence). */
function isPipelineWritebackRfType(t: string | undefined): boolean {
  return (
    t === "keaAliasPersistence" ||
    t === "keaWritebackRaw" ||
    t === "keaWritebackDataModeling"
  );
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
 * an inner peer type, and kept aligned with ``isValidKeaFlowConnection`` for non-subgraph pairs.
 */
export function isValidDirectRfDataEdgeSourceToTarget(
  sourceRfType: string,
  targetRfType: string,
  validationRuleLayoutRfTypes: Set<string>
): boolean {
  const st = sourceRfType;
  const tt = targetRfType;

  if (st === "keaEnd") return false;

  if (tt === "keaEnd") {
    return (
      st === "keaExtraction" ||
      st === "keaAliasing" ||
      st === "keaValidation" ||
      validationRuleLayoutRfTypes.has(st) ||
      isPipelineWritebackRfType(st) ||
      st === "keaReferenceIndex"
    );
  }

  if (tt === "keaReferenceIndex") return st === "keaExtraction";

  if (isPipelineWritebackRfType(tt)) {
    return st === "keaAliasing" || st === "keaValidation" || st === "keaExtraction";
  }

  if (st === "keaReferenceIndex") return tt === "keaEnd";

  if (isPipelineWritebackRfType(st)) return tt === "keaEnd";

  if (st === "keaStart") {
    return tt === "keaSourceView" || tt === "keaExtraction";
  }

  if (tt === "keaSourceView") return st === "keaStart";

  if (st === "keaSourceView") {
    if (tt === "keaAliasing") return false;
    return tt === "keaExtraction";
  }

  if (validationRuleLayoutRfTypes.has(tt)) {
    if (st === "keaExtraction" || st === "keaAliasing") {
      return true;
    }
    return validationRuleLayoutRfTypes.has(st);
  }

  if (validationRuleLayoutRfTypes.has(st)) {
    return tt === "keaEnd" || validationRuleLayoutRfTypes.has(tt);
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

function parentSubflowOf(getNode: GetNode, childId: string): Node | undefined {
  const ch = getNode(childId);
  const pid = ch?.parentId != null && String(ch.parentId).trim() ? String(ch.parentId).trim() : "";
  if (!pid) return undefined;
  const p = getNode(pid);
  return p?.type === "keaSubflow" ? p : undefined;
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

/** Whether ``st → tt`` is allowed for wiring into a subgraph input port (parent → subflow frame). */
function allowedExternalSourceToSubflowInput(st: string): boolean {
  if (st === "keaEnd") return false;
  if (st === "keaSubflow" || st === "keaSubgraph" || isSubflowGraphHubRfType(st)) return false;
  if (st === "keaStart") return true;
  if (st === "keaSourceView") return true;
  if (st === "keaExtraction" || st === "keaAliasing" || st === "keaValidation") return true;
  if (isPipelineWritebackRfType(st) || st === "keaReferenceIndex") return true;
  if (keaValidationRuleLayoutRfTypes.has(st)) return true;
  return false;
}

/** Whether ``st → tt`` is allowed for wiring from a subgraph output port (subflow frame → external). */
function allowedSubflowOutputToExternalTarget(tt: string, validationRuleLayoutRfTypes: Set<string>): boolean {
  if (tt === "keaStart") return false;
  if (tt === "keaSubflow" || tt === "keaSubgraph" || isSubflowGraphHubRfType(tt)) return false;
  if (tt === "keaEnd") return true;
  if (tt === "keaSourceView") return false;
  if (tt === "keaReferenceIndex" || isPipelineWritebackRfType(tt)) return true;
  if (tt === "keaExtraction" || tt === "keaAliasing" || tt === "keaValidation") return true;
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
  if (!st || !tt || tt !== "keaSubflowGraphOut") return false;
  const portId = parsePortIdFromSubflowTargetHandle(c.targetHandle ?? undefined);
  if (portId == null) return false;
  const frame = portFrameForGraphHub(getNode, c.target);
  if (!subflowDeclaresOutputPort(frame, portId)) return false;
  const outEntry = subflowOutputPortEntry(frame, portId);
  if (outEntry?.inner_source_rf_type && st !== outEntry.inner_source_rf_type) return false;
  const parentSf = parentSubflowOf(getNode, c.target);
  if (parentSf) {
    if (!sameSubflowInterior(getNode, c.source, c.target)) return false;
  }
  if (st === "keaEnd" || st === "keaStart" || st === "keaSubflow" || st === "keaSubgraph" || isSubflowGraphHubRfType(st))
    return false;
  if (st === "keaSourceView") return false;
  if (st === "keaExtraction" || st === "keaAliasing" || st === "keaValidation") return true;
  if (isPipelineWritebackRfType(st) || st === "keaReferenceIndex") return true;
  if (validationRuleLayoutRfTypes.has(st)) return true;
  return false;
}

function validGraphInToInterior(getNode: GetNode, c: Connection | Edge, validationRuleLayoutRfTypes: Set<string>): boolean {
  const st = getNode(c.source)?.type;
  const tt = getNode(c.target)?.type;
  if (!st || !tt || st !== "keaSubflowGraphIn") return false;
  const portId = parsePortIdFromSubflowSourceHandle(c.sourceHandle ?? undefined);
  if (portId == null) return false;
  const frame = portFrameForGraphHub(getNode, c.source);
  if (!subflowDeclaresInputPort(frame, portId)) return false;
  const inEntry = subflowInputPortEntry(frame, portId);
  if (inEntry?.inner_target_rf_type && tt !== inEntry.inner_target_rf_type) return false;
  const parentSf = parentSubflowOf(getNode, c.source);
  if (parentSf) {
    if (!sameSubflowInterior(getNode, c.source, c.target)) return false;
  }
  if (tt === "keaEnd" || tt === "keaStart" || tt === "keaSubflow" || tt === "keaSubgraph" || isSubflowGraphHubRfType(tt))
    return false;
  if (tt === "keaSourceView") return false;
  if (isPipelineWritebackRfType(tt) || tt === "keaReferenceIndex") return false;
  if (tt === "keaExtraction" || tt === "keaAliasing" || tt === "keaValidation") return true;
  if (validationRuleLayoutRfTypes.has(tt)) return true;
  return false;
}

/**
 * Full connection validity including ``keaSubgraph`` boundary ports and inner graph-in/out hubs.
 * ``keaSubflow`` is organizational only — it is not a valid connection endpoint.
 */
export function isValidKeaFlowConnection(
  getNode: GetNode,
  c: Connection | Edge,
  validationRuleLayoutRfTypes: Set<string> = keaValidationRuleLayoutRfTypes
): boolean {
  const st = getNode(c.source)?.type;
  const tt = getNode(c.target)?.type;
  if (!st || !tt) return false;
  const srcH = c.sourceHandle ?? undefined;

  if (st === "keaSubflow" || tt === "keaSubflow") return false;

  if (st === "keaSubflowGraphIn") {
    return validGraphInToInterior(getNode, c, validationRuleLayoutRfTypes);
  }
  if (tt === "keaSubflowGraphOut") {
    return validInteriorToGraphOut(getNode, c, validationRuleLayoutRfTypes);
  }
  if (isSubflowGraphHubRfType(st) || isSubflowGraphHubRfType(tt)) {
    return false;
  }

  if (tt === "keaSubgraph") {
    const portId = parsePortIdFromSubflowTargetHandle(c.targetHandle ?? undefined);
    if (portId == null) return false;
    const sg = getNode(c.target);
    if (!subflowDeclaresInputPort(sg, portId)) return false;
    const innerT = subflowInputPortEntry(sg, portId)?.inner_target_rf_type;
    if (innerT) {
      return (
        allowedExternalSourceToSubflowInput(st) &&
        isValidDirectRfDataEdgeSourceToTarget(st, innerT, validationRuleLayoutRfTypes)
      );
    }
    return allowedExternalSourceToSubflowInput(st);
  }

  if (st === "keaSubgraph") {
    const portId = parsePortIdFromSubflowSourceHandle(c.sourceHandle ?? undefined);
    if (portId == null) return false;
    const sg = getNode(c.source);
    if (!subflowDeclaresOutputPort(sg, portId)) return false;
    const innerS = subflowOutputPortEntry(sg, portId)?.inner_source_rf_type;
    if (innerS) {
      return (
        allowedSubflowOutputToExternalTarget(tt, validationRuleLayoutRfTypes) &&
        isValidDirectRfDataEdgeSourceToTarget(innerS, tt, validationRuleLayoutRfTypes)
      );
    }
    return allowedSubflowOutputToExternalTarget(tt, validationRuleLayoutRfTypes);
  }

  if (st === "keaEnd") return false;

  /** Dedicated validation branch (see ``KeaExtractionNode`` / ``KeaAliasingNode`` handle ``validation``). */
  if ((st === "keaExtraction" || st === "keaAliasing") && srcH === "validation") {
    return validationRuleLayoutRfTypes.has(tt);
  }

  if (validationRuleLayoutRfTypes.has(tt)) {
    if (st === "keaExtraction") {
      return srcH === "validation";
    }
    if (st === "keaAliasing") {
      return srcH === "validation";
    }
    /** Source views feed match-rule layout nodes on the main ``out`` handle (no ``validation`` branch). */
    if (st === "keaSourceView") {
      return srcH == null || srcH === "out";
    }
    return validationRuleLayoutRfTypes.has(st);
  }

  if (validationRuleLayoutRfTypes.has(st)) {
    return tt === "keaEnd" || validationRuleLayoutRfTypes.has(tt);
  }

  return isValidDirectRfDataEdgeSourceToTarget(st, tt, validationRuleLayoutRfTypes);
}
