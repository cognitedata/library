import { useEffect, useState } from "react";
import type { Edge, Node } from "@xyflow/react";
import { isDescendantInParentTree } from "./flowParentGeometry";
import YAML from "yaml";
import type { MessageKey } from "../../i18n";
import type { JsonObject } from "../../types/scopeConfig";
import type {
  CanvasEdgeKind,
  SubflowPortsConfig,
  SubflowPortEntry,
  WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { rfTypeToKind } from "../../types/workflowCanvas";
import type { FlowEdgeData } from "./flowDocumentBridge";
import {
  ALIASING_HANDLER_IDS,
  ANNOTATION_KINDS,
  EXTRACTION_HANDLER_IDS,
} from "./handlerRegistry";
import {
  SourceViewFilterNodeEditor,
  emptyAnd,
  emptyLeaf,
  emptyNot,
  emptyOr,
} from "../SourceViewFiltersEditor";
import { resolveConfidenceMatchRuleNames } from "../../utils/confidenceMatchRuleNames";
import {
  getAliasingRuleScopeFilters,
  getExtractionRuleScopeFilters,
  patchAliasingRuleScopeFilters,
  patchExtractionRuleScopeFilters,
  patchSourceViewFilters,
} from "./workflowScopePatch";
import { DeferredCommitInput, DeferredCommitTextarea } from "../DeferredCommitTextField";
import { canChangeSubflowParent } from "./subflowMembership";
import { keaValidationRuleLayoutRfTypes } from "./flowConstants";
import { FlowNodeAccentColorFields } from "./flowNodeAccent";
import { getAliasingTransformRuleRows } from "./aliasingScopeData";

function validationRuleLayoutContextFromRfType(rfType: string | undefined): "source_view" | "extraction" | "aliasing" {
  switch (rfType) {
    case "keaMatchValidationRuleSourceView":
      return "source_view";
    case "keaMatchValidationRuleExtraction":
      return "extraction";
    case "keaMatchValidationRuleAliasing":
      return "aliasing";
    default:
      return "extraction";
  }
}

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  workflowDoc: Record<string, unknown>;
  /** All flow nodes (for subflow parent picker). */
  flowNodes?: Node[];
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
  onPatchEdge: (edgeId: string, kind: CanvasEdgeKind) => void;
  /** Set or clear ``parentId`` (empty string = root canvas). */
  onSetNodeParent?: (nodeId: string, parentSubflowId: string) => void;
  /** Persist subgraph port list and prune edges that referenced removed ports. */
  onApplySubflowPorts?: (subflowId: string, ports: SubflowPortsConfig) => void;
  /** Open drill-in editor for a ``keaSubgraph`` composite. */
  onOpenSubgraphDrill?: (nodeId: string) => void;
  /** When editing a subgraph inner canvas: frame ports and hub ids for in-canvas port management. */
  drillBoundaryPorts?: {
    targetSubgraphId: string;
    ports: SubflowPortsConfig;
    hubInId: string;
    hubOutId: string;
  };
};

function eligibleParentSubflows(flowNodes: Node[], forNodeId: string): { id: string; label: string }[] {
  const out: { id: string; label: string }[] = [];
  for (const n of flowNodes) {
    if (n.type !== "keaSubflow") continue;
    if (n.id === forNodeId) continue;
    if (isDescendantInParentTree(flowNodes, forNodeId, n.id)) continue;
    const lab = String((n.data as Record<string, unknown> | undefined)?.label ?? n.id);
    out.push({ id: n.id, label: lab });
  }
  out.sort((a, b) => a.label.localeCompare(b.label));
  return out;
}

function resolveSourceViewIndex(ref: Record<string, unknown>): number | null {
  const idxRaw = ref.source_view_index;
  if (idxRaw === undefined || idxRaw === null) return null;
  const i = typeof idxRaw === "number" ? idxRaw : parseInt(String(idxRaw), 10);
  if (!Number.isFinite(i) || i < 0) return null;
  return i;
}

function listExtractionRuleNames(doc: Record<string, unknown>): string[] {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  const data = ke?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  const rules = d?.extraction_rules;
  if (!Array.isArray(rules)) return [];
  const out: string[] = [];
  for (const r of rules) {
    if (r && typeof r === "object" && !Array.isArray(r)) {
      const name = (r as Record<string, unknown>).name ?? (r as Record<string, unknown>).rule_id;
      if (name != null && String(name).trim()) out.push(String(name));
    }
  }
  return out;
}

function listSvConfidenceNames(doc: Record<string, unknown>, svIdx: number): string[] {
  const svs = doc.source_views;
  if (!Array.isArray(svs) || svIdx < 0 || svIdx >= svs.length) return [];
  const row = svs[svIdx];
  if (!row || typeof row !== "object" || Array.isArray(row)) return [];
  return resolveConfidenceMatchRuleNames((row as Record<string, unknown>).validation, doc);
}

function listKeConfidenceNames(doc: Record<string, unknown>, extractionRuleName: string): string[] {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.extraction_rules;
  if (!Array.isArray(rules)) return [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() !== extractionRuleName) continue;
    return resolveConfidenceMatchRuleNames(row.validation, doc);
  }
  return [];
}

function listKeyExtractionDataValidationConfidenceNames(doc: Record<string, unknown>): string[] {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(data?.validation, doc);
}

function listAlConfidenceNames(doc: Record<string, unknown>, aliasingRuleName: string): string[] {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  const config = al?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = getAliasingTransformRuleRows(data);
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() !== aliasingRuleName) continue;
    return resolveConfidenceMatchRuleNames(row.validation, doc);
  }
  return [];
}

function listAliasingDataValidationConfidenceNames(doc: Record<string, unknown>): string[] {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  const config = al?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(data?.validation, doc);
}

function listAliasingRuleNames(doc: Record<string, unknown>): string[] {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  const cfg = al?.config as Record<string, unknown> | undefined;
  const d = cfg?.data as Record<string, unknown> | undefined;
  const rules = getAliasingTransformRuleRows(d);
  const out: string[] = [];
  for (const r of rules) {
    if (r && typeof r === "object" && !Array.isArray(r)) {
      const name = (r as Record<string, unknown>).name;
      if (name != null && String(name).trim()) out.push(String(name));
    }
  }
  return out;
}

function readNodeRef(data: Record<string, unknown>): Record<string, unknown> {
  const r = data.ref;
  if (r && typeof r === "object" && !Array.isArray(r)) return { ...r };
  return {};
}

function sourceViewIndexDrift(
  data: Record<string, unknown>,
  doc: Record<string, unknown>
): { ok: boolean; hint: string } {
  const ref = readNodeRef(data);
  const idxRaw = ref.source_view_index;
  if (idxRaw === undefined || idxRaw === null) return { ok: true, hint: "" };
  const i = typeof idxRaw === "number" ? idxRaw : parseInt(String(idxRaw), 10);
  const svs = doc.source_views;
  if (!Array.isArray(svs)) return { ok: true, hint: "" };
  if (!Number.isFinite(i) || i < 0 || i >= svs.length) {
    return { ok: false, hint: "flow.inspectorSourceViewIndexInvalid" };
  }
  return { ok: true, hint: "" };
}

function listSourceViewScopeEntries(doc: Record<string, unknown>): { index: number; label: string }[] {
  const svs = doc.source_views;
  if (!Array.isArray(svs)) return [];
  const out: { index: number; label: string }[] = [];
  for (let i = 0; i < svs.length; i++) {
    const v = svs[i];
    if (!v || typeof v !== "object" || Array.isArray(v)) continue;
    const row = v as Record<string, unknown>;
    const ext = row.view_external_id != null ? String(row.view_external_id) : "";
    out.push({ index: i, label: ext || `source_views[${i}]` });
  }
  return out;
}

function refResolved(
  rfType: string | undefined,
  data: Record<string, unknown>,
  extractionNames: string[],
  aliasingNames: string[],
  workflowDoc: Record<string, unknown>
): { ok: boolean; hint: string } {
  const logical = rfTypeToKind(rfType);
  if (logical === "subflow") return { ok: true, hint: "" };
  const ref = data.ref as Record<string, unknown> | undefined;
  const isMatchRuleLogical =
    logical === "match_validation_source_view" ||
    logical === "match_validation_extraction" ||
    logical === "match_validation_aliasing";
  if (isMatchRuleLogical) {
    const ctx: "source_view" | "extraction" | "aliasing" =
      logical === "match_validation_source_view"
        ? "source_view"
        : logical === "match_validation_extraction"
          ? "extraction"
          : "aliasing";
    const cm =
      data.validation_rule_name != null ? String(data.validation_rule_name).trim() : "";
    if (!cm) return { ok: true, hint: "" };
    if (ctx === "source_view") {
      const idx = ref?.source_view_index;
      if (typeof idx !== "number" || !Number.isFinite(idx)) return { ok: true, hint: "" };
      const names = listSvConfidenceNames(workflowDoc, idx);
      return {
        ok: names.includes(cm),
        hint: names.includes(cm) ? "" : "flow.inspectorRefMissingHint",
      };
    }
    if (ctx === "extraction") {
      if (ref?.extraction_global_validation === true) {
        const names = listKeyExtractionDataValidationConfidenceNames(workflowDoc);
        return {
          ok: names.includes(cm),
          hint: names.includes(cm) ? "" : "flow.inspectorRefMissingHint",
        };
      }
      const rn = ref?.extraction_rule_name != null ? String(ref.extraction_rule_name).trim() : "";
      if (!rn) return { ok: true, hint: "" };
      const names = listKeConfidenceNames(workflowDoc, rn);
      return {
        ok: names.includes(cm),
        hint: names.includes(cm) ? "" : "flow.inspectorRefMissingHint",
      };
    }
    if (ctx === "aliasing") {
      if (ref?.aliasing_global_validation === true) {
        const names = listAliasingDataValidationConfidenceNames(workflowDoc);
        return {
          ok: names.includes(cm),
          hint: names.includes(cm) ? "" : "flow.inspectorRefMissingHint",
        };
      }
      const rn = ref?.aliasing_rule_name != null ? String(ref.aliasing_rule_name).trim() : "";
      if (!rn) return { ok: true, hint: "" };
      const names = listAlConfidenceNames(workflowDoc, rn);
      return {
        ok: names.includes(cm),
        hint: names.includes(cm) ? "" : "flow.inspectorRefMissingHint",
      };
    }
    return { ok: true, hint: "" };
  }
  if (!ref) return { ok: true, hint: "" };
  if (logical === "extraction") {
    const n = ref.extraction_rule_name != null ? String(ref.extraction_rule_name) : "";
    if (!n.trim()) return { ok: true, hint: "" };
    return {
      ok: extractionNames.includes(n),
      hint: extractionNames.includes(n) ? "" : "flow.inspectorRefMissingHint",
    };
  }
  if (logical === "aliasing") {
    const n = ref.aliasing_rule_name != null ? String(ref.aliasing_rule_name) : "";
    if (!n.trim()) return { ok: true, hint: "" };
    return {
      ok: aliasingNames.includes(n),
      hint: aliasingNames.includes(n) ? "" : "flow.inspectorRefMissingHint",
    };
  }
  return { ok: true, hint: "" };
}

function subflowNewPortId(): string {
  return `p_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function swapPortRow<T>(rows: T[], i: number, j: number): T[] {
  if (i < 0 || j < 0 || i >= rows.length || j >= rows.length || i === j) return rows;
  const next = [...rows];
  const a = next[i];
  const b = next[j];
  next[i] = b!;
  next[j] = a!;
  return next;
}

function SubgraphPortsEditorBlock(props: {
  t: TFn;
  syncKeyPrefix: string;
  ports: SubflowPortsConfig;
  onCommit: (next: SubflowPortsConfig) => void;
}) {
  const { t, syncKeyPrefix, ports, onCommit } = props;
  return (
    <div style={{ marginTop: "1rem" }}>
      <p className="kea-flow-inspector__subtitle" style={{ margin: "0 0 0.5rem", fontWeight: 600 }}>
        {t("flow.inspectorSubflowPorts")}
      </p>
      <div style={{ marginBottom: "0.85rem" }}>
        <div style={{ fontWeight: 600, marginBottom: 6 }}>{t("flow.inspectorSubflowInputs")}</div>
        {ports.inputs.map((row: SubflowPortEntry, idx: number) => (
          <div key={`${row.id}-${idx}`} style={{ marginBottom: 6 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem", alignItems: "center" }}>
            <code className="kea-hint" style={{ minWidth: "4.5rem" }}>
              {row.id}
            </code>
            <DeferredCommitInput
              className="kea-input"
              style={{ flex: "1 1 8rem", minWidth: "6rem" }}
              committedValue={String(row.label ?? "")}
              syncKey={`${syncKeyPrefix}-in-lbl-${row.id}`}
              onCommit={(v) => {
                const nextIn = ports.inputs.map((p, i) => (i === idx ? { ...p, label: v } : p));
                onCommit({ inputs: nextIn, outputs: ports.outputs });
              }}
            />
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              disabled={idx === 0}
              title={t("flow.inspectorSubflowMovePortUp")}
              aria-label={t("flow.inspectorSubflowMovePortUp")}
              onClick={() => onCommit({ inputs: swapPortRow(ports.inputs, idx, idx - 1), outputs: ports.outputs })}
            >
              ↑
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              disabled={idx >= ports.inputs.length - 1}
              title={t("flow.inspectorSubflowMovePortDown")}
              aria-label={t("flow.inspectorSubflowMovePortDown")}
              onClick={() => onCommit({ inputs: swapPortRow(ports.inputs, idx, idx + 1), outputs: ports.outputs })}
            >
              ↓
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              disabled={ports.inputs.length <= 1}
              onClick={() => {
                const nextIn = ports.inputs.filter((_, i) => i !== idx);
                onCommit({ inputs: nextIn, outputs: ports.outputs });
              }}
            >
              {t("flow.inspectorSubflowRemovePort")}
            </button>
            </div>
            {row.inner_target_rf_type ? (
              <p className="kea-hint" style={{ margin: "0.2rem 0 0", fontSize: "0.8rem" }}>
                {t("flow.inspectorSubflowPortInnerIn", { type: row.inner_target_rf_type })}
              </p>
            ) : null}
          </div>
        ))}
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          style={{ marginTop: 4 }}
          onClick={() =>
            onCommit({
              inputs: [...ports.inputs, { id: subflowNewPortId(), label: "" }],
              outputs: ports.outputs,
            })
          }
        >
          {t("flow.inspectorSubflowAddInputPort")}
        </button>
      </div>
      <div>
        <div style={{ fontWeight: 600, marginBottom: 6 }}>{t("flow.inspectorSubflowOutputs")}</div>
        {ports.outputs.map((row: SubflowPortEntry, idx: number) => (
          <div key={`${row.id}-o-${idx}`} style={{ marginBottom: 6 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem", alignItems: "center" }}>
            <code className="kea-hint" style={{ minWidth: "4.5rem" }}>
              {row.id}
            </code>
            <DeferredCommitInput
              className="kea-input"
              style={{ flex: "1 1 8rem", minWidth: "6rem" }}
              committedValue={String(row.label ?? "")}
              syncKey={`${syncKeyPrefix}-out-lbl-${row.id}`}
              onCommit={(v) => {
                const nextOut = ports.outputs.map((p, i) => (i === idx ? { ...p, label: v } : p));
                onCommit({ inputs: ports.inputs, outputs: nextOut });
              }}
            />
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              disabled={idx === 0}
              title={t("flow.inspectorSubflowMovePortUp")}
              aria-label={t("flow.inspectorSubflowMovePortUp")}
              onClick={() => onCommit({ inputs: ports.inputs, outputs: swapPortRow(ports.outputs, idx, idx - 1) })}
            >
              ↑
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              disabled={idx >= ports.outputs.length - 1}
              title={t("flow.inspectorSubflowMovePortDown")}
              aria-label={t("flow.inspectorSubflowMovePortDown")}
              onClick={() => onCommit({ inputs: ports.inputs, outputs: swapPortRow(ports.outputs, idx, idx + 1) })}
            >
              ↓
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              disabled={ports.outputs.length <= 1}
              onClick={() => {
                const nextOut = ports.outputs.filter((_, i) => i !== idx);
                onCommit({ inputs: ports.inputs, outputs: nextOut });
              }}
            >
              {t("flow.inspectorSubflowRemovePort")}
            </button>
            </div>
            {row.inner_source_rf_type ? (
              <p className="kea-hint" style={{ margin: "0.2rem 0 0", fontSize: "0.8rem" }}>
                {t("flow.inspectorSubflowPortInnerOut", { type: row.inner_source_rf_type })}
              </p>
            ) : null}
          </div>
        ))}
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          style={{ marginTop: 4 }}
          onClick={() =>
            onCommit({
              inputs: ports.inputs,
              outputs: [...ports.outputs, { id: subflowNewPortId(), label: "" }],
            })
          }
        >
          {t("flow.inspectorSubflowAddOutputPort")}
        </button>
      </div>
    </div>
  );
}

export function FlowNodeInspector({
  t,
  selectedNode,
  selectedEdge,
  workflowDoc,
  flowNodes,
  onPatchWorkflowScope,
  onPatchNode,
  onPatchEdge,
  onSetNodeParent,
  onApplySubflowPorts,
  onOpenSubgraphDrill,
  drillBoundaryPorts,
}: Props) {
  const extractionNames = listExtractionRuleNames(workflowDoc);
  const aliasingNames = listAliasingRuleNames(workflowDoc);
  const sourceViewScopeEntries = listSourceViewScopeEntries(workflowDoc);

  const [scopeFiltersYaml, setScopeFiltersYaml] = useState("");
  const [scopeFiltersYamlInvalid, setScopeFiltersYamlInvalid] = useState(false);

  useEffect(() => {
    if (!selectedNode) {
      setScopeFiltersYaml("");
      setScopeFiltersYamlInvalid(false);
      return;
    }
    const k = selectedNode.type ?? "";
    const raw = (selectedNode.data ?? {}) as Record<string, unknown>;
    const r = readNodeRef(raw);
    if (k === "keaExtraction") {
      const name = String(r.extraction_rule_name ?? "").trim();
      if (!name) {
        setScopeFiltersYaml("");
        return;
      }
      const sf = getExtractionRuleScopeFilters(workflowDoc, name);
      setScopeFiltersYaml(YAML.stringify(sf ?? {}));
      setScopeFiltersYamlInvalid(false);
    } else if (k === "keaAliasing") {
      const name = String(r.aliasing_rule_name ?? "").trim();
      if (!name) {
        setScopeFiltersYaml("");
        return;
      }
      const sf = getAliasingRuleScopeFilters(workflowDoc, name);
      setScopeFiltersYaml(YAML.stringify(sf ?? {}));
      setScopeFiltersYamlInvalid(false);
    } else {
      setScopeFiltersYaml("");
      setScopeFiltersYamlInvalid(false);
    }
  }, [selectedNode, workflowDoc]);

  if (selectedEdge) {
    const fd = (selectedEdge.data ?? {}) as FlowEdgeData;
    const kind = fd.kind ?? "data";
    return (
      <aside className="kea-flow-inspector">
        <h4 className="kea-flow-inspector__title">{t("flow.inspectorEdgeTitle")}</h4>
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {selectedEdge.id}
        </p>
        <label className="kea-label kea-label--block">
          {t("flow.inspectorEdgeKind")}
          <select
            className="kea-input"
            value={kind}
            onChange={(e) => onPatchEdge(selectedEdge.id, e.target.value as CanvasEdgeKind)}
          >
            <option value="data">{t("flow.edgeKindData")}</option>
            <option value="sequence">{t("flow.edgeKindSequence")}</option>
            <option value="parallel_group">{t("flow.edgeKindParallel")}</option>
          </select>
        </label>
      </aside>
    );
  }

  if (!selectedNode) {
    if (drillBoundaryPorts && onApplySubflowPorts) {
      const { targetSubgraphId, ports } = drillBoundaryPorts;
      const commitBoundaryPorts = (next: SubflowPortsConfig) => {
        onApplySubflowPorts(targetSubgraphId, next);
      };
      return (
        <aside className="kea-flow-inspector">
          <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.inspectorDrillBoundaryPortsHint")}
          </p>
          <SubgraphPortsEditorBlock
            t={t}
            syncKeyPrefix={`${targetSubgraphId}-drill`}
            ports={ports}
            onCommit={commitBoundaryPorts}
          />
        </aside>
      );
    }
    return (
      <aside className="kea-flow-inspector">
        <p className="kea-hint">{t("flow.inspectorEmpty")}</p>
      </aside>
    );
  }

  const data = (selectedNode.data ?? {}) as Record<string, unknown>;
  const kind = selectedNode.type ?? "keaExtraction";
  const validationRuleLayoutCtx = validationRuleLayoutContextFromRfType(kind);
  const drift =
    kind === "keaSourceView"
      ? sourceViewIndexDrift(data, workflowDoc)
      : kind === "keaStart" ||
          kind === "keaEnd" ||
          kind === "keaSubflowGraphIn" ||
          kind === "keaSubflowGraphOut"
        ? { ok: true, hint: "" }
        : refResolved(kind, data, extractionNames, aliasingNames, workflowDoc);
  const ref = readNodeRef(data);

  if (kind === "keaStart" || kind === "keaEnd") {
    return (
      <aside className="kea-flow-inspector">
        <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {selectedNode.id} · {kind}
        </p>
        <label className="kea-label kea-label--block">
          {t("flow.inspectorLabel")}
          <DeferredCommitInput
            className="kea-input"
            committedValue={String(data.label ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, label: v })}
          />
        </label>
        <FlowNodeAccentColorFields t={t} nodeId={selectedNode.id} data={data} onPatchNode={onPatchNode} />
        <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
          {kind === "keaStart" ? t("flow.inspectorStartHint") : t("flow.inspectorEndHint")}
        </p>
        <label className="kea-label kea-label--block">
          {t("flow.inspectorNotes")}
          <DeferredCommitTextarea
            className="kea-textarea"
            rows={3}
            committedValue={String(data.notes ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, notes: v })}
          />
        </label>
      </aside>
    );
  }

  if (kind === "keaSubflowGraphIn" || kind === "keaSubflowGraphOut") {
    const showBoundaryPorts =
      drillBoundaryPorts &&
      onApplySubflowPorts &&
      drillBoundaryPorts.hubInId &&
      drillBoundaryPorts.hubOutId &&
      (selectedNode.id === drillBoundaryPorts.hubInId || selectedNode.id === drillBoundaryPorts.hubOutId);
    return (
      <aside className="kea-flow-inspector">
        <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {selectedNode.id} · {kind}
        </p>
        <p className="kea-hint" style={{ marginTop: "0.35rem", maxWidth: "42rem" }}>
          {kind === "keaSubflowGraphIn"
            ? t("flow.inspectorSubflowGraphInHint")
            : t("flow.inspectorSubflowGraphOutHint")}
        </p>
        <label className="kea-label kea-label--block">
          {t("flow.inspectorLabel")}
          <DeferredCommitInput
            className="kea-input"
            committedValue={String(data.label ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, label: v })}
          />
        </label>
        <FlowNodeAccentColorFields t={t} nodeId={selectedNode.id} data={data} onPatchNode={onPatchNode} />
        {showBoundaryPorts && drillBoundaryPorts && onApplySubflowPorts && (
          <SubgraphPortsEditorBlock
            t={t}
            syncKeyPrefix={`${drillBoundaryPorts.targetSubgraphId}-drill`}
            ports={drillBoundaryPorts.ports}
            onCommit={(next) => onApplySubflowPorts(drillBoundaryPorts.targetSubgraphId, next)}
          />
        )}
        <label className="kea-label kea-label--block">
          {t("flow.inspectorNotes")}
          <DeferredCommitTextarea
            className="kea-textarea"
            rows={3}
            committedValue={String(data.notes ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, notes: v })}
          />
        </label>
      </aside>
    );
  }

  if (kind === "keaSubflow") {
    const parents = eligibleParentSubflows(flowNodes ?? [], selectedNode.id);
    return (
      <aside className="kea-flow-inspector">
        <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {selectedNode.id} · {kind}
        </p>
        <label className="kea-label kea-label--block">
          {t("flow.inspectorLabel")}
          <DeferredCommitInput
            className="kea-input"
            committedValue={String(data.label ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, label: v })}
          />
        </label>
        <p className="kea-hint" style={{ marginTop: "0.35rem", maxWidth: "42rem" }}>
          {t("flow.inspectorSubflowOrganizationalHint")}
        </p>
        <FlowNodeAccentColorFields t={t} nodeId={selectedNode.id} data={data} onPatchNode={onPatchNode} />
        {onSetNodeParent && canChangeSubflowParent(selectedNode.type) && parents.length > 0 && (
          <label className="kea-label kea-label--block">
            {t("flow.inspectorParentSubflow")}
            <select
              className="kea-input"
              value={selectedNode.parentId ?? ""}
              onChange={(e) => onSetNodeParent?.(selectedNode.id, e.target.value)}
            >
              <option value="">{t("flow.inspectorRefNone")}</option>
              {parents.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.label} ({p.id})
                </option>
              ))}
            </select>
          </label>
        )}
        <label className="kea-label kea-label--block">
          {t("flow.inspectorNotes")}
          <DeferredCommitTextarea
            className="kea-textarea"
            rows={3}
            committedValue={String(data.notes ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, notes: v })}
          />
        </label>
      </aside>
    );
  }

  if (kind === "keaSubgraph") {
    const parents = eligibleParentSubflows(flowNodes ?? [], selectedNode.id);
    const wfData = data as unknown as WorkflowCanvasNodeData;
    const ports: SubflowPortsConfig = wfData.subflow_ports ?? { inputs: [], outputs: [] };

    const commitPorts = (next: SubflowPortsConfig) => {
      if (onApplySubflowPorts) onApplySubflowPorts(selectedNode.id, next);
      else onPatchNode(selectedNode.id, { ...data, subflow_ports: next });
    };

    return (
      <aside className="kea-flow-inspector">
        <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {selectedNode.id} · {kind}
        </p>
        {onOpenSubgraphDrill && (
          <p style={{ marginBottom: "0.65rem" }}>
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              onClick={() => onOpenSubgraphDrill(selectedNode.id)}
            >
              {t("flow.inspectorOpenSubgraph")}
            </button>
          </p>
        )}
        <label className="kea-label kea-label--block">
          {t("flow.inspectorLabel")}
          <DeferredCommitInput
            className="kea-input"
            committedValue={String(data.label ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, label: v })}
          />
        </label>
        <p className="kea-hint" style={{ marginTop: "0.35rem", maxWidth: "42rem" }}>
          {t("flow.inspectorSubgraphHint")}
        </p>
        <FlowNodeAccentColorFields t={t} nodeId={selectedNode.id} data={data} onPatchNode={onPatchNode} />
        {onApplySubflowPorts && (
          <SubgraphPortsEditorBlock
            t={t}
            syncKeyPrefix={selectedNode.id}
            ports={ports}
            onCommit={commitPorts}
          />
        )}
        {onSetNodeParent && canChangeSubflowParent(selectedNode.type) && parents.length > 0 && (
          <label className="kea-label kea-label--block">
            {t("flow.inspectorParentSubflow")}
            <select
              className="kea-input"
              value={selectedNode.parentId ?? ""}
              onChange={(e) => onSetNodeParent?.(selectedNode.id, e.target.value)}
            >
              <option value="">{t("flow.inspectorRefNone")}</option>
              {parents.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.label} ({p.id})
                </option>
              ))}
            </select>
          </label>
        )}
        <label className="kea-label kea-label--block">
          {t("flow.inspectorNotes")}
          <DeferredCommitTextarea
            className="kea-textarea"
            rows={3}
            committedValue={String(data.notes ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, notes: v })}
          />
        </label>
      </aside>
    );
  }

  if (
    kind === "keaAliasPersistence" ||
    kind === "keaWritebackRaw" ||
    kind === "keaWritebackDataModeling" ||
    kind === "keaReferenceIndex"
  ) {
    const persistenceHint =
      kind === "keaAliasPersistence"
        ? t("flow.inspectorAliasPersistenceHint")
        : kind === "keaReferenceIndex"
          ? t("flow.inspectorReferenceIndexHint")
          : kind === "keaWritebackRaw"
            ? t("flow.inspectorWritebackRawHint")
            : t("flow.inspectorWritebackDataModelingHint");
    return (
      <aside className="kea-flow-inspector">
        <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {selectedNode.id} · {kind}
        </p>
        <label className="kea-label kea-label--block">
          {t("flow.inspectorLabel")}
          <DeferredCommitInput
            className="kea-input"
            committedValue={String(data.label ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, label: v })}
          />
        </label>
        <p className="kea-hint" style={{ marginTop: "0.35rem", maxWidth: "42rem" }}>
          {persistenceHint}
        </p>
        <FlowNodeAccentColorFields t={t} nodeId={selectedNode.id} data={data} onPatchNode={onPatchNode} />
        <label className="kea-label kea-label--block">
          {t("flow.inspectorNotes")}
          <DeferredCommitTextarea
            className="kea-textarea"
            rows={3}
            committedValue={String(data.notes ?? "")}
            syncKey={selectedNode.id}
            onCommit={(v) => onPatchNode(selectedNode.id, { ...data, notes: v })}
          />
        </label>
      </aside>
    );
  }

  return (
    <aside className="kea-flow-inspector">
      <h4 className="kea-flow-inspector__title">{t("flow.inspectorNodeTitle")}</h4>
      <p className="kea-hint" style={{ marginTop: 0 }}>
        {selectedNode.id} · {kind}
      </p>
      {!drift.ok && drift.hint && (
        <p className="kea-hint kea-hint--warn" role="status">
          {t(drift.hint as MessageKey)}
        </p>
      )}
      <label className="kea-label kea-label--block">
        {t("flow.inspectorLabel")}
        <DeferredCommitInput
          className="kea-input"
          committedValue={String(data.label ?? "")}
          syncKey={selectedNode.id}
          onCommit={(v) => onPatchNode(selectedNode.id, { ...data, label: v })}
        />
      </label>
      {onSetNodeParent &&
        canChangeSubflowParent(selectedNode.type) &&
        (flowNodes?.length ?? 0) > 0 &&
        eligibleParentSubflows(flowNodes ?? [], selectedNode.id).length > 0 && (
        <label className="kea-label kea-label--block">
          {t("flow.inspectorParentSubflow")}
          <select
            className="kea-input"
            value={selectedNode.parentId ?? ""}
            onChange={(e) => onSetNodeParent?.(selectedNode.id, e.target.value)}
          >
            <option value="">{t("flow.inspectorRefNone")}</option>
            {eligibleParentSubflows(flowNodes ?? [], selectedNode.id).map((p) => (
              <option key={p.id} value={p.id}>
                {p.label} ({p.id})
              </option>
            ))}
          </select>
        </label>
      )}
      <FlowNodeAccentColorFields t={t} nodeId={selectedNode.id} data={data} onPatchNode={onPatchNode} />
      {keaValidationRuleLayoutRfTypes.has(kind) && (
        <>
          <p className="kea-hint" style={{ marginTop: "0.35rem", maxWidth: "42rem" }}>
            {t("flow.inspectorValidationRuleHint")}
          </p>
          {validationRuleLayoutCtx === "source_view" && (
            <label className="kea-label kea-label--block">
              {t("flow.inspectorSourceViewIndex")}
              <select
                className="kea-input"
                value={
                  readNodeRef(data).source_view_index !== undefined &&
                  readNodeRef(data).source_view_index !== null
                    ? String(readNodeRef(data).source_view_index)
                    : ""
                }
                onChange={(e) => {
                  const v = e.target.value;
                  const nextRef: Record<string, unknown> = { ...readNodeRef(data) };
                  if (v === "") delete nextRef.source_view_index;
                  else nextRef.source_view_index = parseInt(v, 10);
                  onPatchNode(selectedNode.id, {
                    ...data,
                    ref: Object.keys(nextRef).length ? nextRef : undefined,
                  });
                }}
              >
                <option value="">{t("flow.inspectorHandlerUnset")}</option>
                {sourceViewScopeEntries.map(({ index, label }) => (
                  <option key={index} value={String(index)}>
                    {index}: {label}
                  </option>
                ))}
              </select>
            </label>
          )}
          {validationRuleLayoutCtx === "extraction" && !readNodeRef(data).extraction_global_validation && (
            <label className="kea-label kea-label--block">
              {t("flow.inspectorExtractionRuleRef")}
              <select
                className="kea-input"
                value={String(readNodeRef(data).extraction_rule_name ?? "")}
                onChange={(e) => {
                  const nextRef: Record<string, unknown> = { ...readNodeRef(data) };
                  if (!e.target.value.trim()) delete nextRef.extraction_rule_name;
                  else nextRef.extraction_rule_name = e.target.value;
                  onPatchNode(selectedNode.id, {
                    ...data,
                    ref: Object.keys(nextRef).length ? nextRef : undefined,
                  });
                }}
              >
                <option value="">{t("flow.inspectorRefNone")}</option>
                {extractionNames.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
          )}
          {validationRuleLayoutCtx === "aliasing" && !readNodeRef(data).aliasing_global_validation && (
            <label className="kea-label kea-label--block">
              {t("flow.inspectorAliasingRuleRef")}
              <select
                className="kea-input"
                value={String(readNodeRef(data).aliasing_rule_name ?? "")}
                onChange={(e) => {
                  const nextRef: Record<string, unknown> = { ...readNodeRef(data) };
                  if (!e.target.value.trim()) delete nextRef.aliasing_rule_name;
                  else nextRef.aliasing_rule_name = e.target.value;
                  onPatchNode(selectedNode.id, {
                    ...data,
                    ref: Object.keys(nextRef).length ? nextRef : undefined,
                  });
                }}
              >
                <option value="">{t("flow.inspectorRefNone")}</option>
                {aliasingNames.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
          )}
          <label className="kea-label kea-label--block">
            {t("flow.inspectorConfidenceRuleName")}
            <select
              className="kea-input"
              value={String(data.validation_rule_name ?? "")}
              onChange={(e) =>
                onPatchNode(selectedNode.id, {
                  ...data,
                  validation_rule_name: e.target.value || undefined,
                  label: e.target.value || data.label,
                })
              }
            >
              <option value="">{t("flow.inspectorHandlerUnset")}</option>
              {(() => {
                const rref = readNodeRef(data);
                if (validationRuleLayoutCtx === "source_view" && typeof rref.source_view_index === "number") {
                  return listSvConfidenceNames(workflowDoc, rref.source_view_index as number);
                }
                if (
                  validationRuleLayoutCtx === "source_view" &&
                  rref.shared_source_view_validation_chain === true &&
                  Array.isArray(rref.source_view_indices) &&
                  rref.source_view_indices.length > 0
                ) {
                  return listSvConfidenceNames(workflowDoc, Number(rref.source_view_indices[0]));
                }
                if (validationRuleLayoutCtx === "extraction" && rref.extraction_global_validation === true) {
                  return listKeyExtractionDataValidationConfidenceNames(workflowDoc);
                }
                if (
                  validationRuleLayoutCtx === "extraction" &&
                  rref.shared_extraction_validation_chain === true &&
                  Array.isArray(rref.extraction_rule_names) &&
                  rref.extraction_rule_names.length > 0
                ) {
                  return listKeConfidenceNames(workflowDoc, String(rref.extraction_rule_names[0]).trim());
                }
                if (validationRuleLayoutCtx === "extraction" && String(rref.extraction_rule_name ?? "").trim()) {
                  return listKeConfidenceNames(
                    workflowDoc,
                    String(rref.extraction_rule_name).trim()
                  );
                }
                if (validationRuleLayoutCtx === "aliasing" && rref.aliasing_global_validation === true) {
                  return listAliasingDataValidationConfidenceNames(workflowDoc);
                }
                if (
                  validationRuleLayoutCtx === "aliasing" &&
                  rref.shared_aliasing_validation_chain === true &&
                  Array.isArray(rref.aliasing_rule_names) &&
                  rref.aliasing_rule_names.length > 0
                ) {
                  return listAlConfidenceNames(workflowDoc, String(rref.aliasing_rule_names[0]).trim());
                }
                if (validationRuleLayoutCtx === "aliasing" && String(rref.aliasing_rule_name ?? "").trim()) {
                  return listAlConfidenceNames(
                    workflowDoc,
                    String(rref.aliasing_rule_name).trim()
                  );
                }
                return [];
              })().map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
        </>
      )}
      {kind === "keaSourceView" && (
        <>
          <p className="kea-hint" style={{ fontWeight: 600, marginBottom: "0.35rem" }}>
            {t("flow.inspectorSourceViewRef")}
          </p>
          {sourceViewScopeEntries.length > 0 && (
            <label className="kea-label kea-label--block">
              {t("flow.inspectorSourceViewFillFromScope")}
              <select
                key={`${selectedNode.id}-scope-pick`}
                className="kea-input"
                defaultValue=""
                onChange={(e) => {
                  const v = e.target.value;
                  if (v === "") return;
                  const i = parseInt(v, 10);
                  const svs = workflowDoc.source_views;
                  if (!Array.isArray(svs) || !svs[i] || typeof svs[i] !== "object" || Array.isArray(svs[i])) {
                    return;
                  }
                  const row = svs[i] as Record<string, unknown>;
                  const nextRef: Record<string, unknown> = {
                    ...readNodeRef(data),
                    source_view_index: i,
                  };
                  if (row.view_space != null) nextRef.view_space = String(row.view_space);
                  else delete nextRef.view_space;
                  if (row.view_external_id != null) nextRef.view_external_id = String(row.view_external_id);
                  else delete nextRef.view_external_id;
                  if (row.view_version != null) nextRef.view_version = String(row.view_version);
                  else delete nextRef.view_version;
                  onPatchNode(selectedNode.id, { ...data, ref: nextRef });
                }}
              >
                <option value="">{t("flow.inspectorHandlerUnset")}</option>
                {sourceViewScopeEntries.map(({ index, label }) => (
                  <option key={index} value={String(index)}>
                    {index}: {label}
                  </option>
                ))}
              </select>
            </label>
          )}
          <label className="kea-label kea-label--block">
            {t("flow.inspectorSourceViewIndex")}
            <input
              className="kea-input"
              inputMode="numeric"
              value={
                ref.source_view_index !== undefined && ref.source_view_index !== null
                  ? String(ref.source_view_index)
                  : ""
              }
              onChange={(e) => {
                const next = { ...readNodeRef(data) };
                const raw = e.target.value.trim();
                if (raw === "") delete next.source_view_index;
                else {
                  const n = parseInt(raw, 10);
                  if (Number.isFinite(n)) next.source_view_index = n;
                }
                onPatchNode(selectedNode.id, {
                  ...data,
                  ref: Object.keys(next).length ? next : undefined,
                });
              }}
            />
          </label>
          <label className="kea-label kea-label--block">
            {t("flow.inspectorSourceViewSpace")}
            <input
              className="kea-input"
              value={ref.view_space != null ? String(ref.view_space) : ""}
              onChange={(e) => {
                const next = { ...readNodeRef(data) };
                const v = e.target.value;
                if (!v.trim()) delete next.view_space;
                else next.view_space = v;
                onPatchNode(selectedNode.id, {
                  ...data,
                  ref: Object.keys(next).length ? next : undefined,
                });
              }}
            />
          </label>
          <label className="kea-label kea-label--block">
            {t("flow.inspectorSourceViewExternalId")}
            <input
              className="kea-input"
              value={ref.view_external_id != null ? String(ref.view_external_id) : ""}
              onChange={(e) => {
                const next = { ...readNodeRef(data) };
                const v = e.target.value;
                if (!v.trim()) delete next.view_external_id;
                else next.view_external_id = v;
                onPatchNode(selectedNode.id, {
                  ...data,
                  ref: Object.keys(next).length ? next : undefined,
                });
              }}
            />
          </label>
          <label className="kea-label kea-label--block">
            {t("flow.inspectorSourceViewVersion")}
            <input
              className="kea-input"
              value={ref.view_version != null ? String(ref.view_version) : ""}
              onChange={(e) => {
                const next = { ...readNodeRef(data) };
                const v = e.target.value;
                if (!v.trim()) delete next.view_version;
                else next.view_version = v;
                onPatchNode(selectedNode.id, {
                  ...data,
                  ref: Object.keys(next).length ? next : undefined,
                });
              }}
            />
          </label>
          {(() => {
            const svIx = resolveSourceViewIndex(readNodeRef(data));
            if (
              svIx === null ||
              !Array.isArray(workflowDoc.source_views) ||
              svIx >= workflowDoc.source_views.length
            ) {
              return null;
            }
            const view = (workflowDoc.source_views as JsonObject[])[svIx];
            const filters: JsonObject[] = Array.isArray(view.filters)
              ? (view.filters as unknown[]).filter(
                  (x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x)
                )
              : [];
            const setFilters = (nextFilters: JsonObject[]) => {
              onPatchWorkflowScope((doc) => patchSourceViewFilters(doc, svIx, nextFilters));
            };
            return (
              <>
                <h4 className="kea-section-title" style={{ fontSize: "0.9rem", marginTop: "0.75rem" }}>
                  {t("sourceViews.filters")}
                </h4>
                <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.5rem", maxWidth: "42rem" }}>
                  {t("sourceViews.filtersCombineHint")}
                </p>
                {filters.map((row, fi) => (
                  <SourceViewFilterNodeEditor
                    key={`flow-svf-${svIx}-${fi}-${selectedNode.id}`}
                    t={t}
                    value={row}
                    onChange={(next) => {
                      const fl = [...filters];
                      fl[fi] = next;
                      setFilters(fl);
                    }}
                    onRemove={() => {
                      const fl = [...filters];
                      fl.splice(fi, 1);
                      setFilters(fl);
                    }}
                  />
                ))}
                <div className="kea-toolbar-inline" style={{ marginTop: 8, flexWrap: "wrap", gap: 8 }}>
                  <button
                    type="button"
                    className="kea-btn kea-btn--sm"
                    onClick={() => setFilters([...filters, emptyLeaf()])}
                  >
                    {t("sourceViews.filterAddLeaf")}
                  </button>
                  <button
                    type="button"
                    className="kea-btn kea-btn--sm"
                    onClick={() => setFilters([...filters, emptyAnd()])}
                  >
                    {t("sourceViews.filterAddAnd")}
                  </button>
                  <button
                    type="button"
                    className="kea-btn kea-btn--sm"
                    onClick={() => setFilters([...filters, emptyOr()])}
                  >
                    {t("sourceViews.filterAddOr")}
                  </button>
                  <button
                    type="button"
                    className="kea-btn kea-btn--sm"
                    onClick={() => setFilters([...filters, emptyNot()])}
                  >
                    {t("sourceViews.filterAddNot")}
                  </button>
                </div>
              </>
            );
          })()}
        </>
      )}
      {(kind === "keaExtraction" || kind === "keaAliasing") && (
        <label className="kea-label kea-label--block">
          {t("flow.inspectorHandler")}
          <select
            className="kea-input"
            value={String(data.handler_id ?? "")}
            onChange={(e) =>
              onPatchNode(selectedNode.id, {
                ...data,
                handler_id: e.target.value || undefined,
                handler_family: kind === "keaExtraction" ? "extraction" : "aliasing",
                preset_from_palette: false,
              })
            }
          >
            <option value="">{t("flow.inspectorHandlerUnset")}</option>
            {(kind === "keaExtraction" ? EXTRACTION_HANDLER_IDS : ALIASING_HANDLER_IDS).map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
      )}
      {kind === "keaValidation" && (
        <label className="kea-label kea-label--block">
          {t("flow.inspectorAnnotationKind")}
          <select
            className="kea-input"
            value={String(data.annotation_kind ?? ANNOTATION_KINDS[0])}
            onChange={(e) =>
              onPatchNode(selectedNode.id, {
                ...data,
                annotation_kind: e.target.value,
              })
            }
          >
            {ANNOTATION_KINDS.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </label>
      )}
      {kind === "keaExtraction" && (
        <label className="kea-label kea-label--block">
          {t("flow.inspectorExtractionRuleRef")}
          <select
            className="kea-input"
            value={String(
              (data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
                ? (data.ref as Record<string, unknown>).extraction_rule_name
                : undefined) ?? ""
            )}
            onChange={(e) => {
              const ref: Record<string, unknown> = {
                ...(typeof data.ref === "object" && data.ref && !Array.isArray(data.ref)
                  ? (data.ref as Record<string, unknown>)
                  : {}),
              };
              if (!e.target.value.trim()) delete ref.extraction_rule_name;
              else ref.extraction_rule_name = e.target.value;
              onPatchNode(selectedNode.id, { ...data, ref });
            }}
          >
            <option value="">{t("flow.inspectorRefNone")}</option>
            {extractionNames.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
      )}
      {kind === "keaExtraction" &&
        String(
          (data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
            ? (data.ref as Record<string, unknown>).extraction_rule_name
            : undefined) ?? ""
        ).trim() && (
          <div style={{ marginTop: "0.5rem" }}>
            <h4 className="kea-section-title" style={{ fontSize: "0.9rem", marginBottom: "0.35rem" }}>
              {t("flow.inspectorScopeFiltersYaml")}
            </h4>
            <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.35rem", maxWidth: "42rem" }}>
              {t("flow.inspectorScopeFiltersYamlHint")}
            </p>
            {scopeFiltersYamlInvalid ? (
              <p className="kea-hint kea-hint--warn" role="status">
                {t("flow.inspectorScopeFiltersYamlInvalid")}
              </p>
            ) : null}
            <textarea
              className="kea-textarea"
              spellCheck={false}
              style={{ minHeight: 120, fontFamily: "ui-monospace, monospace", fontSize: "0.8rem" }}
              value={scopeFiltersYaml}
              onChange={(e) => {
                setScopeFiltersYaml(e.target.value);
                setScopeFiltersYamlInvalid(false);
              }}
              onBlur={() => {
                const name = String(
                  (data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
                    ? (data.ref as Record<string, unknown>).extraction_rule_name
                    : undefined) ?? ""
                ).trim();
                if (!name) return;
                try {
                  const parsed = YAML.parse(scopeFiltersYaml);
                  const sf =
                    parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
                      ? (parsed as Record<string, unknown>)
                      : {};
                  onPatchWorkflowScope((doc) => patchExtractionRuleScopeFilters(doc, name, sf));
                  setScopeFiltersYamlInvalid(false);
                } catch {
                  setScopeFiltersYamlInvalid(true);
                }
              }}
            />
          </div>
        )}
      {kind === "keaAliasing" && (
        <label className="kea-label kea-label--block">
          {t("flow.inspectorAliasingRuleRef")}
          <select
            className="kea-input"
            value={String(
              (data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
                ? (data.ref as Record<string, unknown>).aliasing_rule_name
                : undefined) ?? ""
            )}
            onChange={(e) => {
              const ref: Record<string, unknown> = {
                ...(typeof data.ref === "object" && data.ref && !Array.isArray(data.ref)
                  ? (data.ref as Record<string, unknown>)
                  : {}),
              };
              if (!e.target.value.trim()) delete ref.aliasing_rule_name;
              else ref.aliasing_rule_name = e.target.value;
              onPatchNode(selectedNode.id, { ...data, ref });
            }}
          >
            <option value="">{t("flow.inspectorRefNone")}</option>
            {aliasingNames.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
      )}
      {kind === "keaAliasing" &&
        String(
          (data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
            ? (data.ref as Record<string, unknown>).aliasing_rule_name
            : undefined) ?? ""
        ).trim() && (
          <div style={{ marginTop: "0.5rem" }}>
            <h4 className="kea-section-title" style={{ fontSize: "0.9rem", marginBottom: "0.35rem" }}>
              {t("flow.inspectorScopeFiltersYaml")}
            </h4>
            <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.35rem", maxWidth: "42rem" }}>
              {t("flow.inspectorScopeFiltersYamlHint")}
            </p>
            {scopeFiltersYamlInvalid ? (
              <p className="kea-hint kea-hint--warn" role="status">
                {t("flow.inspectorScopeFiltersYamlInvalid")}
              </p>
            ) : null}
            <textarea
              className="kea-textarea"
              spellCheck={false}
              style={{ minHeight: 120, fontFamily: "ui-monospace, monospace", fontSize: "0.8rem" }}
              value={scopeFiltersYaml}
              onChange={(e) => {
                setScopeFiltersYaml(e.target.value);
                setScopeFiltersYamlInvalid(false);
              }}
              onBlur={() => {
                const name = String(
                  (data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
                    ? (data.ref as Record<string, unknown>).aliasing_rule_name
                    : undefined) ?? ""
                ).trim();
                if (!name) return;
                try {
                  const parsed = YAML.parse(scopeFiltersYaml);
                  const sf =
                    parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
                      ? (parsed as Record<string, unknown>)
                      : {};
                  onPatchWorkflowScope((doc) => patchAliasingRuleScopeFilters(doc, name, sf));
                  setScopeFiltersYamlInvalid(false);
                } catch {
                  setScopeFiltersYamlInvalid(true);
                }
              }}
            />
          </div>
        )}
      <label className="kea-label kea-label--block">
        {t("flow.inspectorNotes")}
        <DeferredCommitTextarea
          className="kea-textarea"
          rows={3}
          committedValue={String(data.notes ?? "")}
          syncKey={selectedNode.id}
          onCommit={(v) => onPatchNode(selectedNode.id, { ...data, notes: v })}
        />
      </label>
    </aside>
  );
}
