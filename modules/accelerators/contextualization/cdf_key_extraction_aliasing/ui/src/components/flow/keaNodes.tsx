import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";

function nodeClass(selected: boolean, variant: string): string {
  return `kea-flow-node kea-flow-node--${variant}${selected ? " kea-flow-node--selected" : ""}`;
}

export function KeaStartNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  return (
    <div className={nodeClass(!!selected, "start")}>
      <div className="kea-flow-node__badge">start</div>
      <div className="kea-flow-node__title">{d.label || "Start"}</div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

export function KeaEndNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  return (
    <div className={nodeClass(!!selected, "end")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">end</div>
      <div className="kea-flow-node__title">{d.label || "End"}</div>
    </div>
  );
}

export function KeaSourceViewNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  return (
    <div className={nodeClass(!!selected, "source")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">source</div>
      <div className="kea-flow-node__title">{d.label || "Source view"}</div>
      {d.ref?.view_external_id && (
        <div className="kea-flow-node__meta">{String(d.ref.view_external_id)}</div>
      )}
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

export function KeaExtractionNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const handler = d.handler_id ? String(d.handler_id) : "—";
  return (
    <div className={nodeClass(!!selected, "extract")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">
        extract{d.preset_from_palette ? " ●" : ""}
      </div>
      <div className="kea-flow-node__title">{d.label || "Extraction"}</div>
      <div className="kea-flow-node__meta">{handler}</div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

export function KeaAliasingNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const handler = d.handler_id ? String(d.handler_id) : "—";
  return (
    <div className={nodeClass(!!selected, "alias")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">
        alias{d.preset_from_palette ? " ●" : ""}
      </div>
      <div className="kea-flow-node__title">{d.label || "Aliasing"}</div>
      <div className="kea-flow-node__meta">{handler}</div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

export function KeaValidationNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const ak = d.annotation_kind ? String(d.annotation_kind) : "—";
  return (
    <div className={nodeClass(!!selected, "validation")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">annotation</div>
      <div className="kea-flow-node__title">{d.label || "Validation"}</div>
      <div className="kea-flow-node__meta">{ak}</div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

/** Scope `confidence_match_rules[]` — evaluated during listing / extraction / aliasing per parent config. */
export function KeaValidationRuleNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const ctx = d.validation_rule_context ? String(d.validation_rule_context) : "—";
  const ruleName = d.confidence_match_rule_name ? String(d.confidence_match_rule_name) : "—";
  return (
    <div className={nodeClass(!!selected, "validation-rule")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">match</div>
      <div className="kea-flow-node__title">{d.label || "Match validation"}</div>
      <div className="kea-flow-node__meta">
        {ctx} · {ruleName}
      </div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

/** fn_dm_alias_persistence — writes aliases (and optional FK strings) back to describable instances. */
export function KeaAliasPersistenceNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  return (
    <div className={nodeClass(!!selected, "persist-alias")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">persist</div>
      <div className="kea-flow-node__title">{d.label || "Alias write-back"}</div>
      <div className="kea-flow-node__meta">fn_dm_alias_persistence</div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}

/** fn_dm_reference_index — inverted RAW index from FK / document reference JSON in extraction store. */
export function KeaReferenceIndexNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  return (
    <div className={nodeClass(!!selected, "ref-index")}>
      <Handle type="target" position={Position.Left} id="in" />
      <div className="kea-flow-node__badge">index</div>
      <div className="kea-flow-node__title">{d.label || "Reference index"}</div>
      <div className="kea-flow-node__meta">fn_dm_reference_index</div>
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}
