import { useCallback } from "react";
import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { FilterNodeStageConfigFields } from "../FilterNodeStageConfigFields";
import { useFilterNodeConfigDraft } from "../../utils/useFilterNodeConfigDraft";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  nodeId: string;
  data: WorkflowCanvasNodeData;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
  t: TFn;
};

/** Inline filter config in the flow right-hand inspector (``data.config`` on the canvas node). */
export function FilterNodeInspectorFields({ nodeId, data, onPatchNode, t }: Props) {
  const persist = useCallback(
    (config: Record<string, unknown>, label: string) => {
      onPatchNode(nodeId, {
        ...data,
        config,
        label,
      });
    },
    [data, nodeId, onPatchNode]
  );

  const { draftCfg, updateDraft, persistDraft } = useFilterNodeConfigDraft(data, persist);

  return (
    <div style={{ marginTop: "0.75rem" }}>
      <FilterNodeStageConfigFields
        cfg={draftCfg}
        onChange={updateDraft}
        onPersist={persistDraft}
        t={t}
        allowEmpty={false}
        fieldKey={nodeId}
      />
    </div>
  );
}
