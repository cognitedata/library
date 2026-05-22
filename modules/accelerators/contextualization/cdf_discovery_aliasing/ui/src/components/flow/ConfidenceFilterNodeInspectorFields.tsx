import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { ConfidenceFilterNodeStageConfigFields } from "../ConfidenceFilterNodeStageConfigFields";
import { useConfidenceFilterNodeConfigDraft } from "../../utils/useConfidenceFilterNodeConfigDraft";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  nodeId: string;
  data: WorkflowCanvasNodeData;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
  t: TFn;
};

export function ConfidenceFilterNodeInspectorFields({ nodeId, data, onPatchNode, t }: Props) {
  const persist = (config: Record<string, unknown>, label: string) => {
    onPatchNode(nodeId, {
      ...data,
      config,
      label,
    });
  };

  const { draftCfg, updateDraft, persistDraft } = useConfidenceFilterNodeConfigDraft(data, persist);

  return (
    <div className="discovery-stack" style={{ marginTop: "0.5rem" }}>
      <ConfidenceFilterNodeStageConfigFields
        cfg={draftCfg}
        onChange={updateDraft}
        onPersist={persistDraft}
        t={t}
      />
    </div>
  );
}
