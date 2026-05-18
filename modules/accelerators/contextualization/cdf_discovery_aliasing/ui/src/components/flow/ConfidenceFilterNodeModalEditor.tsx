import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasDocument, WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { ConfidenceFilterNodeStageConfigFields } from "../ConfidenceFilterNodeStageConfigFields";
import { patchConfidenceFilterNodeById } from "../../utils/confidenceFilterCanvasUtils";
import { useConfidenceFilterNodeConfigDraft } from "../../utils/useConfidenceFilterNodeConfigDraft";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  nodeId: string;
  nodeData: WorkflowCanvasNodeData;
  onChange: (updater: (canvas: WorkflowCanvasDocument) => WorkflowCanvasDocument) => void;
  t: TFn;
};

export function ConfidenceFilterNodeModalEditor({ nodeId, nodeData, onChange, t }: Props) {
  const persist = (config: Record<string, unknown>) => {
    onChange((canvas) => patchConfidenceFilterNodeById(canvas, nodeId, config));
  };

  const { draftCfg, updateDraft, persistDraft } = useConfidenceFilterNodeConfigDraft(
    nodeData,
    (config, _label) => persist(config)
  );

  return (
    <ConfidenceFilterNodeStageConfigFields
      cfg={draftCfg}
      onChange={updateDraft}
      onPersist={persistDraft}
      t={t}
    />
  );
}
