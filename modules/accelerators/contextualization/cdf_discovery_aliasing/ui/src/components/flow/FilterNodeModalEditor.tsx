import { useCallback } from "react";
import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasDocument, WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { FilterNodeStageConfigFields } from "../FilterNodeStageConfigFields";
import { patchFilterNodeById } from "../../utils/filtersCanvasUtils";
import { useFilterNodeConfigDraft } from "../../utils/useFilterNodeConfigDraft";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type WorkflowCanvasPatch =
  | WorkflowCanvasDocument
  | ((prev: WorkflowCanvasDocument) => WorkflowCanvasDocument);

type Props = {
  nodeId: string;
  nodeData: WorkflowCanvasNodeData | undefined;
  onChange: (patch: WorkflowCanvasPatch) => void;
  t: TFn;
};

/** Flow double-click editor: one filter node only, config read from the clicked RF node. */
export function FilterNodeModalEditor({ nodeId, nodeData, onChange, t }: Props) {
  const persist = useCallback(
    (config: Record<string, unknown>, _label: string) => {
      onChange((canvas) => patchFilterNodeById(canvas, nodeId, config));
    },
    [nodeId, onChange]
  );

  const { draftCfg, updateDraft, persistDraft } = useFilterNodeConfigDraft(nodeData, persist);

  return (
    <FilterNodeStageConfigFields
      cfg={draftCfg}
      onChange={updateDraft}
      onPersist={persistDraft}
      t={t}
      allowEmpty={false}
      fieldKey={nodeId}
    />
  );
}
