import { useCallback } from "react";
import type { MessageKey } from "../i18n";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { FilterNodeStageConfigFields } from "./FilterNodeStageConfigFields";
import { patchFilterNode, type FilterNodeRef } from "../utils/filtersCanvasUtils";
import { useFilterNodeConfigDraft } from "../utils/useFilterNodeConfigDraft";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  ref: FilterNodeRef;
  t: TFn;
};

export function FilterNodeConfigFields({ canvas, onChange, ref, t }: Props) {
  const persist = useCallback(
    (config: Record<string, unknown>, label: string) => {
      onChange(
        patchFilterNode(canvas, ref, {
          ...config,
          description: config.description ?? label,
        })
      );
    },
    [canvas, onChange, ref]
  );

  const { draftCfg, updateDraft, persistDraft } = useFilterNodeConfigDraft(
    ref.node.data,
    persist
  );

  return (
    <div className="kea-loc-fields" style={{ maxWidth: "52rem" }}>
      <FilterNodeStageConfigFields
        cfg={draftCfg}
        onChange={updateDraft}
        onPersist={persistDraft}
        t={t}
        allowEmpty={false}
        fieldKey={ref.node.id}
      />
    </div>
  );
}
