import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import {
  FUSION_NODE_KINDS,
  PIPELINE_ORCHESTRATION_NODE_KINDS,
} from "../../utils/transformNodeEditorKinds";
import { sortPaletteStages } from "../../utils/canvasNodeKindLabel";
import {
  paletteGroupIdForStage,
  type EtlPaletteGroupId,
} from "../../utils/etlPaletteGroupColors";
import { paletteStageTooltip } from "../../utils/paletteStageTooltip";
import { transformHandlerDropMenuGroups } from "./handlerDropMenuOptions";
import type { PaletteDragPayload } from "./transformFlowDrag";
import {
  setTransformPaletteDragData,
} from "./transformFlowDrag";
import { TransformHandlerCategoryPicker } from "./TransformHandlerCategoryPicker";

export type { PaletteDragPayload } from "./transformFlowDrag";
export {
  getTransformFlowDropPayload,
  getTransformPaletteDropPayload,
  setDataTreeEntityDragData,
  setTransformPaletteDragData,
} from "./transformFlowDrag";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  readOnly?: boolean;
};

function paletteKey(stage: TransformCanvasNodeKind): MessageKey {
  return `transform.palette.${stage}` as MessageKey;
}

type PaletteGroupDef =
  | { id: EtlPaletteGroupId; titleKey: MessageKey; stages: TransformCanvasNodeKind[] }
  | { id: "transform"; titleKey: MessageKey; transformGroup: true };

const PALETTE_GROUPS: PaletteGroupDef[] = [
  {
    id: "extract",
    titleKey: "transform.palette.groupExtract",
    stages: ["query_view", "query_raw", "query_classic", "query_sql", "query_records"],
  },
  {
    id: "transform",
    titleKey: "transform.palette.groupTransform",
    transformGroup: true,
  },
  {
    id: "orchestration",
    titleKey: "transform.palette.groupOrchestration",
    stages: [...PIPELINE_ORCHESTRATION_NODE_KINDS],
  },
  {
    id: "contextualization",
    titleKey: "transform.palette.groupContextualization",
    stages: ["file_annotation", "workflow_fanout_plan", "build_index"],
  },
  {
    id: "load",
    titleKey: "transform.palette.groupLoad",
    stages: ["save_view", "save_raw", "save_classic", "save_records", "save_stream"],
  },
  {
    id: "fusion",
    titleKey: "transform.palette.groupFusion",
    stages: [...FUSION_NODE_KINDS],
  },
  {
    id: "debug",
    titleKey: "transform.palette.groupDebug",
    stages: ["node_preview"],
  },
];

export function FlowPalette({ t, readOnly = false }: Props) {
  const handlerGroups = transformHandlerDropMenuGroups();

  return (
    <div className="transform-flow-palette" role="complementary" aria-label={t("transform.paletteAria")}>
      {PALETTE_GROUPS.map((group) => (
        <details
          key={group.titleKey}
          className={`transform-flow-palette__group${
            group.id !== "transform" ? ` transform-flow-palette__group--${group.id}` : ""
          }`}
        >
          <summary className="transform-flow-palette__heading">{t(group.titleKey)}</summary>
          <div className="transform-flow-palette__group-body">
          {"transformGroup" in group && group.transformGroup ? (
            <div className="transform-flow-palette__transform-body">
              <TransformHandlerCategoryPicker
                variant="palette"
                groups={handlerGroups}
                t={t}
                readOnly={readOnly}
                extraPaletteStagesByGroupId={{ derive: ["score"] }}
              />
            </div>
          ) : "stages" in group ? (
            <ul className="transform-flow-palette__list">
              {sortPaletteStages(group.stages, t).map((stage) => {
                const tooltip = paletteStageTooltip(stage, t);
                const accentId = paletteGroupIdForStage(stage);
                return (
                  <li
                    key={stage}
                    className="transform-flow-palette__item-row"
                    data-tooltip={tooltip}
                  >
                    <button
                      type="button"
                      className={`transform-flow-palette__item transform-flow-palette__item--${accentId}`}
                      draggable={!readOnly}
                      disabled={readOnly}
                      onDragStart={(e) =>
                        setTransformPaletteDragData(e, { kind: "etl_stage", stage })
                      }
                    >
                      {t(paletteKey(stage))}
                    </button>
                  </li>
                );
              })}
            </ul>
          ) : null}
          </div>
        </details>
      ))}
      <p className="transform-flow-palette__hint">{t("transform.palette.hint")}</p>
    </div>
  );
}
