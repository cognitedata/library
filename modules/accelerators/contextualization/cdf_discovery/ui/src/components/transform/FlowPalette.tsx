import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import {
  setTransformPaletteDragData,
} from "./transformFlowDrag";

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

const PALETTE_GROUPS: { titleKey: MessageKey; stages: TransformCanvasNodeKind[] }[] = [
  {
    titleKey: "transform.palette.groupExtract",
    stages: ["query_view", "query_raw", "query_classic", "query_sql"],
  },
  {
    titleKey: "transform.palette.groupTransform",
    stages: ["transform", "field_map", "score", "filter", "join", "merge", "build_index"],
  },
  {
    titleKey: "transform.palette.groupLoad",
    stages: ["save_view", "save_raw", "save_classic", "spark_transform"],
  },
  {
    titleKey: "transform.palette.groupOrchestration",
    stages: [
      "function_ref",
      "transformation_ref",
      "subworkflow",
      "dynamic_fanout",
      "simulation",
      "cdf_task",
    ],
  },
];

export function FlowPalette({ t, readOnly = false }: Props) {
  return (
    <div className="transform-flow-palette" role="complementary" aria-label={t("transform.paletteAria")}>
      {PALETTE_GROUPS.map((group) => (
        <section key={group.titleKey} className="transform-flow-palette__group">
          <h3 className="transform-flow-palette__heading">{t(group.titleKey)}</h3>
          <ul className="transform-flow-palette__list">
            {group.stages.map((stage) => (
              <li key={stage}>
                <button
                  type="button"
                  className="transform-flow-palette__item"
                  draggable={!readOnly}
                  disabled={readOnly}
                  onDragStart={(e) =>
                    setTransformPaletteDragData(e, { kind: "etl_stage", stage })
                  }
                  title={t(paletteKey(stage))}
                >
                  {t(paletteKey(stage))}
                </button>
              </li>
            ))}
          </ul>
        </section>
      ))}
      <p className="transform-flow-palette__hint">{t("transform.palette.hint")}</p>
    </div>
  );
}
