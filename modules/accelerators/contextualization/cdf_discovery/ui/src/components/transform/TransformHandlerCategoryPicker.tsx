import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import type { HandlerDropMenuGroup, HandlerDropMenuOption } from "./handlerDropMenuOptions";
import type { PaletteDragPayload } from "./transformFlowDrag";
import { handlerDropMenuOptionTooltip } from "./handlerDropMenuOptions";
import {
  handlerCategoryPaletteGroupId,
  paletteGroupIdForStage,
} from "../../utils/etlPaletteGroupColors";
import { palettePayloadTooltip, paletteStageTooltip } from "../../utils/paletteStageTooltip";
import { setTransformPaletteDragData } from "./transformFlowDrag";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function sortOptions(options: readonly HandlerDropMenuOption[], t: TFn): HandlerDropMenuOption[] {
  return [...options].sort((a, b) =>
    t(a.labelKey).localeCompare(t(b.labelKey), undefined, { sensitivity: "base" })
  );
}

type PaletteProps = {
  variant: "palette";
  groups: readonly HandlerDropMenuGroup[];
  t: TFn;
  readOnly?: boolean;
  /** Canvas stages rendered after handler items in a category (e.g. score under Derive). */
  extraPaletteStagesByGroupId?: Partial<Record<string, readonly TransformCanvasNodeKind[]>>;
};

type MenuProps = {
  variant: "menu";
  groups: readonly HandlerDropMenuGroup[];
  t: TFn;
  onPick: (option: HandlerDropMenuOption) => void;
};

export type TransformHandlerCategoryPickerProps = PaletteProps | MenuProps;

export function TransformHandlerCategoryPicker(props: TransformHandlerCategoryPickerProps) {
  const { groups, t } = props;

  return (
    <>
      {groups.map((group) => {
        const options = sortOptions(group.options, t);
        const extraStages =
          props.variant === "palette" ? (props.extraPaletteStagesByGroupId?.[group.id] ?? []) : [];
        if (!options.length && extraStages.length === 0) return null;
        const categoryAccentId = handlerCategoryPaletteGroupId(group.id);
        return (
          <details
            key={group.id}
            className={`transform-handler-category transform-handler-category--${categoryAccentId}`}
          >
            <summary className="transform-handler-category__summary">{t(group.labelKey)}</summary>
            {props.variant === "palette" ? (
              <ul className="transform-flow-palette__list transform-handler-category__list">
                {options.map((opt) => {
                  const tooltip = handlerDropMenuOptionTooltip(opt);
                  return (
                    <li
                      key={opt.id}
                      className="transform-flow-palette__item-row"
                      data-tooltip={tooltip || undefined}
                    >
                      <button
                        type="button"
                        className={`transform-flow-palette__item transform-flow-palette__item--${categoryAccentId}`}
                        draggable={!props.readOnly}
                        disabled={props.readOnly}
                        onDragStart={(e) =>
                          setTransformPaletteDragData(e, opt.payload as PaletteDragPayload)
                        }
                      >
                        {t(opt.labelKey)}
                      </button>
                    </li>
                  );
                })}
                {extraStages.map((stage) => {
                  const labelKey = `transform.palette.${stage}` as MessageKey;
                  const tooltip = paletteStageTooltip(stage, t);
                  const stageAccentId = paletteGroupIdForStage(stage);
                  return (
                    <li
                      key={`stage-${stage}`}
                      className="transform-flow-palette__item-row"
                      data-tooltip={tooltip}
                    >
                      <button
                        type="button"
                        className={`transform-flow-palette__item transform-flow-palette__item--${stageAccentId}`}
                        draggable={!props.readOnly}
                        disabled={props.readOnly}
                        onDragStart={(e) =>
                          setTransformPaletteDragData(e, { kind: "etl_stage", stage })
                        }
                      >
                        {t(labelKey)}
                      </button>
                    </li>
                  );
                })}
              </ul>
            ) : (
              <div className="transform-handler-category__menu">
                {options.map((opt) => {
                  const tooltip = handlerDropMenuOptionTooltip(opt);
                  return (
                    <button
                      key={opt.id}
                      type="button"
                      className="disc-btn"
                      role="menuitem"
                      onClick={() => props.onPick(opt)}
                      title={palettePayloadTooltip(opt.payload as PaletteDragPayload, t) || tooltip}
                    >
                      {t(opt.labelKey)}
                    </button>
                  );
                })}
              </div>
            )}
          </details>
        );
      })}
    </>
  );
}
