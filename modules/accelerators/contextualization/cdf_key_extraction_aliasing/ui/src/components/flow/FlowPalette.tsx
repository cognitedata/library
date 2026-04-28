import type { MessageKey } from "../../i18n";
import { confidenceMatchDefinitionIds } from "../../utils/confidenceMatchDefinitionIds";
import { ALIASING_HANDLER_IDS, EXTRACTION_HANDLER_IDS } from "./handlerRegistry";

export type PaletteDragPayload =
  | { kind: "extraction"; handlerId: string; preset: true }
  | { kind: "aliasing"; handlerId: string; preset: true }
  | { kind: "match_definition"; ruleId: string }
  | {
      kind: "structural";
      nodeKind:
        | "source_view"
        | "subflow"
        | "subgraph"
        | "reference_index"
        | "alias_persistence"
        | "match_validation_source_view"
        | "match_validation_extraction"
        | "match_validation_aliasing";
    };

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  /** Workflow scope YAML; used to list `validation_rule_definitions` ids. */
  scopeDocument: Record<string, unknown>;
};

const DRAG_MIME = "application/x-kea-flow-palette";

export function setPaletteDragData(e: React.DragEvent, payload: PaletteDragPayload) {
  e.dataTransfer.setData(DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function getPaletteDropPayload(e: React.DragEvent): PaletteDragPayload | null {
  const raw = e.dataTransfer.getData(DRAG_MIME);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as PaletteDragPayload;
  } catch {
    return null;
  }
}

export function FlowPalette({ t, scopeDocument }: Props) {
  const matchDefIds = confidenceMatchDefinitionIds(scopeDocument);
  return (
    <div className="kea-flow-palette" role="complementary" aria-label={t("flow.paletteAria")}>
      <p className="kea-flow-palette__heading">{t("flow.paletteStructural")}</p>
      <div className="kea-flow-palette__buttons">
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          onDragStart={(e) => setPaletteDragData(e, { kind: "structural", nodeKind: "source_view" })}
        >
          {t("flow.structuralSourceView")}
        </button>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          onDragStart={(e) => setPaletteDragData(e, { kind: "structural", nodeKind: "subflow" })}
        >
          {t("flow.structuralSubflow")}
        </button>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          onDragStart={(e) => setPaletteDragData(e, { kind: "structural", nodeKind: "subgraph" })}
        >
          {t("flow.structuralSubgraph")}
        </button>
      </div>
      <p className="kea-flow-palette__heading">{t("flow.paletteCdfTasks")}</p>
      <div className="kea-flow-palette__buttons">
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          onDragStart={(e) =>
            setPaletteDragData(e, { kind: "structural", nodeKind: "reference_index" })
          }
        >
          {t("flow.structuralReferenceIndex")}
        </button>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          onDragStart={(e) =>
            setPaletteDragData(e, { kind: "structural", nodeKind: "alias_persistence" })
          }
        >
          {t("flow.structuralAliasPersistence")}
        </button>
      </div>
      <p className="kea-flow-palette__heading">{t("flow.paletteValidationRuleDefinitions")}</p>
      {matchDefIds.length === 0 ? (
        <p className="kea-hint" style={{ fontSize: "0.8rem", margin: "0 0 0.75rem" }}>
          {t("flow.paletteValidationRuleDefinitionsEmpty")}
        </p>
      ) : (
        <ul className="kea-flow-palette__list kea-flow-palette__list--scroll">
          {matchDefIds.map((id) => (
            <li key={id}>
              <button
                type="button"
                className="kea-flow-palette__item"
                draggable
                title={id}
                onDragStart={(e) =>
                  setPaletteDragData(e, { kind: "match_definition", ruleId: id })
                }
              >
                {id}
              </button>
            </li>
          ))}
        </ul>
      )}
      <p className="kea-flow-palette__heading">{t("flow.paletteExtractionHandlers")}</p>
      <ul className="kea-flow-palette__list">
        {EXTRACTION_HANDLER_IDS.map((id) => (
          <li key={id}>
            <button
              type="button"
              className="kea-flow-palette__item"
              draggable
              onDragStart={(e) =>
                setPaletteDragData(e, { kind: "extraction", handlerId: id, preset: true })
              }
            >
              {id}
            </button>
          </li>
        ))}
      </ul>
      <p className="kea-flow-palette__heading">{t("flow.paletteAliasingHandlers")}</p>
      <ul className="kea-flow-palette__list kea-flow-palette__list--scroll">
        {ALIASING_HANDLER_IDS.map((id) => (
          <li key={id}>
            <button
              type="button"
              className="kea-flow-palette__item"
              draggable
              onDragStart={(e) =>
                setPaletteDragData(e, { kind: "aliasing", handlerId: id, preset: true })
              }
            >
              {id}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
