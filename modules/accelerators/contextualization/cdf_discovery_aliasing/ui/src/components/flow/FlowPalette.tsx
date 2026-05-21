import type { MessageKey } from "../../i18n";
import { confidenceMatchDefinitionIds } from "../../utils/confidenceMatchDefinitionIds";
import {
  TRANSFORM_HANDLER_IDS,
  type DiscoveryTransformHandlerId,
} from "./handlerRegistry";

export type DiscoveryPaletteStage =
  | "save_view"
  | "save_raw"
  | "save_classic"
  | "query_view"
  | "query_raw"
  | "query_classic"
  | "transform"
  | "merge"
  | "join"
  | "validation"
  | "instance_filter"
  | "confidence_filter"
  | "inverted_index";

export type PaletteDragPayload =
  | { kind: "match_definition"; ruleId: string }
  | {
      kind: "discovery";
      stage: DiscoveryPaletteStage;
      /** For `stage: "transform"`, create a discrete transform node for this handler. */
      transformHandlerId?: DiscoveryTransformHandlerId;
    }
  | {
      kind: "structural";
      nodeKind:
        | "source_view"
        | "subgraph"
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
  const queryButtons = (
    [
      ["query_view", "flow.discoveryViewQuery", "flow.paletteTooltip.queryView"],
      ["query_raw", "flow.discoveryRawQuery", "flow.paletteTooltip.queryRaw"],
      ["query_classic", "flow.discoveryClassicQuery", "flow.paletteTooltip.queryClassic"],
    ] as const
  ).map(([stage, labelKey, tooltipKey]) => (
    <button
      key={stage}
      type="button"
      className="kea-btn kea-btn--sm"
      draggable
      title={t(tooltipKey)}
      onDragStart={(e) => setPaletteDragData(e, { kind: "discovery", stage })}
    >
      {t(labelKey)}
    </button>
  ));
  const transformButtons = TRANSFORM_HANDLER_IDS.map((handlerId) => (
    <button
      key={`transform-${handlerId}`}
      type="button"
      className="kea-btn kea-btn--sm"
      draggable
      title={t("flow.paletteTooltip.transform", { handler: handlerId })}
      onDragStart={(e) =>
        setPaletteDragData(e, {
          kind: "discovery",
          stage: "transform",
          transformHandlerId: handlerId,
        })
      }
    >
      {`Transform · ${handlerId}`}
    </button>
  ));
  const validateButtons = (
    [
      ["validation", "flow.discoveryValidate", "flow.paletteTooltip.validate"],
      ["confidence_filter", "flow.discoveryConfidenceFilter", "flow.paletteTooltip.confidenceFilter"],
      ["instance_filter", "flow.discoveryInstanceFilter", "flow.paletteTooltip.instanceFilter"],
    ] as const
  ).map(([stage, labelKey, tooltipKey]) => (
    <button
      key={stage}
      type="button"
      className="kea-btn kea-btn--sm"
      draggable
      title={t(tooltipKey)}
      onDragStart={(e) => setPaletteDragData(e, { kind: "discovery", stage })}
    >
      {t(labelKey)}
    </button>
  ));
  const saveButtons = (
    [
      ["save_view", "flow.discoveryViewSave", "flow.paletteTooltip.saveView"],
      ["save_raw", "flow.discoveryRawSave", "flow.paletteTooltip.saveRaw"],
      ["save_classic", "flow.discoveryClassicSave", "flow.paletteTooltip.saveClassic"],
      ["inverted_index", "flow.discoveryInvertedIndex", "flow.paletteTooltip.invertedIndex"],
    ] as const
  ).map(([stage, labelKey, tooltipKey]) => (
    <button
      key={stage}
      type="button"
      className="kea-btn kea-btn--sm"
      draggable
      title={t(tooltipKey)}
      onDragStart={(e) => setPaletteDragData(e, { kind: "discovery", stage })}
    >
      {t(labelKey)}
    </button>
  ));
  return (
    <div className="kea-flow-palette" role="complementary" aria-label={t("flow.paletteAria")}>
      <p className="kea-flow-palette__heading">{t("flow.paletteDiscoveryPipeline")}</p>
      <div className="kea-flow-palette__buttons" style={{ flexWrap: "wrap", gap: "0.35rem" }}>
        <p className="kea-hint" style={{ width: "100%", margin: "0 0 0.1rem" }}>Query</p>
        {queryButtons}
        <p className="kea-hint" style={{ width: "100%", margin: "0.35rem 0 0.1rem" }}>Transform</p>
        {transformButtons}
        <p className="kea-hint" style={{ width: "100%", margin: "0.35rem 0 0.1rem" }}>Merge / Join</p>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          title={t("flow.paletteTooltip.merge")}
          onDragStart={(e) => setPaletteDragData(e, { kind: "discovery", stage: "merge" })}
        >
          {t("flow.discoveryMerge")}
        </button>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          title={t("flow.paletteTooltip.join")}
          onDragStart={(e) => setPaletteDragData(e, { kind: "discovery", stage: "join" })}
        >
          {t("flow.discoveryJoin")}
        </button>
        <p className="kea-hint" style={{ width: "100%", margin: "0.35rem 0 0.1rem" }}>Validate / filter</p>
        {validateButtons}
        <p className="kea-hint" style={{ width: "100%", margin: "0.35rem 0 0.1rem" }}>Save</p>
        {saveButtons}
      </div>
      <p className="kea-flow-palette__heading">{t("flow.paletteStructural")}</p>
      <div className="kea-flow-palette__buttons" style={{ flexWrap: "wrap", gap: "0.35rem" }}>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          title={t("flow.paletteTooltip.subgraph")}
          onDragStart={(e) => setPaletteDragData(e, { kind: "structural", nodeKind: "subgraph" })}
        >
          {t("flow.structuralSubgraph")}
        </button>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          draggable
          title={t("flow.paletteTooltip.validationRuleLayoutAliasing")}
          onDragStart={(e) =>
            setPaletteDragData(e, { kind: "structural", nodeKind: "match_validation_aliasing" })
          }
        >
          {t("flow.validationRuleLayoutAliasing")}
        </button>
      </div>
      <p className="kea-flow-palette__heading">{t("flow.paletteValidationRuleDefinitions")}</p>
      {matchDefIds.length === 0 ? (
        <p className="kea-hint" style={{ fontSize: "0.8rem", margin: "0 0 0.75rem" }}>
          {t("flow.paletteValidationRuleDefinitionsEmpty")}
        </p>
      ) : (
        <ul className="kea-flow-palette__list">
          {matchDefIds.map((id) => (
            <li key={id}>
              <button
                type="button"
                className="kea-flow-palette__item"
                draggable
                title={t("flow.paletteTooltip.matchDefinition", { id })}
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
    </div>
  );
}
