import type { MessageKey } from "../../i18n";
import { ALIASING_HANDLER_IDS, EXTRACTION_HANDLER_IDS } from "./handlerRegistry";
import type { PaletteDragPayload } from "./FlowPalette";

export type ConnectEndMenuOption = {
  id: string;
  payload: PaletteDragPayload;
  /** Prefer labelKey when set (i18n). */
  labelKey?: MessageKey;
  /** Literal label (e.g. handler id). */
  labelText?: string;
};

const WRITE_BACK_CONNECT_OPTIONS: ConnectEndMenuOption[] = [
  {
    id: "structural-writeback_raw",
    payload: { kind: "structural", nodeKind: "writeback_raw" },
    labelKey: "flow.structuralWritebackRaw",
  },
  {
    id: "structural-writeback_data_modeling",
    payload: { kind: "structural", nodeKind: "writeback_data_modeling" },
    labelKey: "flow.structuralWritebackDataModeling",
  },
];

function labelForOption(opt: ConnectEndMenuOption, t: (key: MessageKey, vars?: Record<string, string | number>) => string): string {
  if (opt.labelText != null) return opt.labelText;
  if (opt.labelKey) return t(opt.labelKey);
  return opt.id;
}

export function formatConnectEndMenuOptionLabel(
  opt: ConnectEndMenuOption,
  t: (key: MessageKey, vars?: Record<string, string | number>) => string
): string {
  return labelForOption(opt, t);
}

/** Palette targets allowed when dragging from a node's source (output) handle onto empty canvas. */
export function connectEndMenuOptionsForSourceType(
  sourceType: string | undefined,
  sourceHandleId?: string | null
): ConnectEndMenuOption[] {
  if (!sourceType) return [];

  if (sourceType === "keaStart") {
    return [
      {
        id: "structural-source_view",
        payload: { kind: "structural", nodeKind: "source_view" },
        labelKey: "flow.structuralSourceView",
      },
    ];
  }

  if (sourceType === "keaSourceView") {
    return EXTRACTION_HANDLER_IDS.map((id) => ({
      id: `extraction-${id}`,
      payload: { kind: "extraction", handlerId: id, preset: true } as const,
      labelText: id,
    }));
  }

  if (sourceType === "keaExtraction") {
    if (sourceHandleId === "validation") {
      return [
        {
          id: "structural-match_validation_extraction",
          payload: { kind: "structural", nodeKind: "match_validation_extraction" },
          labelKey: "flow.validationRuleLayoutExtraction",
        },
      ];
    }
    return [
      ...WRITE_BACK_CONNECT_OPTIONS,
      ...ALIASING_HANDLER_IDS.map((id) => ({
        id: `aliasing-${id}`,
        payload: { kind: "aliasing", handlerId: id, preset: true } as const,
        labelText: id,
      })),
    ];
  }

  if (sourceType === "keaMatchValidationRuleSourceView") {
    return [
      {
        id: "structural-match_validation_source_view",
        payload: { kind: "structural", nodeKind: "match_validation_source_view" },
        labelKey: "flow.validationRuleLayoutSourceView",
      },
    ];
  }
  if (sourceType === "keaMatchValidationRuleExtraction") {
    return [
      {
        id: "structural-match_validation_extraction",
        payload: { kind: "structural", nodeKind: "match_validation_extraction" },
        labelKey: "flow.validationRuleLayoutExtraction",
      },
    ];
  }
  if (sourceType === "keaMatchValidationRuleAliasing") {
    return [
      {
        id: "structural-match_validation_aliasing",
        payload: { kind: "structural", nodeKind: "match_validation_aliasing" },
        labelKey: "flow.validationRuleLayoutAliasing",
      },
    ];
  }

  if (sourceType === "keaAliasing") {
    if (sourceHandleId === "validation") {
      return [
        {
          id: "structural-match_validation_aliasing",
          payload: { kind: "structural", nodeKind: "match_validation_aliasing" },
          labelKey: "flow.validationRuleLayoutAliasing",
        },
      ];
    }
    return [
      ...WRITE_BACK_CONNECT_OPTIONS,
      ...ALIASING_HANDLER_IDS.map((id) => ({
        id: `aliasing-${id}`,
        payload: { kind: "aliasing", handlerId: id, preset: true } as const,
        labelText: id,
      })),
    ];
  }

  return [];
}
