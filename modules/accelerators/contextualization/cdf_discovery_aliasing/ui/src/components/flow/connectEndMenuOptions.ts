import type { MessageKey } from "../../i18n";
import type { DiscoveryPaletteStage, PaletteDragPayload } from "./FlowPalette";
import type { CompileWorkflowDagMode } from "../../utils/workflowCompileMode";
import { discoveryPersistenceOutboundToEndOnlyRfTypes } from "./flowConstants";
import { TRANSFORM_HANDLER_IDS } from "./handlerRegistry";

const DISCOVERY_QUERY_STAGES: readonly DiscoveryPaletteStage[] = [
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
];

export type ConnectEndMenuOption = {
  id: string;
  payload: PaletteDragPayload;
  /** Prefer labelKey when set (i18n). */
  labelKey?: MessageKey;
  /** Literal label (e.g. handler id). */
  labelText?: string;
};

export type ConnectEndMenuGroup = {
  id: "query" | "transform" | "validate" | "filter" | "save" | "structural" | "other";
  labelText: string;
  options: ConnectEndMenuOption[];
};

const DISCOVERY_STAGES: readonly DiscoveryPaletteStage[] = [
  "save_view",
  "save_raw",
  "save_classic",
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "transform",
  "merge",
  "join",
  "validation",
  "instance_filter",
  "confidence_filter",
  "inverted_index",
] as const;

const DISCOVERY_LABEL_KEYS: Record<DiscoveryPaletteStage, MessageKey> = {
  save_view: "flow.discoveryViewSave",
  save_raw: "flow.discoveryRawSave",
  save_classic: "flow.discoveryClassicSave",
  query_view: "flow.discoveryViewQuery",
  query_raw: "flow.discoveryRawQuery",
  query_classic: "flow.discoveryClassicQuery",
  query_sql: "flow.discoverySqlQuery",
  transform: "flow.discoveryTransform",
  merge: "flow.discoveryMerge",
  join: "flow.discoveryJoin",
  validation: "flow.discoveryValidate",
  instance_filter: "flow.discoveryInstanceFilter",
  confidence_filter: "flow.discoveryConfidenceFilter",
  inverted_index: "flow.discoveryInvertedIndex",
};

/** Same copy as flow palette `title` tooltips (drag targets). */
const DISCOVERY_STAGE_TOOLTIP_KEYS: Partial<Record<DiscoveryPaletteStage, MessageKey>> = {
  query_view: "flow.paletteTooltip.queryView",
  query_raw: "flow.paletteTooltip.queryRaw",
  query_classic: "flow.paletteTooltip.queryClassic",
  query_sql: "flow.paletteTooltip.querySql",
  join: "flow.paletteTooltip.join",
  validation: "flow.paletteTooltip.validate",
  instance_filter: "flow.paletteTooltip.instanceFilter",
  confidence_filter: "flow.paletteTooltip.confidenceFilter",
  save_view: "flow.paletteTooltip.saveView",
  save_raw: "flow.paletteTooltip.saveRaw",
  save_classic: "flow.paletteTooltip.saveClassic",
  inverted_index: "flow.paletteTooltip.invertedIndex",
};

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

export function formatConnectEndMenuOptionTooltip(
  opt: ConnectEndMenuOption,
  t: (key: MessageKey, vars?: Record<string, string | number>) => string
): string {
  const p = opt.payload;
  if (p.kind === "match_definition") {
    return t("flow.paletteTooltip.matchDefinition", { id: p.ruleId });
  }
  if (p.kind === "structural") {
    if (p.nodeKind === "match_validation_aliasing") {
      return t("flow.paletteTooltip.validationRuleLayoutAliasing");
    }
    if (p.nodeKind === "subgraph") {
      return t("flow.paletteTooltip.subgraph");
    }
    return labelForOption(opt, t);
  }
  if (p.kind === "discovery") {
    if (p.stage === "transform" && p.transformHandlerId != null) {
      return t("flow.paletteTooltip.transform", { handler: p.transformHandlerId });
    }
    const key = DISCOVERY_STAGE_TOOLTIP_KEYS[p.stage];
    if (key) return t(key);
    return labelForOption(opt, t);
  }
  return labelForOption(opt, t);
}

function groupIdForOptionPayload(payload: PaletteDragPayload): ConnectEndMenuGroup["id"] {
  if (payload.kind === "structural") return "structural";
  if (payload.kind !== "discovery") return "other";
  if (
    payload.stage === "query_view" ||
    payload.stage === "query_raw" ||
    payload.stage === "query_classic" ||
    payload.stage === "query_sql"
  ) {
    return "query";
  }
  if (payload.stage === "transform" || payload.stage === "join") return "transform";
  if (payload.stage === "validation") return "validate";
  if (payload.stage === "instance_filter" || payload.stage === "confidence_filter") return "filter";
  if (
    payload.stage === "save_view" ||
    payload.stage === "save_raw" ||
    payload.stage === "save_classic" ||
    payload.stage === "inverted_index"
  ) {
    return "save";
  }
  return "other";
}

function groupLabel(id: ConnectEndMenuGroup["id"]): string {
  switch (id) {
    case "query":
      return "Query";
    case "transform":
      return "Transform";
    case "validate":
      return "Validate";
    case "filter":
      return "Filter";
    case "save":
      return "Save";
    case "structural":
      return "Structural";
    default:
      return "Other";
  }
}

export function connectEndMenuGroupedOptionsForSourceType(
  sourceType: string | undefined,
  sourceHandleId?: string | null,
  compileDagMode: CompileWorkflowDagMode = "canvas"
): ConnectEndMenuGroup[] {
  const flat = connectEndMenuOptionsForSourceType(sourceType, sourceHandleId, compileDagMode);
  const byGroup = new Map<ConnectEndMenuGroup["id"], ConnectEndMenuOption[]>();
  for (const opt of flat) {
    const gid = groupIdForOptionPayload(opt.payload);
    const arr = byGroup.get(gid) ?? [];
    arr.push(opt);
    byGroup.set(gid, arr);
  }
  const order: ConnectEndMenuGroup["id"][] = ["query", "transform", "validate", "filter", "save", "structural", "other"];
  return order
    .map((gid) => {
      const options = byGroup.get(gid) ?? [];
      return options.length > 0 ? { id: gid, labelText: groupLabel(gid), options } : null;
    })
    .filter((g): g is ConnectEndMenuGroup => g != null);
}

function discoveryOptions(prefix: string): ConnectEndMenuOption[] {
  const out: ConnectEndMenuOption[] = [];
  for (const stage of DISCOVERY_STAGES) {
    if (stage === "transform") {
      for (const handlerId of TRANSFORM_HANDLER_IDS) {
        out.push({
          id: `${prefix}-transform-${handlerId}`,
          payload: { kind: "discovery", stage: "transform", transformHandlerId: handlerId },
          labelText: `Transform · ${handlerId}`,
        });
      }
      continue;
    }
    out.push({
      id: `${prefix}-${stage}`,
      payload: { kind: "discovery", stage },
      labelKey: DISCOVERY_LABEL_KEYS[stage],
    });
  }
  return out;
}

const QUERY_STAGE_SET = new Set<string>(DISCOVERY_QUERY_STAGES as readonly string[]);

/** Discovery palette stages except query nodes (queries may only be placed from Start). */
function discoveryOptionsWithoutQueries(prefix: string): ConnectEndMenuOption[] {
  const out: ConnectEndMenuOption[] = [];
  for (const stage of DISCOVERY_STAGES.filter((s) => !QUERY_STAGE_SET.has(s))) {
    if (stage === "transform") {
      for (const handlerId of TRANSFORM_HANDLER_IDS) {
        out.push({
          id: `${prefix}-transform-${handlerId}`,
          payload: { kind: "discovery", stage: "transform", transformHandlerId: handlerId },
          labelText: `Transform · ${handlerId}`,
        });
      }
      continue;
    }
    out.push({
      id: `${prefix}-${stage}`,
      payload: { kind: "discovery", stage },
      labelKey: DISCOVERY_LABEL_KEYS[stage],
    });
  }
  return out;
}

function discoveryQueryOptions(prefix: string): ConnectEndMenuOption[] {
  return DISCOVERY_QUERY_STAGES.map((stage) => ({
    id: `${prefix}-${stage}`,
    payload: { kind: "discovery", stage },
    labelKey: DISCOVERY_LABEL_KEYS[stage],
  }));
}

/** Palette targets allowed when dragging from a node's source (output) handle onto empty canvas. */
export function connectEndMenuOptionsForSourceType(
  sourceType: string | undefined,
  sourceHandleId?: string | null,
  compileDagMode: CompileWorkflowDagMode = "canvas"
): ConnectEndMenuOption[] {
  if (!sourceType) return [];

  if (sourceType === "discoveryStart") {
    if (compileDagMode === "canvas") {
      return discoveryQueryOptions("from-start");
    }
    return discoveryOptions("from-start");
  }

  if (sourceType === "discoverySourceView") {
    return discoveryOptionsWithoutQueries("from-source-view");
  }

  /** Persistence ``out`` may wire only to ``discoveryEnd``; palette has no End drop — offer no connect-end targets. */
  if (sourceType && discoveryPersistenceOutboundToEndOnlyRfTypes.has(sourceType)) {
    return [];
  }

  const discoverySourceTypes = new Set([
    "discoveryViewQuery",
    "discoveryRawQuery",
    "discoveryClassicQuery",
    "discoverySqlQuery",
    "discoveryTransform",
    "discoveryJoin",
    "discoveryValidate",
    "discoveryInstanceFilter",
    "discoveryConfidenceFilter",
  ]);
  if (sourceType && discoverySourceTypes.has(sourceType)) {
    const opts = discoveryOptionsWithoutQueries(
      `from-${sourceType.replace(/^discovery/, "").toLowerCase()}`
    );
    if (
      sourceType === "discoveryValidate" ||
      sourceType === "discoveryInstanceFilter" ||
      sourceType === "discoveryConfidenceFilter"
    ) {
      return opts.filter((o) => o.payload.kind !== "discovery" || o.payload.stage !== "transform");
    }
    return opts;
  }

  if (sourceType === "discoveryMatchValidationRuleSourceView") return [];
  if (sourceType === "discoveryMatchValidationRuleExtraction") return [];
  if (sourceType === "discoveryMatchValidationRuleAliasing") {
    return [
      {
        id: "structural-match_validation_aliasing",
        payload: { kind: "structural", nodeKind: "match_validation_aliasing" },
        labelKey: "flow.validationRuleLayoutAliasing",
      },
    ];
  }

  void sourceHandleId;
  return [];
}
