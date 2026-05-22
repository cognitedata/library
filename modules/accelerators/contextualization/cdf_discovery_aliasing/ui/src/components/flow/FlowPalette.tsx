import type { MessageKey } from "../../i18n";
import { PaletteOperatorConfigProvider } from "../../context/PaletteOperatorConfigContext";
import type { DataTreeEntityDragPayload, TreeNode } from "../../types/dataTree";
import { FlowPaletteTree } from "./FlowPaletteTree";

export type DiscoveryPaletteStage =
  | "save_view"
  | "save_raw"
  | "save_classic"
  | "query_view"
  | "query_raw"
  | "query_classic"
  | "query_sql"
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
      transformHandlerId?: import("./handlerRegistry").DiscoveryTransformHandlerId;
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

export type PaletteDropPayload = PaletteDragPayload | DataTreeEntityDragPayload;

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  /** Workflow scope YAML; used to list `validation_rule_definitions` ids. */
  scopeDocument: Record<string, unknown>;
  schemaSpace?: string;
  readOnly?: boolean;
};

const PALETTE_DRAG_MIME = "application/x-discovery-flow-palette";
const DATA_TREE_DRAG_MIME = "application/x-discovery-data-tree-entity";

export function setPaletteDragData(e: React.DragEvent, payload: PaletteDragPayload) {
  e.dataTransfer.setData(PALETTE_DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function setDataTreeEntityDragData(e: React.DragEvent, node: TreeNode) {
  const payload: DataTreeEntityDragPayload = { kind: "data_tree_entity", node };
  e.dataTransfer.setData(DATA_TREE_DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function getPaletteDropPayload(e: React.DragEvent): PaletteDropPayload | null {
  const paletteRaw = e.dataTransfer.getData(PALETTE_DRAG_MIME);
  if (paletteRaw) {
    try {
      return JSON.parse(paletteRaw) as PaletteDragPayload;
    } catch {
      return null;
    }
  }
  const treeRaw = e.dataTransfer.getData(DATA_TREE_DRAG_MIME);
  if (treeRaw) {
    try {
      return JSON.parse(treeRaw) as DataTreeEntityDragPayload;
    } catch {
      return null;
    }
  }
  return null;
}

export function FlowPalette({ t, scopeDocument, readOnly }: Props) {
  return (
    <PaletteOperatorConfigProvider>
      <div className="discovery-flow-palette" role="complementary" aria-label={t("flow.paletteAria")}>
        <FlowPaletteTree t={t} scopeDocument={scopeDocument} readOnly={readOnly} />
      </div>
    </PaletteOperatorConfigProvider>
  );
}
