import type { GraphCanvasRef } from "reagraph";

// =============================================================================
// Layout
// =============================================================================

export type LayoutType =
  | "forceDirected2d"
  | "forceDirected3d"
  | "treeTd2d"
  | "treeLr2d"
  | "radialOut2d"
  | "circular2d";

export interface LayoutOption {
  id: LayoutType;
  label: string;
}

export const LAYOUT_OPTIONS: LayoutOption[] = [
  { id: "forceDirected2d", label: "Force 2D" },
  { id: "forceDirected3d", label: "Force 3D" },
  { id: "treeTd2d", label: "Tree (Top-Down)" },
  { id: "treeLr2d", label: "Tree (Left-Right)" },
  { id: "radialOut2d", label: "Radial" },
  { id: "circular2d", label: "Circular" },
];

// =============================================================================
// CDF Data Model Types
// =============================================================================

export interface ViewReference {
  space: string;
  externalId: string;
  version?: string;
}

export interface DataModelInfo {
  space: string;
  externalId: string;
  name?: string;
  description?: string;
  version?: string;
  views: ViewReference[];
}

/**
 * Tuple describing a reverse-relation query to run on node expansion.
 *
 * `[space, viewExternalId, viewVersion, propertyName, isList]`
 *
 * - `space`           - space of the view that defines the relation.
 * - `viewExternalId`  - external id of the view that defines the relation.
 * - `viewVersion`     - version of the view (e.g. `"v1"`, `"1"`). Required so the
 *                       lookup is not pinned to any specific version.
 * - `propertyName`    - direct relation property on the view that points back to
 *                       the node being expanded.
 * - `isList`          - `true` when the relation is `list<direct>`, otherwise `false`.
 */
export type ReverseRelationQuery = [
  space: string,
  viewExternalId: string,
  viewVersion: string,
  propertyName: string,
  isList: boolean,
];

export interface ViewPriorityConfig {
  viewTypePriority?: string[];
  priorityViewNames?: string[];
  skipViewsForType?: string[];
  skipViewsForProperties?: string[];
}

// =============================================================================
// CDF Instance Types
// =============================================================================

export interface CDFNode {
  instanceType: "node";
  space: string;
  externalId: string;
  version?: number;
  createdTime?: number;
  lastUpdatedTime?: number;
  type?: { space: string; externalId: string };
  properties?: Record<string, unknown>;
}

export interface CDFEdge {
  instanceType: "edge";
  space: string;
  externalId: string;
  version?: number;
  createdTime?: number;
  lastUpdatedTime?: number;
  type: { space: string; externalId: string };
  startNode: { space: string; externalId: string };
  endNode: { space: string; externalId: string };
  properties?: Record<string, unknown>;
}

// =============================================================================
// Graph Data Types
// =============================================================================

export interface GraphNode {
  id: string;
  label: string;
  fill?: string;
  data: CDFNode;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  label?: string;
  fill?: string;
  size?: number;
  data: CDFEdge;
}

export interface NodeTypeInfo {
  externalId: string;
  space: string;
  color: string;
  count: number;
}

export interface GraphData {
  nodes: GraphNode[];
  connections: GraphEdge[];
  nodeTypes: NodeTypeInfo[];
}

export interface GraphSelection {
  type: "node" | "edge" | null;
  id: string | null;
  node?: GraphNode;
  edge?: GraphEdge;
}

// =============================================================================
// Visual / Theme Configuration
// =============================================================================

export interface GraphThemeConfig {
  canvas: {
    background: string;
  };
  node: {
    opacity: number;
    selectedOpacity: number;
    inactiveOpacity: number;
    label: {
      color: string;
      stroke: string;
      strokeWidth: number;
      activeColor: string;
      fontSize: number;
    };
  };
  edge: {
    fill: string;
    activeFill: string;
    opacity: number;
    selectedOpacity: number;
    inactiveOpacity: number;
    label: {
      color: string;
      stroke: string;
      strokeWidth: number;
      activeColor: string;
      fontSize: number;
    };
  };
  ring: {
    fill: string;
    activeFill: string;
  };
  arrow: {
    fill: string;
    activeFill: string;
  };
  cluster: {
    stroke: string;
    fill: string;
    label: {
      color: string;
    };
  };
  lasso: {
    border: string;
    background: string;
  };
}

export interface GraphVisualConfig {
  pathHighlightColor: string;
  pathHighlightSize: number;
  iconSize: number;
  defaultNodeColor: string;
  nodeTypePalette: string[];
}

// =============================================================================
// Node Color Palette & Icons
// =============================================================================

export const NODE_TYPE_PALETTE = [
  "#3b82f6",
  "#22c55e",
  "#ef4444",
  "#a855f7",
  "#f59e0b",
  "#06b6d4",
  "#e11d48",
  "#0ea5e9",
  "#8b5cf6",
  "#f97316",
  "#14b8a6",
  "#f43f5e",
];

export const DEFAULT_NODE_COLOR = "#94a3b8";

export const NODE_TYPE_ICONS: Record<string, string> = {
  Connector: "Plug",
  Wire: "Minus",
  Cable: "Cable",
  Cavity: "CircleDot",
  Shunt: "Zap",
  ShuntCollection: "LayoutGrid",
  GroundReference: "ArrowDownToLine",
  HardwareOccurence: "Cpu",
  TextElement: "Type",
  WireExtermity: "GitCommitHorizontal",
  CogniteFile: "FileText",
  CogniteFileCategory: "FolderOpen",
  CogniteTimeSeries: "Activity",
  CogniteDatapoint: "TrendingUp",
  CogniteAsset: "Box",
  CogniteEquipment: "Wrench",
  CogniteEquipmentType: "Settings",
  CogniteAssetClass: "Layers",
  CogniteAssetType: "Tag",
  ISA95Asset: "Factory",
  Enterprise: "Building2",
  Site: "Building",
  Area: "MapPin",
  ProcessCell: "Grid3X3",
  ProcessArea: "LayoutGrid",
  ProductionLine: "ArrowRightLeft",
  ProductionUnit: "Cpu",
  Equipment: "Cog",
  EquipmentModule: "CircuitBoard",
  WorkCell: "Workflow",
  WorkCenter: "Server",
  WorkUnit: "Puzzle",
  WorkOrder: "ClipboardList",
  CogniteActivity: "Calendar",
  MaintenanceOrder: "Hammer",
  FaultCode: "AlertTriangle",
  QualityAlert: "ShieldAlert",
  Product: "Package",
  ProductComponent: "Component",
  ProductNode: "Boxes",
  Batch: "Beaker",
  Cognite3DModel: "Box",
  CogniteCADModel: "Box",
  CogniteCADNode: "Shapes",
  Cognite360Image: "Image",
  CognitePointCloudModel: "Scan",
  CogniteAnnotation: "MessageSquare",
  CogniteDiagramAnnotation: "StickyNote",
  CogniteSourceSystem: "Database",
  default: "Circle",
};

export function getIconForType(typeExternalId: string | undefined): string {
  if (!typeExternalId) return NODE_TYPE_ICONS.default;

  if (NODE_TYPE_ICONS[typeExternalId]) {
    return NODE_TYPE_ICONS[typeExternalId];
  }

  const lower = typeExternalId.toLowerCase();

  if (lower.includes("connector") || lower.includes("plug")) return "Plug";
  if (lower.includes("wire")) return "Minus";
  if (lower.includes("cable")) return "Cable";
  if (lower.includes("cavity")) return "CircleDot";
  if (lower.includes("shunt") && lower.includes("collection")) return "LayoutGrid";
  if (lower.includes("shunt")) return "Zap";
  if (lower.includes("ground")) return "ArrowDownToLine";
  if (lower.includes("hardware")) return "Cpu";
  if (lower.includes("file") || lower.includes("document")) return "FileText";
  if (lower.includes("timeseries") || lower.includes("series")) return "Activity";
  if (lower.includes("asset")) return "Box";
  if (lower.includes("equipment")) return "Wrench";
  if (lower.includes("work") || lower.includes("order") || lower.includes("maintenance"))
    return "ClipboardList";
  if (lower.includes("product")) return "Package";
  if (lower.includes("area") || lower.includes("location")) return "MapPin";
  if (lower.includes("site") || lower.includes("building")) return "Building";
  if (lower.includes("3d") || lower.includes("model") || lower.includes("cad")) return "Box";
  if (lower.includes("image") || lower.includes("photo")) return "Image";
  if (lower.includes("batch")) return "Beaker";
  if (lower.includes("alert") || lower.includes("fault")) return "AlertTriangle";

  return NODE_TYPE_ICONS.default;
}

// =============================================================================
// Instance ID Helpers
// =============================================================================

export function createInstanceId(space: string, externalId: string) {
  return `${space}:${externalId}`;
}

export function parseInstanceId(id: string) {
  const [space, ...rest] = id.split(":");
  return { space, externalId: rest.join(":") };
}

// =============================================================================
// Node Label
// =============================================================================

function findNameInObject(obj: Record<string, unknown> | unknown): string | undefined {
  if (!obj || typeof obj !== "object") return undefined;
  const objRecord = obj as Record<string, unknown>;
  if (typeof objRecord.name === "string" && objRecord.name.trim().length > 0) return objRecord.name;
  for (const value of Object.values(objRecord)) {
    if (value && typeof value === "object") {
      const nested = findNameInObject(value);
      if (nested) return nested;
    }
  }
  return undefined;
}

export function getNodeLabel(node: CDFNode): string {
  if (node.properties && typeof node.properties === "object") {
    for (const viewObj of Object.values(node.properties)) {
      if (viewObj && typeof viewObj === "object") {
        const maybe = findNameInObject(viewObj);
        if (maybe) return maybe;
      }
    }
  }
  if (node.type?.externalId) {
    return node.type.externalId;
  }
  return node.externalId;
}

// =============================================================================
// Lite Hook API Types
// =============================================================================

/**
 * CDF-friendly input configuration for the `useGraphViewer` hook.
 */
export interface UseGraphViewerConfig {
  dataModel: {
    space: string;
    externalId: string;
    version: string;
  };
  instance?: {
    space: string;
    externalId: string;
  };
  options?: UseGraphViewerOptions;
}

export interface UseGraphViewerOptions {
  maxNodes?: number;
  layout?: LayoutType;
  whitelistedRelationProps?: string[];
  coreReverseQueries?: ReverseRelationQuery[];
  viewPriorityConfig?: ViewPriorityConfig;
  initialConnectionLimit?: number;
  visualConfig?: Partial<GraphVisualConfig>;
  themeConfig?: Partial<GraphThemeConfig>;
  features?: Partial<LiteFeatureFlags>;
}

export interface LiteFeatureFlags {
  enableLegend: boolean;
  enableZoomControls: boolean;
  enableNodeExpansion: boolean;
}

export const DEFAULT_LITE_FEATURES: LiteFeatureFlags = {
  enableLegend: true,
  enableZoomControls: true,
  enableNodeExpansion: true,
};

export interface GraphStats {
  totalNodes: number;
  totalConnections: number;
  nodeTypes: Record<string, number>;
  connectionTypes: Record<string, number>;
}

export interface UseGraphViewerReturn {
  GraphCanvas: React.FC<{ className?: string }>;
  isLoading: boolean;
  error: string | null;
  graphData: GraphData;
  stats: GraphStats | null;
  layout: LayoutType;
  setLayout: (layout: LayoutType) => void;
  selections: string[];
  setSelections: (ids: string[]) => void;
  selectedNode: GraphNode | null;
  selectedEdge: GraphEdge | null;
  expandNode: (nodeId: string) => Promise<void>;
  loadInstance: (space: string, externalId: string) => Promise<void>;
  fitView: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
  clear: () => void;
  graphRef: React.RefObject<GraphCanvasRef | null>;
}
