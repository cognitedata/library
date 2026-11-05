import { type Theme, lightTheme } from "reagraph";
import {
  DEFAULT_NODE_COLOR,
  NODE_TYPE_PALETTE,
  getIconForType,
  type GraphEdge,
  type GraphNode,
  type GraphThemeConfig,
  type GraphVisualConfig,
} from "./types";

// =============================================================================
// Visual Configuration Defaults
// =============================================================================

export const DEFAULT_VISUAL_CONFIG: GraphVisualConfig = {
  pathHighlightColor: "#22c55e",
  pathHighlightSize: 3,
  iconSize: 64,
  defaultNodeColor: DEFAULT_NODE_COLOR,
  nodeTypePalette: NODE_TYPE_PALETTE,
};

// =============================================================================
// Theme Configuration Defaults
// =============================================================================

export const DEFAULT_THEME_CONFIG: GraphThemeConfig = {
  canvas: {
    background: "#FAFBFC",
  },
  node: {
    opacity: 1,
    selectedOpacity: 1,
    inactiveOpacity: 0.3,
    label: {
      color: "#1E293B",
      stroke: "#FFFFFF",
      strokeWidth: 3,
      activeColor: "#0F172A",
      fontSize: 12,
    },
  },
  edge: {
    fill: "#94A3B8",
    activeFill: "#3B82F6",
    opacity: 0.7,
    selectedOpacity: 1,
    inactiveOpacity: 0.2,
    label: {
      color: "#64748B",
      stroke: "#FFFFFF",
      strokeWidth: 2,
      activeColor: "#3B82F6",
      fontSize: 10,
    },
  },
  ring: {
    fill: "#3B82F6",
    activeFill: "#2563EB",
  },
  arrow: {
    fill: "#94A3B8",
    activeFill: "#3B82F6",
  },
  cluster: {
    stroke: "#E2E8F0",
    fill: "#F8FAFC",
    label: {
      color: "#475569",
    },
  },
  lasso: {
    border: "#3B82F6",
    background: "rgba(59, 130, 246, 0.1)",
  },
};

// =============================================================================
// Icon Paths
// =============================================================================

export const ICON_PATHS: Record<string, string> = {
  Plug: "M6 8h12v8H6z M9 8V5 M12 8V5 M15 8V5 M9 16v3 M12 16v3 M15 16v3",
  Minus: "M3 12h18 M3 10v4 M21 10v4",
  Cable:
    "M4 6c3 0 5 3 8 6c3-3 5-6 8-6 M4 12h4c2 0 3 1 4 2c1-1 2-2 4-2h4 M4 18c3 0 5-3 8-6c3 3 5 6 8 6",
  CircleDot: "M12 6a6 6 0 0 1 6 6v6H6v-6a6 6 0 0 1 6-6z M12 10v4 M10 12h4",
  Zap: "M12 3L6 12h5v6l6-9h-5V3z",
  Cpu: "M7 7h10v10H7z M10 7V4 M14 7V4 M10 17v3 M14 17v3 M7 10H4 M7 14H4 M17 10h3 M17 14h3",
  ArrowDownToLine: "M12 3v10 M7 13h10 M9 16h6 M11 19h2",
  Type: "M6 6h12 M12 6v12 M8 18h8",
  LayoutGrid: "M4 4h6v6H4z M14 4h6v6h-6z M4 14h6v6H4z M14 14h6v6h-6z",
  GitCommit: "M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8z M3 12h5 M16 12h5",
  FileText:
    "M6 2h8l4 4v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2z M14 2v4h4 M8 12h8 M8 16h8",
  Activity: "M3 12h4l2-6 3 12 2-6h7",
  Box: "M3 8l9-5 9 5v8l-9 5-9-5V8z M12 8v14 M3 8l9 5 9-5",
  Wrench: "M14 4l-4 4 6 6 4-4a5 5 0 0 0-6-6z M10 8L4 14l4 4 6-6",
  MapPin:
    "M12 2a8 8 0 0 0-8 8c0 6 8 12 8 12s8-6 8-12a8 8 0 0 0-8-8z M12 7a3 3 0 1 0 0 6 3 3 0 0 0 0-6z",
  Building:
    "M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16 M9 7h2 M13 7h2 M9 11h2 M13 11h2 M9 15h6",
  AlertTriangle: "M12 3L2 20h20L12 3z M12 9v5 M12 16v2",
  Circle: "M12 4a8 8 0 1 0 0 16 8 8 0 0 0 0-16z",
  Package: "M3 8l9-5 9 5-9 5-9-5z M3 8v8l9 5V13 M21 8v8l-9 5V13",
  ClipboardList: "M8 4h8v2H8V4z M6 6h12v14H6V6z M9 10h6 M9 14h6",
  Cog: "M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z M12 4v2 M12 18v2 M5 12H3 M21 12h-2 M6.3 6.3l1.4 1.4 M16.3 16.3l1.4 1.4 M6.3 17.7l1.4-1.4 M16.3 7.7l1.4-1.4",
  default: "M12 4a8 8 0 1 0 0 16 8 8 0 0 0 0-16z",
};

// =============================================================================
// Helper Functions
// =============================================================================

export function buildReagraphTheme(config: Partial<GraphThemeConfig> = {}): Theme {
  const theme = { ...DEFAULT_THEME_CONFIG, ...config };

  return {
    ...lightTheme,
    canvas: {
      ...lightTheme.canvas,
      background: theme.canvas.background,
    },
    node: {
      ...lightTheme.node,
      opacity: theme.node.opacity,
      selectedOpacity: theme.node.selectedOpacity,
      inactiveOpacity: theme.node.inactiveOpacity,
      label: {
        ...lightTheme.node.label,
        color: theme.node.label.color,
        stroke: theme.node.label.stroke,
        activeColor: theme.node.label.activeColor,
      },
    },
    edge: {
      ...lightTheme.edge,
      fill: theme.edge.fill,
      activeFill: theme.edge.activeFill,
      opacity: theme.edge.opacity,
      selectedOpacity: theme.edge.selectedOpacity,
      inactiveOpacity: theme.edge.inactiveOpacity,
      label: {
        ...lightTheme.edge.label,
        color: theme.edge.label.color,
        stroke: theme.edge.label.stroke,
        activeColor: theme.edge.label.activeColor,
      },
    },
    ring: {
      ...lightTheme.ring,
      fill: theme.ring.fill,
      activeFill: theme.ring.activeFill,
    },
    arrow: {
      ...lightTheme.arrow,
      fill: theme.arrow.fill,
      activeFill: theme.arrow.activeFill,
    },
    cluster: {
      ...lightTheme.cluster,
      stroke: theme.cluster.stroke,
      fill: theme.cluster.fill,
      label: {
        ...lightTheme.cluster?.label,
        color: theme.cluster.label.color,
      },
    },
    lasso: {
      ...lightTheme.lasso,
      border: theme.lasso.border,
      background: theme.lasso.background,
    },
  };
}

export function mergeVisualConfig(config?: Partial<GraphVisualConfig>): GraphVisualConfig {
  return { ...DEFAULT_VISUAL_CONFIG, ...config };
}

// =============================================================================
// Icon Generation
// =============================================================================

const iconUrlCache = new Map<string, string>();

export function getIconUrl(iconName: string, bgColor: string, iconSize = 64): string {
  const cacheKey = `${iconName}:${bgColor}:${iconSize}`;
  if (iconUrlCache.has(cacheKey)) {
    return iconUrlCache.get(cacheKey)!;
  }

  const pathData = ICON_PATHS[iconName] || ICON_PATHS.default;

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${iconSize}" height="${iconSize}" viewBox="0 0 ${iconSize} ${iconSize}">
    <circle cx="${iconSize / 2}" cy="${iconSize / 2}" r="${iconSize / 2 - 2}" fill="${bgColor}"/>
    <circle cx="${iconSize / 2}" cy="${iconSize / 2}" r="${iconSize / 2 - 4}" fill="none" stroke="rgba(255,255,255,0.25)" stroke-width="2"/>
    <g transform="translate(${iconSize * 0.1875}, ${iconSize * 0.1875}) scale(${iconSize * 0.026})" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="none">
      <path d="${pathData}"/>
    </g>
  </svg>`;

  const dataUrl = `data:image/svg+xml;base64,${btoa(svg)}`;
  iconUrlCache.set(cacheKey, dataUrl);
  return dataUrl;
}

// =============================================================================
// Node/Edge Transformations (Reagraph format)
// =============================================================================

export function transformNodes(
  nodes: GraphNode[],
  visualConfig: GraphVisualConfig
): Array<{
  id: string;
  label: string;
  fill: string;
  icon: string;
  data: GraphNode;
}> {
  return nodes.map((node) => {
    const typeExternalId = node.data?.type?.externalId;
    const iconName = getIconForType(typeExternalId);
    const fillColor = node.fill || visualConfig.defaultNodeColor;
    const iconUrl = getIconUrl(iconName, fillColor, visualConfig.iconSize);

    return {
      id: node.id,
      label: node.label,
      fill: fillColor,
      icon: iconUrl,
      data: node,
    };
  });
}

export function transformEdges(
  connections: GraphEdge[],
  highlightedConnectionIds?: Set<string>,
  visualConfig?: Partial<GraphVisualConfig>
): Array<{
  id: string;
  source: string;
  target: string;
  label?: string;
  fill?: string;
  size?: number;
  data: GraphEdge;
}> {
  const config = visualConfig || {
    pathHighlightColor: "#22c55e",
    pathHighlightSize: 3,
  };

  return connections.map((edge) => {
    const isHighlighted = highlightedConnectionIds?.has(edge.id) ?? false;
    return {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      data: edge,
      ...(isHighlighted && {
        fill: config.pathHighlightColor,
        size: config.pathHighlightSize,
      }),
    };
  });
}
