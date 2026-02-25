import { useMemo } from "react";
import {
  buildGraphForViewIds,
  getSignificantOverviewViewIds,
  COGNITE_CORE_NODE_ID,
  CDF_CORE_TYPES,
  computeOrganicLayout,
  LAYOUT,
} from "@/lib/diagramData";
import type { DocModel } from "@/types/dataModel";

const COLORS = ["#059669", "#b45309", "#2563eb", "#7c3aed", "#db2777", "#0891b2"];
/** Muted fill for CDF core types (CogniteAsset, etc.) to distinguish from extended types */
const CDF_CORE_COLORS_LIGHT = ["#0d7660", "#92400e", "#1d4ed8", "#5b21b6", "#9d174d", "#0e7490"];
const INHERITANCE_COLOR = "#94a3b8";
const RELATION_COLOR = "#f59e0b";


interface SvgDiagramProps {
  doc: DocModel;
  isDark: boolean;
  className?: string;
  /** When set, show only this subset (topic diagram). When unset, Overview shows significant CDF core types only. */
  viewIds?: string[];
  /** View IDs to pull toward diagram center (e.g. core type and its extenders). */
  centerViewIds?: string[];
  /** When a diagram box (view type) is clicked, call with that view id to e.g. open the view pop-out. */
  onViewClick?: (viewId: string) => void;
  /** Available estate (e.g. container width/height when data model was chosen). Layout and viewBox use this so diagram fits. */
  layoutWidth?: number;
  layoutHeight?: number;
}

function truncate(s: string, max: number): string {
  if (s.length <= max) return s;
  return s.slice(0, max - 2) + "…";
}
/** Max chars that fit in MAX_BOX_WIDTH with tight padding; only truncate when necessary */
const MAX_LABEL_CHARS = 38;

/** Exit point from box [x,y,w,h] when going from center (cx,cy) toward (tx,ty). */
function boxEdgeIntersection(
  cx: number,
  cy: number,
  tx: number,
  ty: number,
  x: number,
  y: number,
  w: number,
  h: number
): { px: number; py: number } {
  const dx = tx - cx;
  const dy = ty - cy;
  let bestT = Infinity;
  if (dx > 1e-6) {
    const t = (x + w - cx) / dx;
    if (t > 0.01 && t < bestT) bestT = t;
  } else if (dx < -1e-6) {
    const t = (x - cx) / dx;
    if (t > 0.01 && t < bestT) bestT = t;
  }
  if (dy > 1e-6) {
    const t = (y + h - cy) / dy;
    if (t > 0.01 && t < bestT) bestT = t;
  } else if (dy < -1e-6) {
    const t = (y - cy) / dy;
    if (t > 0.01 && t < bestT) bestT = t;
  }
  if (bestT === Infinity) bestT = 1;
  return {
    px: cx + bestT * dx,
    py: cy + bestT * dy,
  };
}

const EDGE_OFFSET_PX = 10;

/**
 * Two-curve (cubic Bézier) edge path: smooth S-curve using horizontal spread.
 * Control points at (midX, start.py) and (midX, end.py). Optional perpendicular offset separates overlapping edges.
 * labelT: position along curve (0..1) for the label; default 0.5.
 */
function twoCurvePath(
  start: { px: number; py: number },
  end: { px: number; py: number },
  offsetPx: number = 0,
  labelT: number = 0.5
): { pathD: string; labelX: number; labelY: number } {
  const midX = (start.px + end.px) / 2;
  let c1x = midX;
  let c1y = start.py;
  let c2x = midX;
  let c2y = end.py;
  if (offsetPx !== 0) {
    const dx = end.px - start.px;
    const dy = end.py - start.py;
    const len = Math.sqrt(dx * dx + dy * dy) || 1;
    const perpX = (-dy / len) * offsetPx;
    const perpY = (dx / len) * offsetPx;
    c1x += perpX;
    c1y += perpY;
    c2x += perpX;
    c2y += perpY;
  }
  const pathD = `M ${start.px} ${start.py} C ${c1x} ${c1y} ${c2x} ${c2y} ${end.px} ${end.py}`;
  const t = Math.max(0.05, Math.min(0.95, labelT));
  const u = 1 - t;
  const labelX = u * u * u * start.px + 3 * u * u * t * c1x + 3 * u * t * t * c2x + t * t * t * end.px;
  const labelY = u * u * u * start.py + 3 * u * u * t * c1y + 3 * u * t * t * c2y + t * t * t * end.py;
  return { pathD, labelX, labelY };
}

export function SvgDiagram({ doc, isDark, className = "", viewIds, centerViewIds, onViewClick, layoutWidth, layoutHeight }: SvgDiagramProps) {
  const { nodes, edges, effectiveCenterViewIds } = useMemo(() => {
    if (viewIds?.length) {
      const { nodes: n, edges: e } = buildGraphForViewIds(doc, viewIds);
      return { nodes: n, edges: e, effectiveCenterViewIds: centerViewIds ?? [] };
    }
    const { viewIds: sigViewIds, centerViewIds: sigCenter } = getSignificantOverviewViewIds(doc);
    if (sigViewIds.length === 0) {
      const { nodes: n, edges: e } = buildGraphForViewIds(doc, Object.keys(doc.views));
      return { nodes: n, edges: e, effectiveCenterViewIds: [] };
    }
    const { nodes: n, edges: e } = buildGraphForViewIds(doc, sigViewIds);
    return { nodes: n, edges: e, effectiveCenterViewIds: sigCenter };
  }, [doc, viewIds, centerViewIds]);

  const centerNodeIds = useMemo(() => {
    if (effectiveCenterViewIds.length === 0) return undefined;
    const nodeIdSet = new Set(nodes.map((n) => n.id));
    const set = new Set<string>();
    for (const id of effectiveCenterViewIds) {
      if (nodeIdSet.has(id)) set.add(id);
    }
    return set.size > 0 ? set : undefined;
  }, [nodes, effectiveCenterViewIds]);

  const width = layoutWidth ?? 800;
  const height = layoutHeight ?? 500;

  const positions = useMemo(
    () => computeOrganicLayout(nodes, edges, centerNodeIds, width, height),
    [nodes, edges, centerNodeIds, width, height]
  );

  const { edgeOffsets, labelTs } = useMemo(() => {
    const key = (a: string, b: string) => [a, b].sort().join("|");
    const byPair = new Map<string, { i: number; kind: "inheritance" | "relation" }[]>();
    edges.forEach((e, i) => {
      const k = key(e.from, e.to);
      if (!byPair.has(k)) byPair.set(k, []);
      byPair.get(k)!.push({ i, kind: e.kind });
    });
    const offsets = new Array<number>(edges.length).fill(0);
    const ts = new Array<number>(edges.length).fill(0.5);
    byPair.forEach((list) => {
      if (list.length < 2) return;
      const sorted = [...list].sort((a, b) => (a.kind === "inheritance" && b.kind !== "inheritance" ? -1 : a.kind !== "inheritance" && b.kind === "inheritance" ? 1 : 0));
      const n = sorted.length;
      const step = n > 1 ? (2 * EDGE_OFFSET_PX) / (n - 1) : 0;
      const tMin = 0.25;
      const tMax = 0.75;
      const tStep = n > 1 ? (tMax - tMin) / (n - 1) : 0;
      sorted.forEach((item, j) => {
        offsets[item.i] = n === 1 ? 0 : -EDGE_OFFSET_PX + j * step;
        ts[item.i] = n === 1 ? 0.5 : tMin + j * tStep;
      });
    });
    return { edgeOffsets: offsets, labelTs: ts };
  }, [edges]);

  const stroke = isDark ? "#475569" : "#cbd5e1";

  return (
    <div className={`flex flex-col flex-1 min-h-0 rounded-xl border ${isDark ? "border-slate-600 bg-slate-800/50" : "border-slate-200 bg-slate-50"} p-2 ${className}`}>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="xMidYMid meet"
        className="w-full h-full min-h-0"
        style={{ display: "block" }}
      >
        <defs>
          <marker
            id="arrow-inherit"
            markerWidth="10"
            markerHeight="10"
            refX="9"
            refY="3"
            orient="auto"
            markerUnits="strokeWidth"
          >
            <path d="M0,0 L0,6 L9,3 z" fill={INHERITANCE_COLOR} />
          </marker>
          <marker
            id="arrow-relation"
            markerWidth="10"
            markerHeight="10"
            refX="9"
            refY="3"
            orient="auto"
            markerUnits="strokeWidth"
          >
            <path d="M0,0 L0,6 L9,3 z" fill={RELATION_COLOR} />
          </marker>
        </defs>

        {/* Edges: multi-segment paths (horizontal – vertical – horizontal) with rounded corners to use width */}
        {edges.map((e, i) => {
          const from = positions.get(e.from);
          const to = positions.get(e.to);
          const fromNode = nodes.find((n) => n.id === e.from);
          const toNode = nodes.find((n) => n.id === e.to);
          if (!from || !to) return null;
          const fromW = fromNode?.width ?? LAYOUT.BOX_WIDTH;
          const toW = toNode?.width ?? LAYOUT.BOX_WIDTH;
          const fromCx = from.x + fromW / 2;
          const fromCy = from.y + LAYOUT.BOX_HEIGHT / 2;
          const toCx = to.x + toW / 2;
          const toCy = to.y + LAYOUT.BOX_HEIGHT / 2;
          const start = boxEdgeIntersection(
            fromCx,
            fromCy,
            toCx,
            toCy,
            from.x,
            from.y,
            fromW,
            LAYOUT.BOX_HEIGHT
          );
          const end = boxEdgeIntersection(
            toCx,
            toCy,
            fromCx,
            fromCy,
            to.x,
            to.y,
            toW,
            LAYOUT.BOX_HEIGHT
          );
          const offsetPx = edgeOffsets[i] ?? 0;
          const labelT = labelTs[i] ?? 0.5;
          const { pathD, labelX, labelY } = twoCurvePath(start, end, offsetPx, labelT);
          const labelOffset = 8;
          const isInherit = e.kind === "inheritance";
          return (
            <g key={`${e.from}-${e.to}-${i}`}>
              <path
                d={pathD}
                fill="none"
                stroke={isInherit ? INHERITANCE_COLOR : RELATION_COLOR}
                strokeWidth={isInherit ? 1.5 : 1}
                strokeDasharray={isInherit ? "none" : "4,2"}
                markerEnd={`url(#arrow-${isInherit ? "inherit" : "relation"})`}
              />
              {e.label && (
                <text
                  x={labelX + labelOffset}
                  y={labelY}
                  textAnchor="middle"
                  fontSize="8"
                  fill={isDark ? "#94a3b8" : "#64748b"}
                  fontStyle="normal"
                >
                  {e.label}
                </text>
              )}
            </g>
          );
        })}

        {/* Nodes: variable width from label length, tight horizontal padding; click opens view pop-out when onViewClick provided */}
        {nodes.map((n) => {
          const pos = positions.get(n.id);
          if (!pos) return null;
          const w = n.width ?? LAYOUT.BOX_WIDTH;
          const isCogniteCore = n.id === COGNITE_CORE_NODE_ID;
          const cdfCoreIndex = CDF_CORE_TYPES.indexOf(n.id as (typeof CDF_CORE_TYPES)[number]);
          const isCdfCoreType = cdfCoreIndex >= 0;
          const isClickable = onViewClick && !isCogniteCore && doc.views[n.id];
          const color = isCogniteCore
            ? isDark
              ? "#0f172a"
              : "#1e3a5f"
            : isCdfCoreType
              ? CDF_CORE_COLORS_LIGHT[cdfCoreIndex % CDF_CORE_COLORS_LIGHT.length]
              : COLORS[n.depth % COLORS.length];
          return (
            <g
              key={n.id}
              role={isClickable ? "button" : undefined}
              tabIndex={isClickable ? 0 : undefined}
              className={isClickable ? "cursor-pointer outline-none hover:opacity-90" : undefined}
              style={isClickable ? { cursor: "pointer" } : undefined}
              onClick={isClickable ? () => onViewClick(n.id) : undefined}
              onKeyDown={
                isClickable
                  ? (e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        onViewClick(n.id);
                      }
                    }
                  : undefined
              }
            >
              {isClickable && <title>{`Open ${n.label}`}</title>}
              <rect
                x={pos.x}
                y={pos.y}
                width={w}
                height={LAYOUT.BOX_HEIGHT}
                rx="4"
                fill={color}
                stroke={stroke}
                strokeWidth="1"
              />
              <text
                x={pos.x + w / 2}
                y={pos.y + LAYOUT.BOX_HEIGHT / 2 + 3}
                textAnchor="middle"
                fontSize="10"
                fill="#fff"
                fontStyle="normal"
                pointerEvents="none"
              >
                {n.label.length <= MAX_LABEL_CHARS ? n.label : truncate(n.label, MAX_LABEL_CHARS)}
              </text>
            </g>
          );
        })}
      </svg>
      <p className={`flex-shrink-0 mt-1.5 text-xs ${isDark ? "text-slate-400" : "text-slate-500"}`}>
        {nodes.some((n) => n.id === COGNITE_CORE_NODE_ID)
          ? "Main view types extending from Cognite Core. "
          : ""}
        Solid lines = inheritance (implements). Dashed = relations. {nodes.filter((n) => n.id !== COGNITE_CORE_NODE_ID).length} views shown.
      </p>
    </div>
  );
}
