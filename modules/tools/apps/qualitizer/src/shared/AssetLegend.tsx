import { useEffect, useMemo, useState } from "react";
import type { CogniteClient } from "@cognite/sdk";
import { hierarchy, tree, type HierarchyPointLink, type HierarchyPointNode } from "d3-hierarchy";
import { linkVertical } from "d3-shape";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { formatResourceDisplayLabel } from "@/shared/format-resource-display-label";
import {
  buildViewInheritanceTree,
  type ViewInheritanceNode,
  type ViewInheritanceRef,
  type ViewInheritanceTree,
} from "@/shared/view-inheritance";

type LoadState = "idle" | "loading" | "success" | "error";

export type AssetLegendViewRef = ViewInheritanceRef;

type AssetLegendProps = {
  view: AssetLegendViewRef;
  className?: string;
  title?: string;
  /** When false, only the diagram is rendered (no outer card chrome). */
  framed?: boolean;
};

const NODE_WIDTH = 152;
const NODE_HEIGHT = 48;
const LEVEL_GAP = 56;
const SIBLING_GAP = 24;

function nodeLabel(node: ViewInheritanceNode): string {
  return formatResourceDisplayLabel(node.name, node.externalId);
}

function nodeSubtitle(node: ViewInheritanceNode): string {
  return `${node.space} · ${node.version}`;
}

function nodeStatusBadge(node: ViewInheritanceNode): string | null {
  if (node.cycle) return "cycle";
  if (node.missing) return "not found";
  if (node.truncated) return "truncated";
  return null;
}

function nodeFill(node: ViewInheritanceNode, isRoot: boolean): string {
  if (isRoot) return "#0f172a";
  if (node.space === "cdf_cdm" || node.space === "cdf_idm") return "#eef2ff";
  return "#ffffff";
}

function nodeStroke(node: ViewInheritanceNode, isRoot: boolean): string {
  if (isRoot) return "#0f172a";
  if (node.cycle || node.missing) return "#f59e0b";
  if (node.space === "cdf_cdm" || node.space === "cdf_idm") return "#6366f1";
  return "#cbd5e1";
}

function nodeTextFill(isRoot: boolean): string {
  return isRoot ? "#ffffff" : "#0f172a";
}

function nodeSubtitleFill(isRoot: boolean): string {
  return isRoot ? "#cbd5e1" : "#64748b";
}

function layoutInheritanceTree(root: ViewInheritanceNode) {
  const hierarchyRoot = hierarchy(root, (node) =>
    node.children.length > 0 ? node.children : null
  );

  const layout = tree<ViewInheritanceNode>()
    .nodeSize([NODE_WIDTH + SIBLING_GAP, NODE_HEIGHT + LEVEL_GAP])
    .separation((left, right) => (left.parent === right.parent ? 1 : 1.15));

  layout(hierarchyRoot);

  const nodes = hierarchyRoot.descendants();
  const links = hierarchyRoot.links();

  const xs = nodes.map((node) => node.x);
  const ys = nodes.map((node) => node.y);
  const minX = Math.min(...xs, 0) - NODE_WIDTH / 2 - 16;
  const maxX = Math.max(...xs, 0) + NODE_WIDTH / 2 + 16;
  const minY = Math.min(...ys, 0) - NODE_HEIGHT / 2 - 8;
  const maxY = Math.max(...ys, 0) + NODE_HEIGHT / 2 + 24;

  const offsetX = -minX;
  const offsetY = -minY;

  return {
    nodes,
    links,
    width: maxX - minX,
    height: maxY - minY,
    offsetX,
    offsetY,
  };
}

function ViewInheritanceDiagram({ tree }: { tree: ViewInheritanceTree }) {
  const laidOut = useMemo(() => layoutInheritanceTree(tree.root), [tree.root]);

  const linkPath = useMemo(
    () =>
      linkVertical<HierarchyPointNode<ViewInheritanceNode>, HierarchyPointLink<ViewInheritanceNode>>()
        .x((point) => point.x + laidOut.offsetX)
        .y((point) => point.y + laidOut.offsetY),
    [laidOut.offsetX, laidOut.offsetY]
  );

  return (
    <div className="overflow-x-auto">
      <svg
        width={laidOut.width}
        height={laidOut.height}
        role="img"
        aria-label="View inheritance diagram"
        className="block min-w-full"
      >
        <g>
          {laidOut.links.map((link) => (
            <path
              key={`${link.source.data.id}->${link.target.data.id}`}
              d={linkPath(link) ?? undefined}
              fill="none"
              stroke="#94a3b8"
              strokeWidth={1.5}
              strokeOpacity={0.9}
            />
          ))}
          {laidOut.nodes.map((point) => {
            const node = point.data;
            const isRoot = point.depth === 0;
            const x = point.x + laidOut.offsetX;
            const y = point.y + laidOut.offsetY;
            const badge = nodeStatusBadge(node);

            return (
              <g key={node.id} transform={`translate(${x - NODE_WIDTH / 2},${y - NODE_HEIGHT / 2})`}>
                <rect
                  width={NODE_WIDTH}
                  height={NODE_HEIGHT}
                  rx={10}
                  ry={10}
                  fill={nodeFill(node, isRoot)}
                  stroke={nodeStroke(node, isRoot)}
                  strokeWidth={isRoot ? 2 : 1.5}
                  strokeDasharray={node.cycle || node.missing ? "4 3" : undefined}
                />
                <text
                  x={NODE_WIDTH / 2}
                  y={20}
                  textAnchor="middle"
                  fill={nodeTextFill(isRoot)}
                  fontSize={12}
                  fontWeight={600}
                >
                  {truncate(nodeLabel(node), 22)}
                </text>
                <text
                  x={NODE_WIDTH / 2}
                  y={36}
                  textAnchor="middle"
                  fill={nodeSubtitleFill(isRoot)}
                  fontSize={10}
                >
                  {truncate(nodeSubtitle(node), 26)}
                </text>
                {badge !== null ? (
                  <text
                    x={NODE_WIDTH / 2}
                    y={NODE_HEIGHT - 4}
                    textAnchor="middle"
                    fill="#b45309"
                    fontSize={9}
                  >
                    {badge}
                  </text>
                ) : null}
              </g>
            );
          })}
        </g>
      </svg>
    </div>
  );
}

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 1)}…`;
}

function AssetLegendBody({
  status,
  error,
  tree,
  view,
}: {
  status: LoadState;
  error: string | null;
  tree: ViewInheritanceTree | null;
  view: ViewInheritanceRef;
}) {
  if (status === "loading") {
    return <p className="text-sm text-slate-500">Loading view inheritance…</p>;
  }

  if (status === "error") {
    return (
      <ApiError
        message={error}
        api="POST /models/views/byids"
        requestBody={{
          items: [{ space: view.space, externalId: view.externalId, version: view.version }],
        }}
      />
    );
  }

  if (tree === null) return null;

  if (tree.root.children.length === 0 && !tree.root.missing) {
    return (
      <p className="text-sm text-slate-500">
        <code className="text-xs">{nodeLabel(tree.root)}</code> does not declare any{" "}
        <code className="text-xs">implements</code> references.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <p className="text-xs text-slate-500">
        Top node is the declared view. Arrows point to views listed in its{" "}
        <code className="text-[11px]">implements</code> array; each level repeats for transitive
        inheritance.
      </p>
      <ViewInheritanceDiagram tree={tree} />
      {tree.truncated ? (
        <p className="text-xs text-amber-700">Inheritance tree was truncated due to size limits.</p>
      ) : null}
    </div>
  );
}

export function AssetLegend({ view, className, title, framed = true }: AssetLegendProps) {
  const { sdk } = useAppSdk();
  const [status, setStatus] = useState<LoadState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [tree, setTree] = useState<ViewInheritanceTree | null>(null);

  const viewKey = `${view.space}/${view.externalId}/${view.version}`;
  const heading =
    title ?? `Asset legend · ${formatResourceDisplayLabel(null, view.externalId)}`;

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setStatus("loading");
      setError(null);
      setTree(null);

      try {
        const result = await buildViewInheritanceTree(sdk, view);
        if (!cancelled) {
          setTree(result);
          setStatus("success");
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load inheritance.");
          setStatus("error");
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [sdk, viewKey, view]);

  const body = <AssetLegendBody status={status} error={error} tree={tree} view={view} />;

  if (!framed) {
    return <div className={className}>{body}</div>;
  }

  return (
    <section
      className={`rounded-md border border-slate-200 bg-white p-4 ${className ?? ""}`}
      aria-label={heading}
    >
      <h4 className="mb-3 text-sm font-semibold text-slate-900">{heading}</h4>
      {body}
    </section>
  );
}

export async function loadViewInheritanceTree(
  sdk: CogniteClient,
  view: ViewInheritanceRef
): Promise<ViewInheritanceTree> {
  return buildViewInheritanceTree(sdk, view);
}
