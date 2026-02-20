import { useEffect, useRef, useState } from "react";
import { select } from "d3-selection";
import type { FieldNode, Link, LoadState, ModelNode, SelectedNode, ViewNode } from "./types";
import { useI18n } from "@/shared/i18n";

type DataCatalogGraphProps = {
  status: LoadState;
  sortedModels: ModelNode[];
  sortedViews: ViewNode[];
  sortedFields: FieldNode[];
  links: Link[];
  onSelectNode: (node: SelectedNode) => void;
};

const columnX = [40, 340, 640];
const rowSpacing = 20;
const minNodeHeight = 14;
const nodeWidth = 180;
const tooltipWidth = 260;

export function DataCatalogGraph({
  status,
  sortedModels,
  sortedViews,
  sortedFields,
  links,
  onSelectNode,
}: DataCatalogGraphProps) {
  const { t } = useI18n();
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    title: string;
    connections: string[];
  } | null>(null);
  const [leftTooltip, setLeftTooltip] = useState<{
    x: number;
    y: number;
    title: string;
    connections: string[];
  } | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = select(svgRef.current);
    svg.selectAll("*").remove();

    if (status !== "success") return;

    const modelKeySet = new Set(sortedModels.map((item) => item.key));
    const viewKeySet = new Set(sortedViews.map((item) => item.key));
    const fieldKeySet = new Set(sortedFields.map((item) => item.key));

    const modelToViews = new Map<string, string[]>();
    const viewToModels = new Map<string, string[]>();
    const viewToFields = new Map<string, string[]>();
    const fieldToViews = new Map<string, string[]>();

    for (const link of links) {
      if (modelKeySet.has(link.from) && viewKeySet.has(link.to)) {
        modelToViews.set(link.from, [...(modelToViews.get(link.from) ?? []), link.to]);
        viewToModels.set(link.to, [...(viewToModels.get(link.to) ?? []), link.from]);
        continue;
      }
      if (viewKeySet.has(link.from) && fieldKeySet.has(link.to)) {
        viewToFields.set(link.from, [...(viewToFields.get(link.from) ?? []), link.to]);
        fieldToViews.set(link.to, [...(fieldToViews.get(link.to) ?? []), link.from]);
      }
    }

    const orderByAverage = (
      items: Array<ModelNode | ViewNode | FieldNode>,
      neighborMap: Map<string, string[]>,
      neighborIndex: Map<string, number>
    ) => {
      return [...items].sort((a, b) => {
        const aNeighbors = neighborMap.get(a.key) ?? [];
        const bNeighbors = neighborMap.get(b.key) ?? [];
        const aAvg =
          aNeighbors.length === 0
            ? Number.POSITIVE_INFINITY
            : aNeighbors.reduce((sum, key) => sum + (neighborIndex.get(key) ?? 0), 0) /
              aNeighbors.length;
        const bAvg =
          bNeighbors.length === 0
            ? Number.POSITIVE_INFINITY
            : bNeighbors.reduce((sum, key) => sum + (neighborIndex.get(key) ?? 0), 0) /
              bNeighbors.length;
        if (aAvg === bAvg) return a.label.localeCompare(b.label);
        return aAvg - bAvg;
      });
    };

    let orderedModels = [...sortedModels];
    let orderedViews = [...sortedViews];
    let orderedFields = [...sortedFields];

    for (let i = 0; i < 2; i += 1) {
      const modelIndex = new Map(orderedModels.map((item, index) => [item.key, index]));
      const viewIndex = new Map(orderedViews.map((item, index) => [item.key, index]));
      const fieldIndex = new Map(orderedFields.map((item, index) => [item.key, index]));

      orderedViews = orderByAverage(
        orderedViews,
        new Map(
          orderedViews.map((view) => [
            view.key,
            [
              ...(viewToModels.get(view.key) ?? []),
              ...(viewToFields.get(view.key) ?? []),
            ],
          ])
        ),
        new Map([...modelIndex, ...fieldIndex])
      );
      const updatedViewIndex = new Map(orderedViews.map((item, index) => [item.key, index]));
      orderedModels = orderByAverage(orderedModels, modelToViews, updatedViewIndex);
      orderedFields = orderByAverage(orderedFields, fieldToViews, updatedViewIndex);
    }

    const columns = [orderedModels, orderedViews, orderedFields];
    const minHeights = columns.map((items) => items.length * minNodeHeight);
    const height = Math.max(240, Math.max(...minHeights) + 40);
    const width = columnX[columnX.length - 1] + nodeWidth + 80;
    svg.attr("width", width).attr("height", height);

    const connectionCounts = new Map<string, number>();
    for (const link of links) {
      connectionCounts.set(link.from, (connectionCounts.get(link.from) ?? 0) + 1);
      connectionCounts.set(link.to, (connectionCounts.get(link.to) ?? 0) + 1);
    }

    const positionMap = new Map<string, { x: number; y: number; height: number; index: number }>();
    const layoutColumn = (items: Array<ModelNode | ViewNode | FieldNode>, colIndex: number) => {
      const totalWeight = items.reduce((sum, item) => sum + (connectionCounts.get(item.key) ?? 0), 0);
      const extraHeight = Math.max(0, height - 40 - items.length * minNodeHeight);
      let cursorY = 20;

      for (const [index, item] of items.entries()) {
        const weight = connectionCounts.get(item.key) ?? 0;
        const scaled = totalWeight > 0 ? (weight / totalWeight) * extraHeight : 0;
        const itemHeight = minNodeHeight + scaled;
        positionMap.set(item.key, {
          x: columnX[colIndex],
          y: cursorY + itemHeight / 2,
          height: itemHeight,
          index,
        });
        cursorY += itemHeight;
      }
    };

    layoutColumn(orderedModels, 0);
    layoutColumn(orderedViews, 1);
    layoutColumn(orderedFields, 2);

    const linkGroup = svg.append("g").attr("stroke", "#cbd5f5").attr("stroke-width", 1);
    const linkSelection = linkGroup
      .selectAll("line")
      .data(links)
      .enter()
      .append("line")
      .attr("x1", (d) => positionMap.get(d.from)?.x ?? 0)
      .attr("y1", (d) => positionMap.get(d.from)?.y ?? 0)
      .attr("x2", (d) => positionMap.get(d.to)?.x ?? 0)
      .attr("y2", (d) => positionMap.get(d.to)?.y ?? 0)
      .attr("opacity", 0.5);

    const nodeGroup = svg.append("g").attr("class", "meta-nodes");
    const textGroup = svg.append("g").attr("class", "meta-labels");

    const pastelForIndex = (index: number) => {
      const hue = (index * 47) % 360;
      return `hsl(${hue}, 70%, 85%)`;
    };

    const highlightNode = (key: string) => {
      const connected = new Set<string>([key]);
      for (const link of links) {
        if (link.from === key) connected.add(link.to);
        if (link.to === key) connected.add(link.from);
      }

      linkSelection
        .attr("opacity", (d) => (d.from === key || d.to === key ? 0.9 : 0.1))
        .attr("stroke", (d) => (d.from === key || d.to === key ? "#4f46e5" : "#cbd5f5"));

      nodeGroup
        .selectAll("rect")
        .attr("opacity", (d) =>
          connected.has((d as ModelNode | ViewNode | FieldNode).key) ? 1 : 0.2
        )
        .attr("fill", (d) => {
          const key = (d as ModelNode | ViewNode | FieldNode).key;
          const index = positionMap.get(key)?.index ?? 0;
          return connected.has(key) ? "#94c5f5" : pastelForIndex(index);
        });

      textGroup
        .selectAll("text")
        .attr("opacity", (d) =>
          connected.has((d as ModelNode | ViewNode | FieldNode).key) ? 1 : 0.2
        )
        .style("font-weight", (d) =>
          connected.has((d as ModelNode | ViewNode | FieldNode).key) ? "600" : "400"
        );
    };

    const resetHighlight = () => {
      linkSelection.attr("opacity", 0.5).attr("stroke", "#cbd5f5");
      nodeGroup
        .selectAll("rect")
        .attr("opacity", 1)
        .attr("fill", (d) => {
          const key = (d as ModelNode | ViewNode | FieldNode).key;
          const index = positionMap.get(key)?.index ?? 0;
          return pastelForIndex(index);
        });
      textGroup.selectAll("text").attr("opacity", 1).style("font-weight", "400");
    };

    const labelMap = new Map<string, string>();
    orderedModels.forEach((item) => labelMap.set(item.key, item.label));
    orderedViews.forEach((item) => labelMap.set(item.key, item.label));
    orderedFields.forEach((item) => labelMap.set(item.key, item.label));

    const showTooltip = (key: string) => {
      const pos = positionMap.get(key);
      if (!pos) return;
      const leftKeys: string[] = [];
      const rightKeys: string[] = [];
      for (const link of links) {
        if (link.from === key) {
          rightKeys.push(link.to);
        } else if (link.to === key) {
          leftKeys.push(link.from);
        }
      }

      const leftConnections = leftKeys
        .map((linkKey) => labelMap.get(linkKey) ?? linkKey)
        .sort((a, b) => a.localeCompare(b));
      const rightConnections = rightKeys
        .map((linkKey) => labelMap.get(linkKey) ?? linkKey)
        .sort((a, b) => a.localeCompare(b));

      if (rightConnections.length > 0) {
        setTooltip({
          x: pos.x + nodeWidth + 20,
          y: Math.max(10, pos.y - 40),
          title: labelMap.get(key) ?? key,
          connections: rightConnections,
        });
      } else {
        setTooltip(null);
      }

      if (leftConnections.length > 0) {
        setLeftTooltip({
          x: Math.max(10, pos.x - tooltipWidth - 20),
          y: Math.max(10, pos.y - 40),
          title: labelMap.get(key) ?? key,
          connections: leftConnections,
        });
      } else {
        setLeftTooltip(null);
      }
    };

    const hideTooltip = () => {
      setTooltip(null);
      setLeftTooltip(null);
    };

    const renderColumn = (
      items: Array<ModelNode | ViewNode | FieldNode>,
      colIndex: number,
      column: SelectedNode["column"]
    ) => {
      const group = nodeGroup.append("g");
      const labelGroup = textGroup.append("g");
      group
        .selectAll("rect")
        .data(items)
        .enter()
        .append("rect")
        .attr("x", columnX[colIndex])
        .attr(
          "y",
          (d) =>
            (positionMap.get(d.key)?.y ?? 0) - (positionMap.get(d.key)?.height ?? 0) / 2
        )
        .attr("width", nodeWidth)
        .attr("height", (d) => positionMap.get(d.key)?.height ?? minNodeHeight)
        .attr("rx", 3)
        .attr("fill", (d) => {
          const index = positionMap.get(d.key)?.index ?? 0;
          return pastelForIndex(index);
        })
        .style("cursor", "pointer")
        .on("mouseenter", (_event, d) => {
          highlightNode(d.key);
          showTooltip(d.key);
        })
        .on("mouseleave", () => {
          resetHighlight();
          hideTooltip();
        })
        .on("click", (_event, d) => {
          onSelectNode({ column, node: d });
        });

      labelGroup
        .selectAll("text")
        .data(items)
        .enter()
        .append("text")
        .attr("x", columnX[colIndex] + 10)
        .attr("y", (d) => (positionMap.get(d.key)?.y ?? 0) + 4)
        .style("font-size", "11px")
        .style("fill", "#1f2937")
        .style("cursor", "pointer")
        .on("mouseenter", (_event, d) => {
          highlightNode(d.key);
          showTooltip(d.key);
        })
        .on("mouseleave", () => {
          resetHighlight();
          hideTooltip();
        })
        .on("click", (_event, d) => {
          onSelectNode({ column, node: d });
        })
        .text((d) => d.label);
    };

    renderColumn(orderedModels, 0, "dataModels");
    renderColumn(orderedViews, 1, "views");
    renderColumn(orderedFields, 2, "fields");
  }, [sortedFields, sortedModels, sortedViews, links, status, onSelectNode, t]);

  return (
    <div className="relative overflow-auto">
      <svg ref={svgRef} />
      {tooltip ? (
        <div
          className="pointer-events-none absolute rounded-md border border-slate-200 bg-white p-3 text-xs text-slate-700 shadow-lg"
          style={{
            left: tooltip.x,
            top: tooltip.y,
            width: tooltipWidth,
          }}
        >
          <div className="mb-1 font-semibold text-slate-800">{tooltip.title}</div>
          {tooltip.connections.length === 0 ? (
            <div className="text-slate-500">{t("dataCatalog.tooltip.empty")}</div>
          ) : (
            <div className="max-h-48 overflow-auto">
              {tooltip.connections.map((item, index) => (
                <div key={`${item}-${index}`} className="truncate">
                  {item}
                </div>
              ))}
            </div>
          )}
        </div>
      ) : null}
      {leftTooltip ? (
        <div
          className="pointer-events-none absolute rounded-md border border-slate-200 bg-white p-3 text-xs text-slate-700 shadow-lg"
          style={{
            left: leftTooltip.x,
            top: leftTooltip.y,
            width: tooltipWidth,
          }}
        >
          <div className="mb-1 font-semibold text-slate-800">{leftTooltip.title}</div>
          {leftTooltip.connections.length === 0 ? (
            <div className="text-slate-500">{t("dataCatalog.tooltip.empty")}</div>
          ) : (
            <div className="max-h-48 overflow-auto">
              {leftTooltip.connections.map((item, index) => (
                <div key={`${item}-${index}`} className="truncate">
                  {item}
                </div>
              ))}
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
