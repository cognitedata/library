import { useEffect, useRef } from "react";
import { hierarchy, partition, HierarchyNode } from "d3-hierarchy";
import { select } from "d3-selection";
import { arc } from "d3-shape";
import { SunburstData, SunburstProps } from "@/shared/quality-types";

interface PartitionNode extends HierarchyNode<SunburstData> {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
}

export function Sunburst({
  width = 600,
  height = 600,
  title = "Assets Overview",
  data,
}: SunburstProps) {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;

    const svg = select(svgRef.current);
    svg.selectAll("*").remove();

    const radius = Math.min(width, height) / 2;
    const g = svg
      .append("g")
      .attr("transform", `translate(${width / 2},${height / 2})`);

    if (!data) {
      return;
    }

    const root = hierarchy(data)
      .sum((d) => (d.children ? 0 : d.value || 0))
      .sort((a, b) => (b.value || 0) - (a.value || 0));

    const partitionLayout = partition<SunburstData>().size([2 * Math.PI, radius]);

    const partitionRoot = partitionLayout(root) as PartitionNode;

    const arcGenerator = arc<PartitionNode>()
      .startAngle((d) => d.x0)
      .endAngle((d) => d.x1)
      .innerRadius((d) => d.y0)
      .outerRadius((d) => d.y1);

    const burstColor = (burstData: SunburstData) => {
      const coverage = burstData.coverage ?? 0;

      // Color stops: 0 = bright red, 50 = orange, 100 = dark green
      const red = { r: 255, g: 0, b: 0 };
      const orange = { r: 255, g: 140, b: 0 };
      const green = { r: 0, g: 128, b: 0 };

      let color;
      if (coverage <= 50) {
        // Interpolate between red and orange (0-50)
        const t = coverage / 50;
        color = {
          r: Math.round(red.r + (orange.r - red.r) * t),
          g: Math.round(red.g + (orange.g - red.g) * t),
          b: Math.round(red.b + (orange.b - red.b) * t),
        };
      } else {
        // Interpolate between orange and green (50-100)
        const t = (coverage - 50) / 50;
        color = {
          r: Math.round(orange.r + (green.r - orange.r) * t),
          g: Math.round(orange.g + (green.g - orange.g) * t),
          b: Math.round(orange.b + (green.b - orange.b) * t),
        };
      }

      return `rgb(${color.r}, ${color.g}, ${color.b})`;
    };

    const nodes = partitionRoot.descendants().filter((d) => d.depth > 0) as PartitionNode[];

    g.selectAll("path")
      .data(nodes)
      .enter()
      .append("path")
      .attr("d", arcGenerator)
      .style("fill", (d) => burstColor(d.data))
      .style("stroke", "#fff")
      .style("stroke-width", "2px")
      .style("opacity", 0.9)
      .on("mouseover", function (_event, d) {
        select(this).style("opacity", 1).style("stroke-width", "3px");

        // Show tooltip
        const tooltip = svg
          .append("g")
          .attr("class", "tooltip")
          .attr("transform", `translate(${width / 2},${height / 2})`);

        tooltip
          .append("text")
          .attr("text-anchor", "middle")
          .attr("dy", "-10")
          .style("font-size", "14px")
          .style("font-weight", "bold")
          .style("fill", "#1a1a2e")
          .text(d.data.name);

        tooltip
          .append("text")
          .attr("text-anchor", "middle")
          .attr("dy", "10")
          .style("font-size", "12px")
          .style("fill", "#6b7280")
          .text(`Coverage: ${(d.data.coverage ?? 0).toFixed(1)}%`);
      })
      .on("mouseout", function () {
        select(this).style("opacity", 0.9).style("stroke-width", "2px");
        svg.selectAll(".tooltip").remove();
      });

    // Add labels for outer segments
    g.selectAll("text")
      .data(nodes.filter((d) => d.depth === 2 && d.x1 - d.x0 > 0.1))
      .enter()
      .append("text")
      .attr("transform", (d) => {
        const x = ((d.x0 + d.x1) / 2) * (180 / Math.PI);
        const y = -(d.y0 + d.y1) / 2;
        return `rotate(${x - 90}) translate(${y},0) rotate(${x < 180 ? 0 : 180})`;
      })
      .attr("text-anchor", "middle")
      .attr("dy", "0.35em")
      .style("font-size", "11px")
      .style("fill", "#1a1a2e")
      .style("pointer-events", "none")
      .text((d) => d.data.name);
  }, [width, height, data]);

  return (
    <div className="flex flex-col items-center gap-4 p-6">
      <h2 className="text-2xl font-semibold text-gray-900">{title}</h2>
      {data ? (
        <svg ref={svgRef} width={width} height={height} />
      ) : (
        <div className="text-sm text-slate-500">No sunburst data available.</div>
      )}
    </div>
  );
}
