import type { Node } from "@xyflow/react";
import { rfTypeToKind } from "../../types/transformCanvas";
import { defaultEtlNodeSize, maxEtlNodeWidth, parseFlowNodeDimension } from "./etlFlowNodeSizing";

function readNodeWidth(node: Node): number | undefined {
  const style = (node.style ?? {}) as Record<string, unknown>;
  return parseFlowNodeDimension(node.width) ?? parseFlowNodeDimension(style.width);
}

export function autoResizeFlowNodesToContent(nodes: Node[]): Node[] {
  return nodes.map((node) => {
    const kind = rfTypeToKind(node.type);
    const nodeMaxWidthPx = maxEtlNodeWidth(kind);
    const defaultWidth = defaultEtlNodeSize(kind).width;
    const currentWidth = readNodeWidth(node);
    const isAtDefaultWidth = currentWidth != null && Math.abs(currentWidth - defaultWidth) <= 1;
    const labelMaxWidthPx = nodeMaxWidthPx;
    const style = (node.style ?? {}) as Record<string, unknown>;
    const { width: _width, height: _height, ...styleRest } = style;
    styleRest.maxWidth = `${nodeMaxWidthPx}px`;
    if (isAtDefaultWidth) {
      styleRest["--etl-label-max-width"] = `${labelMaxWidthPx}px`;
    } else {
      delete styleRest["--etl-label-max-width"];
    }
    const nextStyle = Object.keys(styleRest).length > 0 ? styleRest : undefined;
    return {
      ...node,
      width: undefined,
      height: undefined,
      measured: undefined,
      style: nextStyle,
    };
  });
}
