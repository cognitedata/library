import { useEffect } from "react";
import { ConnectionLineType, useReactFlow, useUpdateNodeInternals } from "@xyflow/react";
import type {
  TransformCanvasEdgePathStyle,
  TransformCanvasHandleOrientation,
} from "../../types/transformCanvas";
import { normalizeTransformCanvasEdgePathStyle } from "../../types/transformCanvas";

function refreshNodeInternals(
  getNodes: () => ReturnType<ReturnType<typeof useReactFlow>["getNodes"]>,
  updateNodeInternals: (id: string) => void
) {
  for (const n of getNodes()) {
    updateNodeInternals(n.id);
  }
}

/** Recalculate edge paths after handle positions or edge path style change. */
export function FlowHandleOrientationEdgeSync({
  orientation,
  edgePathStyle,
}: {
  orientation: TransformCanvasHandleOrientation;
  edgePathStyle: TransformCanvasEdgePathStyle;
}) {
  const { getNodes, setEdges } = useReactFlow();
  const updateNodeInternals = useUpdateNodeInternals();
  const rfEdgeType = normalizeTransformCanvasEdgePathStyle(edgePathStyle);

  useEffect(() => {
    setEdges((eds) => {
      let changed = false;
      const next = eds.map((e) => {
        if ((e.type ?? rfEdgeType) === rfEdgeType) return e;
        changed = true;
        const { pathOptions: _pathOptions, ...rest } = e;
        return { ...rest, type: rfEdgeType };
      });
      return changed ? next : eds;
    });

    let raf2 = 0;
    const raf1 = requestAnimationFrame(() => {
      raf2 = requestAnimationFrame(() => {
        refreshNodeInternals(getNodes, updateNodeInternals);
      });
    });
    return () => {
      cancelAnimationFrame(raf1);
      if (raf2) cancelAnimationFrame(raf2);
    };
  }, [orientation, rfEdgeType, getNodes, updateNodeInternals, setEdges]);

  return null;
}

export function defaultTransformFlowEdgeOptions(edgePathStyle: TransformCanvasEdgePathStyle) {
  return {
    animated: false as const,
    type: normalizeTransformCanvasEdgePathStyle(edgePathStyle),
  };
}

export function connectionLineTypeForEdgePathStyle(
  edgePathStyle: TransformCanvasEdgePathStyle
): ConnectionLineType {
  switch (normalizeTransformCanvasEdgePathStyle(edgePathStyle)) {
    case "straight":
      return ConnectionLineType.Straight;
    case "step":
      return ConnectionLineType.Step;
    case "smoothstep":
      return ConnectionLineType.SmoothStep;
    case "simplebezier":
      return ConnectionLineType.SimpleBezier;
    case "default":
    default:
      return ConnectionLineType.Bezier;
  }
}
