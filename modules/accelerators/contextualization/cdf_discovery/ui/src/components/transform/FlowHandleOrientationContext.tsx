import { createContext, useContext, type ReactNode } from "react";
import type { TransformCanvasHandleOrientation } from "../../types/transformCanvas";

const FlowHandleOrientationContext = createContext<TransformCanvasHandleOrientation>("lr");

export function FlowHandleOrientationProvider({
  value,
  children,
}: {
  value: TransformCanvasHandleOrientation;
  children: ReactNode;
}) {
  return <FlowHandleOrientationContext.Provider value={value}>{children}</FlowHandleOrientationContext.Provider>;
}

export function useFlowHandleOrientation(): TransformCanvasHandleOrientation {
  return useContext(FlowHandleOrientationContext);
}
