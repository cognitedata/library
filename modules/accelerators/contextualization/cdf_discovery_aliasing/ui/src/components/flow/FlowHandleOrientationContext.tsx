import { createContext, useContext } from "react";
import type { WorkflowCanvasHandleOrientation } from "../../types/workflowCanvas";

const FlowHandleOrientationContext = createContext<WorkflowCanvasHandleOrientation>("lr");

export function FlowHandleOrientationProvider({
  value,
  children,
}: {
  value: WorkflowCanvasHandleOrientation;
  children: React.ReactNode;
}) {
  return <FlowHandleOrientationContext.Provider value={value}>{children}</FlowHandleOrientationContext.Provider>;
}

export function useFlowHandleOrientation(): WorkflowCanvasHandleOrientation {
  return useContext(FlowHandleOrientationContext);
}
