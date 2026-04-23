import type { Node } from "@xyflow/react";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { orderFlowNodesForReactFlow } from "./flowDocumentBridge";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";

/** Ids removed when deleting a ``keaSubflow`` frame (frame + optional legacy hub nodes). */
export function collectSubflowFrameAndHubIds(nodes: Node[], subflowId: string): Set<string> {
  const sf = nodes.find((n) => n.id === subflowId);
  if (!sf || sf.type !== "keaSubflow") return new Set([subflowId]);
  const data = (sf.data ?? {}) as WorkflowCanvasNodeData;
  const hubIn = String(data.subflow_hub_input_id ?? "").trim();
  const hubOut = String(data.subflow_hub_output_id ?? "").trim();
  const out = new Set<string>([subflowId]);
  if (hubIn) out.add(hubIn);
  if (hubOut) out.add(hubOut);
  for (const n of nodes) {
    const pid = n.parentId != null && String(n.parentId).trim() ? String(n.parentId).trim() : "";
    if (pid !== subflowId) continue;
    if (n.type === "keaSubflowGraphIn" || n.type === "keaSubflowGraphOut") {
      out.add(n.id);
    }
  }
  return out;
}

/**
 * Delete a ``keaSubflow`` group: lift every non-hub direct child to the root canvas (preserving
 * absolute layout), then remove the frame and hub nodes only.
 */
export function removeSubflowFrameAndLiftChildren(nodes: Node[], subflowId: string): Node[] {
  const sf = nodes.find((n) => n.id === subflowId);
  if (!sf || sf.type !== "keaSubflow") return nodes;

  const removeIds = collectSubflowFrameAndHubIds(nodes, subflowId);
  const data = (sf.data ?? {}) as WorkflowCanvasNodeData;
  const explicitHubs = new Set(
    [String(data.subflow_hub_input_id ?? "").trim(), String(data.subflow_hub_output_id ?? "").trim()].filter(Boolean)
  );

  let next = [...nodes];
  const directChildren = next.filter((n) => {
    const pid = n.parentId != null && String(n.parentId).trim() ? String(n.parentId).trim() : "";
    return pid === subflowId;
  });
  for (const ch of directChildren) {
    if (explicitHubs.has(ch.id)) continue;
    if (ch.type === "keaSubflowGraphIn" || ch.type === "keaSubflowGraphOut") continue;
    next = assignFlowNodeSubflowParent(next, ch.id, null);
  }
  return orderFlowNodesForReactFlow(next.filter((n) => !removeIds.has(n.id)));
}
