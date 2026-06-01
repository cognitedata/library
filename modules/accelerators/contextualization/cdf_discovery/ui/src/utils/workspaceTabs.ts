import type { ExtractDocumentTab, MonitorDocumentTab, SettingsDocumentTab } from "../types/discoveryNodes";
import type { TreeNode } from "../types/discoveryNodes";
import { EXTRACT_ROOT, MONITOR_ROOT } from "./treeNodeIds";

export function opensExtractTab(node: Pick<TreeNode, "id" | "kind">): boolean {
  return node.id === EXTRACT_ROOT || node.kind === "extract";
}

export function opensMonitorTab(node: Pick<TreeNode, "id" | "kind">): boolean {
  return node.id === MONITOR_ROOT || node.kind === "monitor";
}

export function createExtractTab(label: string): ExtractDocumentTab {
  return { kind: "extract", id: EXTRACT_ROOT, label };
}

export function createMonitorTab(
  label: string,
  activeSection: "workflowState" | "schedules" = "workflowState"
): MonitorDocumentTab {
  return { kind: "monitor", id: MONITOR_ROOT, label, activeSection };
}

export function createSettingsTab(label: string): SettingsDocumentTab {
  return { kind: "settings", id: "settings", label };
}
