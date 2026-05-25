import type { TreeNode } from "../types/discoveryNodes";
import { GOVERNANCE_GROUPS, GOVERNANCE_SPACES } from "./treeNodeIds";

const SPACES_ADIR_PREFIX = `${GOVERNANCE_SPACES}:adir:`;
const GROUPS_ADIR_PREFIX = `${GOVERNANCE_GROUPS}:adir:`;

export type GovernanceArtifactCreateContext = {
  kind: "spaces" | "groups";
  parentRel: string;
};

function decodeNodeSegment(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

/** Declared-artifact folder prefix (``spaces/...`` or ``auth/...``) for the selected tree node. */
export function governanceArtifactCreateContextFromNode(
  node: Pick<TreeNode, "id" | "kind" | "meta">
): GovernanceArtifactCreateContext | null {
  if (node.meta?.live_cdf === true) return null;
  if (node.kind === "gov_space" || node.kind === "gov_group" || node.kind === "gov_artifact_file") {
    return null;
  }

  if (node.id === GOVERNANCE_SPACES) {
    return { kind: "spaces", parentRel: "spaces" };
  }
  if (node.id === GOVERNANCE_GROUPS) {
    return { kind: "groups", parentRel: "auth" };
  }
  if (node.id.startsWith(SPACES_ADIR_PREFIX)) {
    const prefix = decodeNodeSegment(node.id.slice(SPACES_ADIR_PREFIX.length));
    if (!prefix.startsWith("spaces")) return null;
    return { kind: "spaces", parentRel: prefix };
  }
  if (node.id.startsWith(GROUPS_ADIR_PREFIX)) {
    const prefix = decodeNodeSegment(node.id.slice(GROUPS_ADIR_PREFIX.length));
    if (!prefix.startsWith("auth")) return null;
    return { kind: "groups", parentRel: prefix };
  }
  const artifactPrefix = node.meta?.artifact_prefix;
  if (typeof artifactPrefix === "string" && artifactPrefix.trim()) {
    const prefix = artifactPrefix.trim().replace(/\\/g, "/");
    if (prefix.startsWith("spaces/") || prefix === "spaces") {
      return { kind: "spaces", parentRel: prefix };
    }
    if (prefix.startsWith("auth/") || prefix === "auth") {
      return { kind: "groups", parentRel: prefix };
    }
  }
  return null;
}
