import type { PaletteDragPayload } from "../components/flow/FlowPalette";

export type OpenTarget =
  | { type: "classic_list"; resource_type: string }
  | {
      type: "dm_instances";
      view_space: string;
      view_external_id: string;
      view_version: string;
    }
  | { type: "raw_rows"; database: string; table: string };

export type TreeNode = {
  id: string;
  label: string;
  kind: string;
  has_children: boolean;
  /** True when this node id is in operator favorites (``stars.node_ids``). */
  starred?: boolean;
  open_target?: OpenTarget;
  meta?: {
    palette_payload?: PaletteDragPayload;
    domain?: string;
    [key: string]: unknown;
  };
};

export type DataTreeEntityDragPayload = {
  kind: "data_tree_entity";
  node: TreeNode;
};
