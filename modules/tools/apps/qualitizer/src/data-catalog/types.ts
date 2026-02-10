export type LoadState = "idle" | "loading" | "success" | "error";

export type ModelNode = {
  key: string;
  label: string;
};

export type ViewNode = {
  key: string;
  label: string;
  space: string;
  externalId: string;
  version?: string;
  usedFor?: "node" | "edge" | "all";
};

export type FieldNode = {
  key: string;
  label: string;
};

export type Link = {
  from: string;
  to: string;
};

export type SelectedNode = {
  column: "dataModels" | "views" | "fields";
  node: ModelNode | ViewNode | FieldNode;
};
