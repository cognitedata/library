export type SelectedDataModel = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
};

export type SelectedView = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
  usedFor?: "node" | "edge" | "all";
};
