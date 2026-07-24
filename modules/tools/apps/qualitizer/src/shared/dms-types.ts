export type ViewSource = {
  type: "view";
  space: string;
  externalId: string;
  version: string;
};

export type LoadState = "idle" | "loading" | "success" | "error";

export type LoadProgress = {
  phase: string;
  current: number;
  total: number;
  detail?: string;
};
