export type DmVersionSnapshot = {
  space: string;
  externalId: string;
  version: string;
  name?: string;
  description?: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  views?: unknown;
};

export type ViewRef = {
  key: string;
  version: string;
  raw: unknown;
};

export type PropChange = {
  kind: "add" | "remove" | "modify";
  name: string;
  before?: string;
  after?: string;
  semanticLines?: string[];
};

export type ViewVersionDiff = {
  viewLabel: string;
  fromVersion: string;
  toVersion: string;
  metaChanges: string[];
  propChanges: PropChange[];
  filterChanged: boolean;
};

export type TransitionDiff = {
  fromVersion: string;
  toVersion: string;
  fromSnap: DmVersionSnapshot;
  toSnap: DmVersionSnapshot;
  modelMetaChanges: string[];
  viewsAdded: ViewRef[];
  viewsRemoved: ViewRef[];
  viewVersionChanges: Array<{ ref: ViewRef; prevRef: ViewRef; viewDiff: ViewVersionDiff | null }>;
};
