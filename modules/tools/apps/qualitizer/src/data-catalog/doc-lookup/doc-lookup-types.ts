export type DocLookupViewRef = {
  space: string;
  externalId: string;
  version: string;
};

export type DocLookupNodeSummary = {
  space: string;
  externalId: string;
  instanceType: "node";
  typeSpace: string | null;
  typeExternalId: string | null;
  createdTime: number | null;
  lastUpdatedTime: number | null;
  version: number | null;
};

export type PropertyValueChange = {
  path: string;
  kind: "added" | "removed" | "changed";
  baseline: unknown;
  value: unknown;
};

export type DocLookupViewVersionData = {
  view: DocLookupViewRef;
  properties: Record<string, unknown>;
  retrieveError: string | null;
  propertyKey: string | null;
  viewLastUpdatedTime: number | null;
  previousVersion: string | null;
  dataMatchesPrevious: boolean;
  changesFromPrevious: PropertyValueChange[];
};

export type DocLookupViewRetrieveChunk = {
  sources: DocLookupViewRef[];
  request: unknown;
  response: unknown;
};

export type DocLookupViewDefinitionData = {
  viewSpace: string;
  viewExternalId: string;
  versions: DocLookupViewVersionData[];
  allVersionsIdentical: boolean;
  uniqueStoredDataCount: number;
  latestVersion: string;
  displayVersions: DocLookupViewVersionData[];
  retrieveDiagnostics: DocLookupViewRetrieveChunk[];
};

export type DocLookupNodeResult = {
  node: DocLookupNodeSummary;
  views: DocLookupViewRef[];
  viewDefinitions: DocLookupViewDefinitionData[];
  inspectError: string | null;
  rawListItem: unknown;
  rawInspectResponse: unknown | null;
};

export type DocLookupResult = {
  queriedExternalId: string;
  nodes: DocLookupNodeResult[];
  listTruncated: boolean;
};

export type DocLookupProgress = {
  phase: "list" | "inspect" | "retrieve";
  current: number;
  total: number;
  detail?: string;
};

export type DocLookupViewSearchResult = {
  request: unknown;
  response: unknown;
  error: string | null;
};
