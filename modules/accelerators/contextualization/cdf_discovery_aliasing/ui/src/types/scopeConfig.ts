/** Loose JSON for v1 scope / trigger configuration. */

export type JsonObject = Record<string, unknown>;

export type LocationNode = {
  id?: string;
  name?: string;
  description?: string;
  locations?: LocationNode[];
};

export type AliasingScopeHierarchy = {
  levels?: string[];
  locations?: LocationNode[];
};

export function emptyLocationNode(): LocationNode {
  return { id: "", name: "", description: "", locations: [] };
}
