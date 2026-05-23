/** Business configuration shapes for pipeline ``config.data``. */

/** Tree node; child nodes use ``locations`` for nested scope hierarchy. */
export type ScopeNode = {
  id?: string;
  name?: string;
  description?: string;
  locations?: ScopeNode[];
  files?: string[];
};

/** Top-level ``scope_hierarchy`` in ``default.config.yaml``. */
export type ScopeHierarchyData = {
  levels: string[];
  scope: ScopeNode[];
};

export type PatternEntry = {
  category?: string;
  resourceType?: string;
  resourceSubType?: string;
  standard?: string;
  /** Pipeline YAML uses ``sample``; ``samples`` accepted when reading. */
  sample: string[];
};

export type PatternsData = {
  patterns: PatternEntry[];
};

export function emptyScopeNode(): ScopeNode {
  return { id: "", name: "", description: "", locations: [], files: [] };
}

export function emptyPattern(): PatternEntry {
  return { category: "general", sample: [] };
}
