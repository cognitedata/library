/** Business configuration shapes for pipeline ``config.data``. */

/** Tree node; child nodes use ``locations`` for nested scope hierarchy. */
export type ScopeNode = {
  name?: string;
  description?: string;
  locations?: ScopeNode[];
  files?: string[];
};

/** Hierarchy levels and scope tree under ``config.data``. */
export type ScopeHierarchyData = {
  hierarchy_levels: string[];
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
  return { name: "", description: "", locations: [], files: [] };
}

export function emptyPattern(): PatternEntry {
  return { category: "general", sample: [] };
}
