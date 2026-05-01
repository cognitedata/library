/**
 * Lowercased keys/strings in ``aliasing_pipeline`` YAML that are structural tokens,
 * not tag-transform rule names (shared by canvas sync and scope seeding).
 */
export const ALIASING_PIPELINE_NAME_NOISE: ReadonlySet<string> = new Set(
  [
    "sequential",
    "parallel",
    "concurrent",
    "ordered",
    "hierarchy",
    "mode",
    "children",
    "branches",
    "rules",
    "config",
    "validation",
    "scope_filters",
    "conditions",
    "description",
    "enabled",
    "priority",
    "preserve_original",
    "name",
    "handler",
    "type",
    "match",
    "expression",
    "expressions",
    "extraction",
    "aliasing",
  ].map((s) => s.toLowerCase())
);
