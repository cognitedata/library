/**
 * React Flow `type` strings for validation-rule **layout** nodes on the canvas.
 * Persisted in workflow canvas YAML; keep literals stable unless migrating stored documents.
 */
export const discoveryValidationRuleLayoutRfTypes = new Set<string>([
  "discoveryMatchValidationRuleSourceView",
  "discoveryMatchValidationRuleExtraction",
  "discoveryMatchValidationRuleAliasing",
]);

/** Data-model / classic / RAW query nodes (``fn_dm_*_query``). Incoming data edges must come from ``discoveryStart`` only. */
export const discoveryQueryRfTypes = new Set<string>([
  "discoveryViewQuery",
  "discoveryRawQuery",
  "discoveryClassicQuery",
  "discoverySqlQuery",
]);

/**
 * Persistence / terminal layout nodes whose primary ``out`` data edge may target only ``discoveryEnd``
 * (saves, alias persistence, inverted-index persistence).
 * (Upstream may still wire into these nodes.)
 */
export const discoveryPersistenceOutboundToEndOnlyRfTypes = new Set<string>([
  "discoveryViewSave",
  "discoveryRawSave",
  "discoveryClassicSave",
  "discoveryAliasPersistence",
  "discoveryInvertedIndex",
]);

/** Pipeline stages and subgraph frames that can be disabled without removing from the canvas. */
export const discoveryWorkflowDisableableRfTypes = new Set<string>([
  "discoverySubgraph",
  "discoveryViewSave",
  "discoveryRawSave",
  "discoveryClassicSave",
  "discoveryViewQuery",
  "discoveryRawQuery",
  "discoveryClassicQuery",
  "discoverySqlQuery",
  "discoveryTransform",
  "discoveryMerge",
  "discoveryJoin",
  "discoveryValidate",
  "discoveryInstanceFilter",
  "discoveryConfidenceFilter",
  "discoveryInvertedIndex",
]);

/** Discovery pipeline stages (canvas kinds compiled to ``fn_dm_*`` Cognite Functions). */
export const discoveryStageRfTypes = new Set<string>([
  "discoveryViewSave",
  "discoveryRawSave",
  "discoveryClassicSave",
  "discoveryViewQuery",
  "discoveryRawQuery",
  "discoveryClassicQuery",
  "discoverySqlQuery",
  "discoveryTransform",
  "discoveryMerge",
  "discoveryJoin",
  "discoveryValidate",
  "discoveryInstanceFilter",
  "discoveryConfidenceFilter",
]);

