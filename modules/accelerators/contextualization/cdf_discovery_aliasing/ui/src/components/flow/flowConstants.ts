/**
 * React Flow `type` strings for validation-rule **layout** nodes on the canvas.
 * Persisted in workflow canvas YAML; keep literals stable unless migrating stored documents.
 */
export const keaValidationRuleLayoutRfTypes = new Set<string>([
  "keaMatchValidationRuleSourceView",
  "keaMatchValidationRuleExtraction",
  "keaMatchValidationRuleAliasing",
]);

/** Data-model / classic / RAW query nodes (``fn_dm_*_query``). Incoming data edges must come from ``keaStart`` only. */
export const keaDiscoveryQueryRfTypes = new Set<string>(["keaViewQuery", "keaRawQuery", "keaClassicQuery"]);

/**
 * Persistence / terminal layout nodes whose primary ``out`` data edge may target only ``keaEnd``
 * (saves, alias persistence, inverted-index persistence).
 * (Upstream may still wire into these nodes.)
 */
export const keaPersistenceOutboundToEndOnlyRfTypes = new Set<string>([
  "keaViewSave",
  "keaRawSave",
  "keaClassicSave",
  "keaAliasPersistence",
  "keaInvertedIndex",
]);

/** Discovery pipeline stages (canvas kinds compiled to ``fn_dm_*`` Cognite Functions). */
export const keaDiscoveryStageRfTypes = new Set<string>([
  "keaViewSave",
  "keaRawSave",
  "keaClassicSave",
  "keaViewQuery",
  "keaRawQuery",
  "keaClassicQuery",
  "keaTransform",
  "keaJoin",
  "keaDiscoveryValidate",
  "keaDiscoveryInstanceFilter",
  "keaDiscoveryConfidenceFilter",
]);

/** Nodes whose React Flow card exposes a ``validation`` source handle for match-definition layout wiring. */
export function rfTypeHasPipelineValidationSourceHandle(t: string | undefined): boolean {
  if (!t) return false;
  return t === "keaExtraction" || t === "keaAliasing";
}
