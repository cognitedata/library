/**
 * React Flow `type` strings for validation-rule **layout** nodes on the canvas.
 * Persisted in workflow canvas YAML; keep literals stable unless migrating stored documents.
 */
export const keaValidationRuleLayoutRfTypes = new Set<string>([
  "keaMatchValidationRuleSourceView",
  "keaMatchValidationRuleExtraction",
  "keaMatchValidationRuleAliasing",
]);
