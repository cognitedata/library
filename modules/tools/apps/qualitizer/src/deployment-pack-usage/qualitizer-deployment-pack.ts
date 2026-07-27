import type { DeploymentPackDefinition } from "./types";

export const QUALITIZER_DEPLOYMENT_PACK: DeploymentPackDefinition = {
  id: "dp:app:qualitizer",
  name: "Qualitizer",
  synonyms: ["dp:tool:qualitizer"],
  description:
    "This application (Qualitizer). Included on every deployment-pack scan for reporting; not inferred from CDF resources.",
  signals: {},
  reportingMarker: "qualitizer",
};

/** Toolkit quickpanel `moduleIds` entries that are not deployment packs (exact match). */
export const TOOLKIT_MODULE_ID_EXCLUSIONS: readonly string[] = [
  "a-mis_transformations",
  "Atlas AI agents",
  "auth",
  "cdf_cdm_migration",
];
