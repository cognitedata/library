import type { DeploymentPackDefinition } from "./types";

export const QUALITIZER_DEPLOYMENT_PACK: DeploymentPackDefinition = {
  id: "dp:app:qualitizer",
  name: "Qualitizer",
  description:
    "This application (Qualitizer). Included on every deployment-pack scan for reporting; not inferred from CDF resources.",
  signals: {},
  reportingMarker: "qualitizer",
};
