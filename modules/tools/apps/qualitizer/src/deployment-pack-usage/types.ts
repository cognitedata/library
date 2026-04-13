export type DataModelRef = {
  space: string;
  externalId: string;
  version?: string;
};

export type IsaManufacturingDerivativeRule = {
  minDistinctiveViewsInOneDataModel: number;
};

export type DeploymentPackDefinition = {
  id: string;
  name: string;
  description: string;
  signals: {
    functionExternalIds?: string[];
    dataModels?: DataModelRef[];
    transformationExternalIds?: string[];
    locationFilterExternalIds?: string[];
  };
  /**
   * Heuristic: fingerprint views from the official ISA 88/95 manufacturing template
   * (library `isa_manufacturing_extension`) — tolerates renamed spaces/data models if the
   * view set still resembles the template.
   */
  isaManufacturingDerivative?: IsaManufacturingDerivativeRule;
  /**
   * When set to `qualitizer`, this pack is not probed against CDF. It is always reported as
   * in use while scanning from Qualitizer so analytics payloads always include the app.
   */
  reportingMarker?: "qualitizer";
};

export type DeploymentPackMatch = {
  kind: "function" | "dataModel" | "transformation" | "locationFilter";
  detail: string;
};

export type DeploymentPackUsageResult = {
  packId: string;
  packName: string;
  description: string;
  inUse: boolean;
  matched: DeploymentPackMatch[];
  missing: DeploymentPackMatch[];
};

export type DeploymentPackProbeContext = {
  hasFunction: (externalId: string) => Promise<boolean>;
  hasDataModel: (ref: DataModelRef) => Promise<boolean>;
  hasTransformation: (externalId: string) => Promise<boolean>;
  hasLocationFilter: (externalId: string) => Promise<boolean>;
  evaluateIsaManufacturingDerivative?: (
    rule: IsaManufacturingDerivativeRule
  ) => Promise<{ inUse: boolean; matched: DeploymentPackMatch[]; missing: DeploymentPackMatch[] }>;
};
