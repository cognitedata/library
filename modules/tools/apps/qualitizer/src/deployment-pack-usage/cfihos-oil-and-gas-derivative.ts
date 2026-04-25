import type { IsaDataModelSummary } from "./isa-manufacturing-derivative";
import type { DeploymentPackMatch, CfihosOilAndGasDerivativeRule } from "./types";

/**
 * Customer-space views from cognitedata/library
 * `modules/models/cfihos_oil_and_gas_extension` composed domain model (dm_dom_oil_and_gas /
 * dm_sol_search_oil_and_gas). Excludes cdf_cdm / cdf_idm includes so the fingerprint targets the
 * CFIHOS extension slice, not generic CDM membership.
 * @see https://github.com/cognitedata/library/tree/main/modules/models/cfihos_oil_and_gas_extension
 */
export const CFIHOS_OIL_AND_GAS_TEMPLATE_VIEW_EXTERNAL_IDS = [
  "CommonLCIProperties",
  "Compressor",
  "DrillingEquipment",
  "ElectricalEquipmentClass",
  "Enclosure",
  "Files",
  "FunctionalLocationProperties",
  "HealthSafetyAndEnvironmentEquipmentClass",
  "HeatExchanger",
  "Infrastructure",
  "InstrumentEquipment",
  "ItAndTelecomEquipment",
  "MechanicalEquipmentClass",
  "MiscellaneousEquipment",
  "Notification",
  "PipingAndPipelineEquipment",
  "Pump",
  "SubseaEquipmentClass",
  "Tag",
  "TimeSeriesData",
  "Tool",
  "Turbine",
  "Valve",
  "WorkOrder",
  "WorkOrderOperation",
  "FunctionalLocation",
  "MaintenanceAndIntegrity",
  "FailureMode",
  "Equipment",
] as const;

const TEMPLATE_LOWER = new Set(
  CFIHOS_OIL_AND_GAS_TEMPLATE_VIEW_EXTERNAL_IDS.map((id) => id.toLowerCase())
);

function countTemplateViewOverlap(viewExternalIds: string[]): number {
  const present = new Set(viewExternalIds.map((id) => id.toLowerCase()));
  let n = 0;
  for (const id of TEMPLATE_LOWER) {
    if (present.has(id)) n++;
  }
  return n;
}

function metadataCfihosOilAndGasScore(dm: IsaDataModelSummary): number {
  const blob = [dm.space, dm.externalId, dm.name ?? "", dm.description ?? ""]
    .join(" ")
    .toLowerCase();
  let s = 0;
  if (/cfihos/.test(blob)) s += 6;
  if (/iso[\s_-]?14224|14224/.test(blob)) s += 4;
  if (/oil[\s_-]?and[\s_-]?gas|oil_and_gas|oil & gas/.test(blob)) s += 4;
  if (/tag-?centric|timeseriesdata|unified tag/.test(blob)) s += 2;
  if (/wf_cfihos_oil_and_gas|cfihos_oil_and_gas/.test(blob)) s += 4;
  if (/aveva|opc\s*ua|sap.*maintenance|lci\b/.test(blob) && /oil|gas|cfihos/.test(blob)) s += 2;
  return s;
}

export function evaluateCfihosOilAndGasDerivativeFromModels(
  models: IsaDataModelSummary[],
  rule: CfihosOilAndGasDerivativeRule
): {
  inUse: boolean;
  matched: DeploymentPackMatch[];
  missing: DeploymentPackMatch[];
} {
  const minV = rule.minDistinctiveViewsInOneDataModel;

  const anyViewsListed = models.some((m) => m.viewExternalIds.length > 0);
  if (!anyViewsListed) {
    for (const dm of models) {
      const meta = metadataCfihosOilAndGasScore(dm);
      const blob = `${dm.externalId} ${dm.name ?? ""} ${dm.description ?? ""}`.toLowerCase();
      if (meta >= 10 && /cfihos|iso.?14224|oil.?and.?gas/.test(blob)) {
        return {
          inUse: true,
          matched: [
            {
              kind: "dataModel",
              detail: `Weak signal: ${dm.space}/${dm.externalId} matches CFIHOS oil & gas naming (data model list had no view membership — confirm in UI or retrieve with inline views).`,
            },
          ],
          missing: [],
        };
      }
    }
  }

  for (const dm of models) {
    const vCount = countTemplateViewOverlap(dm.viewExternalIds);
    const meta = metadataCfihosOilAndGasScore(dm);
    const nameBlob = `${dm.externalId} ${dm.name ?? ""}`;

    const hitsTemplateThreshold = vCount >= minV;
    const strongCombo = vCount >= 12 && meta >= 6;
    const softCombo = vCount >= 10 && meta >= 8;
    const cfihosNamingAndViews =
      /cfihos_oil_and_gas|cfihos oil|oil and gas domain/i.test(nameBlob) && vCount >= 10;

    if (hitsTemplateThreshold || strongCombo || softCombo || cfihosNamingAndViews) {
      const matched: DeploymentPackMatch[] = [
        {
          kind: "dataModel",
          detail: `CFIHOS / ISO 14224 oil & gas template views: ${vCount}/${CFIHOS_OIL_AND_GAS_TEMPLATE_VIEW_EXTERNAL_IDS.length} in ${dm.space}/${dm.externalId}${dm.version ? `@${dm.version}` : ""}`,
        },
      ];
      if (meta > 0) {
        matched.push({
          kind: "dataModel",
          detail: `Naming/metadata cues (score ${meta}) align with Hub CFIHOS oil & gas pack pattern`,
        });
      }
      return { inUse: true, matched, missing: [] };
    }
  }

  let best: IsaDataModelSummary | null = null;
  let bestCount = 0;
  for (const dm of models) {
    const c = countTemplateViewOverlap(dm.viewExternalIds);
    if (c > bestCount) {
      bestCount = c;
      best = dm;
    }
  }

  const bestLoc = best
    ? `${best.space}/${best.externalId}${best.version ? `@${best.version}` : ""}`
    : "—";

  return {
    inUse: false,
    matched: [],
    missing: [
      {
        kind: "dataModel",
        detail: `No composed data model matched the CFIHOS oil & gas derivative heuristic (best ${bestCount}/${CFIHOS_OIL_AND_GAS_TEMPLATE_VIEW_EXTERNAL_IDS.length} template views at ${bestLoc}; threshold ≥${minV}, or 10–12 views with strong CFIHOS / ISO 14224 naming).`,
      },
    ],
  };
}
