import type { DeploymentPackMatch, IsaManufacturingDerivativeRule } from "./types";

export type IsaDataModelSummary = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
  description?: string;
  viewExternalIds: string[];
};

/**
 * ISA-specific view externalIds from cognitedata/library
 * `modules/models/isa_manufacturing_extension` SLA/EDM (same ISA view set as SLM).
 * @see https://github.com/cognitedata/library/tree/main/modules/models/isa_manufacturing_extension
 */
export const ISA_MANUFACTURING_TEMPLATE_VIEW_EXTERNAL_IDS = [
  "ISAAsset",
  "Area",
  "Batch",
  "ControlModule",
  "Enterprise",
  "Equipment",
  "EquipmentModule",
  "ProcessParameter",
  "Operation",
  "Phase",
  "Procedure",
  "ProcessCell",
  "QualityResult",
  "Recipe",
  "Site",
  "Unit",
  "UnitProcedure",
  "WorkOrder",
  "ProductRequest",
  "ProductDefinition",
  "ProductSegment",
  "Material",
  "MaterialLot",
  "Personnel",
  "ISAFile",
  "ISATimeSeries",
] as const;

const TEMPLATE_LOWER = new Set(
  ISA_MANUFACTURING_TEMPLATE_VIEW_EXTERNAL_IDS.map((id) => id.toLowerCase())
);

function countTemplateViewOverlap(viewExternalIds: string[]): number {
  const present = new Set(viewExternalIds.map((id) => id.toLowerCase()));
  let n = 0;
  for (const id of TEMPLATE_LOWER) {
    if (present.has(id)) n++;
  }
  return n;
}

function metadataIsaManufacturingScore(dm: IsaDataModelSummary): number {
  const blob = [dm.space, dm.externalId, dm.name ?? "", dm.description ?? ""]
    .join(" ")
    .toLowerCase();
  let s = 0;
  if (/isa_manufacturing|isa-manufacturing|isa manufacturing/.test(blob)) s += 5;
  if (/isa[\s_-]?(88|95)/.test(blob)) s += 3;
  if ((/\bedm\b|\bslm\b/.test(blob) || blob.includes("enterprise data model")) && /manufactur/.test(blob))
    s += 2;
  if (/_isa_|sp_isa|\.isa\.|isa batch|batch manufactur/.test(blob)) s += 3;
  return s;
}

export function evaluateIsaManufacturingDerivativeFromModels(
  models: IsaDataModelSummary[],
  rule: IsaManufacturingDerivativeRule
): {
  inUse: boolean;
  matched: DeploymentPackMatch[];
  missing: DeploymentPackMatch[];
} {
  const minV = rule.minDistinctiveViewsInOneDataModel;

  const anyViewsListed = models.some((m) => m.viewExternalIds.length > 0);
  if (!anyViewsListed) {
    for (const dm of models) {
      const meta = metadataIsaManufacturingScore(dm);
      const blob = `${dm.externalId} ${dm.name ?? ""} ${dm.description ?? ""}`;
      if (
        meta >= 8 &&
        /isa|manufactur|isa-?88|isa-?95|batch control/i.test(blob)
      ) {
        return {
          inUse: true,
          matched: [
            {
              kind: "dataModel",
              detail: `Weak signal: ${dm.space}/${dm.externalId} matches ISA manufacturing naming (data model list had no view membership — confirm in UI or retrieve with inline views).`,
            },
          ],
          missing: [],
        };
      }
    }
  }

  for (const dm of models) {
    const vCount = countTemplateViewOverlap(dm.viewExternalIds);
    const meta = metadataIsaManufacturingScore(dm);
    const nameBlob = `${dm.externalId} ${dm.name ?? ""}`;

    const hitsTemplateThreshold = vCount >= minV;
    const strongCombo = vCount >= 10 && meta >= 5;
    const softCombo = vCount >= 8 && meta >= 6;
    const isaNamingAndViews =
      /isa_manufacturing|isa manufacturing|isa_manufactur/i.test(nameBlob) && vCount >= 8;

    if (hitsTemplateThreshold || strongCombo || softCombo || isaNamingAndViews) {
      const matched: DeploymentPackMatch[] = [
        {
          kind: "dataModel",
          detail: `ISA 88/95 template views: ${vCount}/${ISA_MANUFACTURING_TEMPLATE_VIEW_EXTERNAL_IDS.length} in ${dm.space}/${dm.externalId}${dm.version ? `@${dm.version}` : ""}`,
        },
      ];
      if (meta > 0) {
        matched.push({
          kind: "dataModel",
          detail: `Naming/metadata cues (score ${meta}) align with Hub ISA manufacturing pack pattern`,
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
        detail: `No composed data model matched the ISA manufacturing derivative heuristic (best ${bestCount}/${ISA_MANUFACTURING_TEMPLATE_VIEW_EXTERNAL_IDS.length} template views at ${bestLoc}; threshold ≥${minV}, or 8–10 views with strong ISA naming / description).`,
      },
    ],
  };
}

export function extractViewExternalIdsFromDataModelViews(views: unknown): string[] {
  if (!Array.isArray(views)) return [];
  const out: string[] = [];
  for (const v of views) {
    if (v && typeof v === "object" && "externalId" in v) {
      const e = (v as { externalId?: unknown }).externalId;
      if (typeof e === "string" && e.length > 0) out.push(e);
    }
  }
  return out;
}
