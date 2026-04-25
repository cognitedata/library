import type {
  DeploymentPackDefinition,
  DeploymentPackProbeContext,
  DeploymentPackUsageResult,
} from "./types";

function emptySignals(pack: DeploymentPackDefinition): boolean {
  const s = pack.signals;
  return (
    (s.functionExternalIds?.length ?? 0) === 0 &&
    (s.dataModels?.length ?? 0) === 0 &&
    (s.transformationExternalIds?.length ?? 0) === 0 &&
    (s.locationFilterExternalIds?.length ?? 0) === 0
  );
}

export async function detectDeploymentPackUsage(
  packs: DeploymentPackDefinition[],
  ctx: DeploymentPackProbeContext
): Promise<DeploymentPackUsageResult[]> {
  const out: DeploymentPackUsageResult[] = [];

  for (const pack of packs) {
    if (pack.reportingMarker === "qualitizer") {
      out.push({
        packId: pack.id,
        packName: pack.name,
        description: pack.description,
        inUse: true,
        matched: [],
        missing: [],
      });
      continue;
    }

    if (pack.cfihosOilAndGasDerivative) {
      if (!ctx.evaluateCfihosOilAndGasDerivative) {
        out.push({
          packId: pack.id,
          packName: pack.name,
          description: pack.description,
          inUse: false,
          matched: [],
          missing: [
            {
              kind: "dataModel",
              detail: "CFIHOS oil & gas derivative probe is not available in this context.",
            },
          ],
        });
      } else {
        const r = await ctx.evaluateCfihosOilAndGasDerivative(pack.cfihosOilAndGasDerivative);
        out.push({
          packId: pack.id,
          packName: pack.name,
          description: pack.description,
          inUse: r.inUse,
          matched: r.matched,
          missing: r.missing,
        });
      }
      continue;
    }

    if (pack.isaManufacturingDerivative) {
      if (!ctx.evaluateIsaManufacturingDerivative) {
        out.push({
          packId: pack.id,
          packName: pack.name,
          description: pack.description,
          inUse: false,
          matched: [],
          missing: [
            {
              kind: "dataModel",
              detail: "ISA manufacturing derivative probe is not available in this context.",
            },
          ],
        });
      } else {
        const r = await ctx.evaluateIsaManufacturingDerivative(pack.isaManufacturingDerivative);
        out.push({
          packId: pack.id,
          packName: pack.name,
          description: pack.description,
          inUse: r.inUse,
          matched: r.matched,
          missing: r.missing,
        });
      }
      continue;
    }

    if (emptySignals(pack)) {
      out.push({
        packId: pack.id,
        packName: pack.name,
        description: pack.description,
        inUse: false,
        matched: [],
        missing: [],
      });
      continue;
    }

    const matched: DeploymentPackUsageResult["matched"] = [];
    const missing: DeploymentPackUsageResult["missing"] = [];
    let allPresent = true;

    for (const externalId of pack.signals.functionExternalIds ?? []) {
      if (await ctx.hasFunction(externalId)) {
        matched.push({ kind: "function", detail: externalId });
      } else {
        missing.push({ kind: "function", detail: externalId });
        allPresent = false;
      }
    }

    for (const ref of pack.signals.dataModels ?? []) {
      const label = `${ref.space}/${ref.externalId}${ref.version ? `@${ref.version}` : ""}`;
      if (await ctx.hasDataModel(ref)) {
        matched.push({ kind: "dataModel", detail: label });
      } else {
        missing.push({ kind: "dataModel", detail: label });
        allPresent = false;
      }
    }

    for (const externalId of pack.signals.transformationExternalIds ?? []) {
      if (await ctx.hasTransformation(externalId)) {
        matched.push({ kind: "transformation", detail: externalId });
      } else {
        missing.push({ kind: "transformation", detail: externalId });
        allPresent = false;
      }
    }

    for (const externalId of pack.signals.locationFilterExternalIds ?? []) {
      if (await ctx.hasLocationFilter(externalId)) {
        matched.push({ kind: "locationFilter", detail: externalId });
      } else {
        missing.push({ kind: "locationFilter", detail: externalId });
        allPresent = false;
      }
    }

    out.push({
      packId: pack.id,
      packName: pack.name,
      description: pack.description,
      inUse: allPresent,
      matched,
      missing,
    });
  }

  return out;
}
