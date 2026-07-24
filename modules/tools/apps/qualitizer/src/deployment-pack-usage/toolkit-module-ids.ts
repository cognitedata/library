import { isKnownDeploymentPackId, normalizeDeploymentPackId } from "./deployment-pack-aliases";
import { TOOLKIT_MODULE_ID_EXCLUSIONS } from "./qualitizer-deployment-pack";

export type ClassifiedToolkitModuleIds = {
  packIds: string[];
  unknownModuleIds: string[];
};

const EXCLUDED = new Set(TOOLKIT_MODULE_ID_EXCLUSIONS);

export function classifyToolkitModuleIds(moduleIds: string[]): ClassifiedToolkitModuleIds {
  const packIdSet = new Set<string>();
  const unknown = new Set<string>();

  for (const raw of moduleIds) {
    const trimmed = raw.trim();
    if (!trimmed || EXCLUDED.has(trimmed)) continue;
    if (isKnownDeploymentPackId(trimmed)) {
      packIdSet.add(normalizeDeploymentPackId(trimmed));
      continue;
    }
    unknown.add(trimmed);
  }

  return {
    packIds: [...packIdSet].sort((a, b) => a.localeCompare(b)),
    unknownModuleIds: [...unknown].sort((a, b) => a.localeCompare(b)),
  };
}
