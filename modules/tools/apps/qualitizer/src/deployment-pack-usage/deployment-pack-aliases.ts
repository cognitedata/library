import { DEPLOYMENT_PACKS } from "./deployment-packs";
import { QUALITIZER_DEPLOYMENT_PACK } from "./qualitizer-deployment-pack";

const ALL_PACKS = [QUALITIZER_DEPLOYMENT_PACK, ...DEPLOYMENT_PACKS];

const KNOWN_IDS = new Set(ALL_PACKS.map((p) => p.id));

const NAME_TO_ID = new Map(ALL_PACKS.map((p) => [p.name, p.id]));

const SYNONYM_TO_ID = new Map<string, string>();
for (const pack of ALL_PACKS) {
  for (const synonym of pack.synonyms ?? []) {
    SYNONYM_TO_ID.set(synonym, pack.id);
  }
}

const DISPLAY_NAME_BY_ID = new Map(ALL_PACKS.map((p) => [p.id, p.name]));

export function normalizeDeploymentPackId(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return trimmed;

  const lower = trimmed.toLowerCase();
  const synonym = SYNONYM_TO_ID.get(lower);
  if (synonym) return synonym;
  if (KNOWN_IDS.has(lower)) return lower;

  const byName = NAME_TO_ID.get(trimmed);
  if (byName) return byName;

  for (const [name, id] of NAME_TO_ID) {
    if (name.toLowerCase() === lower) return id;
  }

  return trimmed;
}

export function normalizePackInUse(packInUse: Record<string, boolean>): Record<string, boolean> {
  const normalized: Record<string, boolean> = {};
  for (const [key, inUse] of Object.entries(packInUse)) {
    if (!inUse) continue;
    normalized[normalizeDeploymentPackId(key)] = true;
  }
  return normalized;
}

export function packIdsFromNormalizedInUse(packInUse: Record<string, boolean>): string[] {
  return Object.keys(normalizePackInUse(packInUse)).sort((a, b) => a.localeCompare(b));
}

export function deploymentPackDisplayName(packId: string): string {
  const canonical = normalizeDeploymentPackId(packId);
  return DISPLAY_NAME_BY_ID.get(canonical) ?? canonical;
}

export function isKnownDeploymentPackId(raw: string): boolean {
  return KNOWN_IDS.has(normalizeDeploymentPackId(raw));
}
