import type { CellDetails, DataSetSummary, GroupSummary, NormalizedCapability } from "@/permissions/types";

type Translator = (key: string, params?: Record<string, string | number>) => string;

export function normalizeCapability(capability: Record<string, unknown>): NormalizedCapability {
  const entries = Object.entries(capability).filter(([key]) => key !== "projectUrlNames");
  if (entries.length === 0) {
    return { name: "Unknown" };
  }
  const [name, value] = entries[0];
  const normalized = value as NormalizedCapability;
  return { ...normalized, name: name.replace("Acl", "") };
}

export function scopeIsUnrestricted(scope?: Record<string, unknown>): boolean {
  if (!scope) return true;
  const keys = Object.keys(scope);
  if (keys.length === 0) return true;
  if (keys.includes("all") || keys.includes("allScope")) return true;
  return false;
}

function mergeDatasetIds(scopes: (Record<string, unknown> | undefined)[]): number[] | undefined {
  const ids = new Set<number>();
  for (const s of scopes) {
    const ds = s?.["datasetScope"] as { ids?: (number | string)[] } | undefined;
    for (const id of ds?.ids ?? []) ids.add(Number(id));
  }
  if (ids.size === 0) return undefined;
  return [...ids].sort((a, b) => a - b);
}

function mergeIdScopeIds(scopes: (Record<string, unknown> | undefined)[]): number[] | undefined {
  const ids = new Set<number>();
  for (const s of scopes) {
    const is = s?.["idScope"] as { ids?: (number | string)[] } | undefined;
    for (const id of is?.ids ?? []) ids.add(Number(id));
  }
  if (ids.size === 0) return undefined;
  return [...ids].sort((a, b) => a - b);
}

function mergeSpaceIds(scopes: (Record<string, unknown> | undefined)[]): string[] | undefined {
  const spaceIds = new Set<string>();
  for (const s of scopes) {
    const sp = s?.["spaceIdScope"] as { spaceIds?: string[] } | undefined;
    for (const id of sp?.spaceIds ?? []) spaceIds.add(id);
  }
  if (spaceIds.size === 0) return undefined;
  return [...spaceIds].sort((a, b) => a.localeCompare(b));
}

function mergeApps(scopes: (Record<string, unknown> | undefined)[]): string[] | undefined {
  const apps = new Set<string>();
  for (const s of scopes) {
    const ap = s?.["appScope"] as { apps?: string[] } | undefined;
    for (const id of ap?.apps ?? []) apps.add(id);
  }
  if (apps.size === 0) return undefined;
  return [...apps].sort((a, b) => a.localeCompare(b));
}

function mergeDbsToTables(
  scopes: (Record<string, unknown> | undefined)[]
): Record<string, string[]> | undefined {
  const byDb = new Map<string, Set<string>>();
  for (const s of scopes) {
    const ts = s?.["tableScope"] as { dbsToTables?: Record<string, string[] | string> } | undefined;
    if (!ts?.dbsToTables) continue;
    for (const [db, tables] of Object.entries(ts.dbsToTables)) {
      if (!byDb.has(db)) byDb.set(db, new Set());
      const set = byDb.get(db)!;
      if (Array.isArray(tables)) for (const tbl of tables) set.add(tbl);
      else if (typeof tables === "string") set.add(tables);
    }
  }
  if (byDb.size === 0) return undefined;
  const out: Record<string, string[]> = {};
  for (const [db, set] of [...byDb.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    out[db] = [...set].sort((a, b) => a.localeCompare(b));
  }
  return out;
}

function mergeScopesWidest(scopes: (Record<string, unknown> | undefined)[]): Record<string, unknown> {
  if (scopes.some(scopeIsUnrestricted)) {
    return { all: {} };
  }
  const defined = scopes.filter((s): s is Record<string, unknown> => s != null);
  const merged: Record<string, unknown> = {};
  const ds = mergeDatasetIds(scopes);
  if (ds) merged["datasetScope"] = { ids: ds };
  const idSc = mergeIdScopeIds(scopes);
  if (idSc) merged["idScope"] = { ids: idSc };
  const sp = mergeSpaceIds(scopes);
  if (sp) merged["spaceIdScope"] = { spaceIds: sp };
  const ap = mergeApps(scopes);
  if (ap) merged["appScope"] = { apps: ap };
  const tb = mergeDbsToTables(scopes);
  if (tb) merged["tableScope"] = { dbsToTables: tb };
  for (const s of defined) {
    for (const [k, v] of Object.entries(s)) {
      if (
        k === "datasetScope" ||
        k === "idScope" ||
        k === "spaceIdScope" ||
        k === "appScope" ||
        k === "tableScope" ||
        k === "all" ||
        k === "allScope"
      ) {
        continue;
      }
      if (merged[k] === undefined) merged[k] = v;
    }
  }
  return merged;
}

export function mergeCapabilitiesWidestForSameName(
  capabilities: NormalizedCapability[]
): NormalizedCapability | null {
  if (capabilities.length === 0) return null;
  const name = capabilities[0]!.name;
  const actionSet = new Set<string>();
  for (const c of capabilities) {
    for (const a of c.actions ?? []) actionSet.add(a);
  }
  const actions = [...actionSet].sort();
  const scope = mergeScopesWidest(capabilities.map((c) => c.scope));
  const out: NormalizedCapability = { name, actions };
  if (Object.keys(scope).length > 0) out.scope = scope;
  return out;
}

export function getCapability(
  group: GroupSummary,
  name: string
): Record<string, unknown> | undefined {
  return group.capabilities.find((cap) => normalizeCapability(cap).name === name);
}

export function stableCapabilitySignature(norm: NormalizedCapability): string {
  const actions = [...(norm.actions ?? [])].sort();
  return JSON.stringify({ name: norm.name, actions, scope: norm.scope ?? {} });
}

function stableJsonCanonical(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableJsonCanonical).join(",")}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableJsonCanonical(obj[k])}`).join(",")}}`;
}

export function capabilityScopesDeepEqual(
  a: Record<string, unknown> | undefined,
  b: Record<string, unknown> | undefined
): boolean {
  return stableJsonCanonical(a ?? {}) === stableJsonCanonical(b ?? {});
}

export function getActionDisplay(cap: NormalizedCapability, t?: Translator): CellDetails {
  const actions = cap.actions ?? [];
  if (actions.length === 0) {
    return { shortText: "", titleText: "", color: "" };
  }
  if (actions.length === 1 && actions.includes("READ")) {
    return { shortText: "R", titleText: t ? t("permissions.legend.read") : "Read", color: "#bae6fd" };
  }
  if (actions.length === 2 && actions.includes("READ") && actions.includes("WRITE")) {
    return {
      shortText: "W",
      titleText: t ? t("permissions.legend.write") : "Read and Write",
      color: "#bbf7d0",
    };
  }
  const advancedReadOps = ["READ", "LIST"];
  const advancedWriteOps = [
    "WRITE",
    "CREATE",
    "DELETE",
    "UPDATE",
    "REVIEW",
    "SUGGEST",
    "WRITE_PROPERTIES",
  ];
  const ownerOps = ["OWNER", "MEMBEROF"];
  if (isContainedWithinActionTypes(advancedReadOps, actions)) {
    return { shortText: "R+", titleText: actions.join(", "), color: "#A6D6D6" };
  }
  if (isContainedWithinActionTypes([...advancedWriteOps, ...advancedReadOps], actions)) {
    return { shortText: "W+", titleText: actions.join(", "), color: "#86efac" };
  }
  if (isContainedWithinActionTypes([...advancedReadOps, ...advancedWriteOps, ...ownerOps], actions)) {
    return { shortText: "O", titleText: actions.join(", "), color: "#fef9c3" };
  }
  return { shortText: "A", titleText: actions.join(", "), color: "#fed7aa" };
}

export type CapabilityActionBand = "empty" | "read" | "write" | "owner" | "advanced";

export function capabilityActionBand(norm: NormalizedCapability): CapabilityActionBand {
  const actions = norm.actions ?? [];
  if (actions.length === 0) return "empty";
  const { shortText } = getActionDisplay(norm);
  if (shortText === "R" || shortText === "R+") return "read";
  if (shortText === "W" || shortText === "W+") return "write";
  if (shortText === "O") return "owner";
  if (shortText === "A") return "advanced";
  return "empty";
}

/** Same capability name and scope; signatures differ only by read-tier vs write-tier (R/R+ vs W/W+). */
export function driftIsReadWriteTierOnly(leftSig: string, rightSig: string): boolean {
  let left: NormalizedCapability;
  let right: NormalizedCapability;
  try {
    left = JSON.parse(leftSig) as NormalizedCapability;
    right = JSON.parse(rightSig) as NormalizedCapability;
  } catch {
    return false;
  }
  if (left.name !== right.name) return false;
  if (!capabilityScopesDeepEqual(left.scope, right.scope)) return false;
  if (stableCapabilitySignature(left) === stableCapabilitySignature(right)) return false;
  const bandL = capabilityActionBand(left);
  const bandR = capabilityActionBand(right);
  return (
    (bandL === "read" && bandR === "write") ||
    (bandL === "write" && bandR === "read")
  );
}

export function isContainedWithinActionTypes(referenceSet: string[], actions: string[]): boolean {
  for (const action of actions) {
    if (!referenceSet.includes(action)) {
      return false;
    }
  }
  return true;
}

export function getScopeDisplay(
  cap: NormalizedCapability,
  datasets: DataSetSummary[],
  t?: Translator
): CellDetails {
  if (JSON.stringify(cap.scope ?? {}) === '{"all":{}}') {
    return { shortText: "", titleText: t ? t("permissions.scope.all") : "All", color: "" };
  }
  const details: CellDetails[] = [];
  const scope = cap.scope ?? {};
  const datasetScope = scope["datasetScope"] as { ids?: number[] } | undefined;
  if (datasetScope?.ids) {
    
    
    const names = datasetScope.ids.map((id) => {
      // Some input data comes as stirngs, some as numbers, so make sure to compare as strings
      const dataset = datasets.find((ds) => `${ds.id}` === `${id}`);
      return dataset?.name ?? String(id);
    });

    details.push({
      shortText: `DS[${names.length}]`,
      titleText: `${t ? t("permissions.scope.datasets") : "Datasets"}:\n${names.join("\n")}`,
      color: "",
    });
  }
  const idScope = scope["idScope"] as { ids?: number[] } | undefined;
  if (idScope?.ids) {
    const names = idScope.ids.map((id) => {
      const dataset = datasets.find((ds) => ds.id === id);
      return dataset?.name ?? String(id);
    });
    details.push({
      shortText: `ID[${names.length}]`,
      titleText: `${t ? t("permissions.scope.ids") : "IDs"}:\n${names.join("\n")}`,
      color: "",
    });
  }
  const spaceIdScope = scope["spaceIdScope"] as { spaceIds?: string[] } | undefined;
  if (spaceIdScope?.spaceIds) {
    details.push({
      shortText: `SP[${spaceIdScope.spaceIds.length}]`,
      titleText: `${t ? t("permissions.scope.spaces") : "Spaces"}:\n${spaceIdScope.spaceIds.join("\n")}`,
      color: "",
    });
  }
  const appScope = scope["appScope"] as { apps?: string[] } | undefined;
  if (appScope?.apps) {
    details.push({
      shortText: `APP[${appScope.apps.length}]`,
      titleText: `${t ? t("permissions.scope.apps") : "Apps"}:\n${appScope.apps.join("\n")}`,
      color: "",
    });
  }
  const tableScope = scope["tableScope"] as { dbsToTables?: Record<string, string[]> } | undefined;
  if (tableScope?.dbsToTables) {
    const tables = Object.keys(tableScope.dbsToTables);
    details.push({
      shortText: `TB[${tables.length}]`,
      titleText: `${t ? t("permissions.scope.tables") : "Tables"}:\n${tables.join("\n")}`,
      color: "",
    });
  }
  if (details.length === 1) return details[0];
  if (details.length > 1) {
    return {
      shortText: t ? t("permissions.scope.multi") : "Multi",
      titleText: details.map((detail) => detail.titleText).join("; "),
      color: "#e9d5ff",
    };
  }
  return {
    shortText: t ? t("permissions.scope.unknown") : "Unknown",
    titleText: JSON.stringify(cap.scope ?? {}),
    color: "#fecaca",
  };
}
