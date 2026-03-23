import type {
  CompliantGroupEntry,
  GroupSummary,
  NormalizedCapability,
  PermissionScopeDriftEntry,
} from "./types";

export function renderProgressBar(value: number, total: number) {
  const safeTotal = total > 0 ? total : 0;
  const percent = safeTotal > 0 ? Math.min(100, (value / safeTotal) * 100) : 0;
  return { percent };
}

export function toTimestamp(value: unknown): number | undefined {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  return undefined;
}

export function formatIsoDate(value?: number, fallback = "Unknown"): string {
  if (!value) return fallback;
  return new Date(value).toISOString().slice(0, 10);
}

export function isOlderThanSixMonths(value?: number): boolean {
  if (!value) return false;
  const sixMonthsMs = 1000 * 60 * 60 * 24 * 30 * 6;
  return value < Date.now() - sixMonthsMs;
}

export function normalizeCapability(
  capability: Record<string, unknown>
): NormalizedCapability {
  const entries = Object.entries(capability).filter(
    ([key]) => key !== "projectUrlNames"
  );
  if (entries.length === 0) return { name: "Unknown" };
  const [name, value] = entries[0];
  const normalized = value as NormalizedCapability;
  return { ...normalized, name: name.replace("Acl", "") };
}

export function extractScopeEntries(
  scope?: Record<string, unknown>
): Array<{ type: string; items: string[] }> {
  const entries: Array<{ type: string; items: string[] }> = [];
  if (!scope) return entries;

  const datasetScope = scope["datasetScope"] as
    | { ids?: number[] }
    | undefined;
  if (datasetScope?.ids?.length) {
    entries.push({
      type: "datasetScope.ids",
      items: datasetScope.ids.map((v) => String(v)).sort(),
    });
  }

  const idScope = scope["idScope"] as { ids?: number[] } | undefined;
  if (idScope?.ids?.length) {
    entries.push({
      type: "idScope.ids",
      items: idScope.ids.map((v) => String(v)).sort(),
    });
  }

  const spaceIdScope = scope["spaceIdScope"] as
    | { spaceIds?: string[] }
    | undefined;
  if (spaceIdScope?.spaceIds?.length) {
    entries.push({
      type: "spaceIdScope.spaceIds",
      items: [...spaceIdScope.spaceIds].sort(),
    });
  }

  const tableScope = scope["tableScope"] as
    | { dbsToTables?: Record<string, string[]> }
    | undefined;
  if (tableScope?.dbsToTables) {
    const tableKeys = Object.entries(tableScope.dbsToTables).flatMap(
      ([db, tables]) => {
        if (Array.isArray(tables))
          return tables.map((table) => `${db}.${table}`);
        if (typeof tables === "string") return [`${db}.${tables}`];
        return [];
      }
    );
    if (tableKeys.length > 0) {
      entries.push({ type: "tableScope.dbsToTables", items: tableKeys.sort() });
    }
  }
  return entries;
}

export function parsePythonVersion(runtime?: string) {
  if (!runtime) return null;
  const match = runtime.match(/py(\d{2,3})/i);
  if (!match) return null;
  const raw = match[1];
  if (raw.length === 2)
    return { major: Number(raw[0]), minor: Number(raw[1]) };
  if (raw.length === 3)
    return { major: Number(raw[0]), minor: Number(raw.slice(1)) };
  return null;
}

export function computePermissionScopeDrift(
  groups: GroupSummary[]
): Array<{
  id: string;
  summary: string;
  scopeType: string;
  leftGroup: string;
  rightGroup: string;
  common: string[];
  leftOnly: string[];
  rightOnly: string[];
}> {
  const findings: Array<{
    id: string;
    summary: string;
    scopeType: string;
    leftGroup: string;
    rightGroup: string;
    common: string[];
    leftOnly: string[];
    rightOnly: string[];
  }> = [];
  const seen = new Set<string>();
  const byCapability = new Map<
    string,
    Array<{ groupName: string; scopeType: string; items: string[] }>
  >();

  for (const group of groups) {
    const groupName = group.name ?? `Group ${group.id}`;
    for (const cap of group.capabilities ?? []) {
      const normalized = normalizeCapability(cap);
      const actions = (normalized.actions ?? []).slice().sort().join("|");
      const key = `${normalized.name}::${actions}`;
      for (const entry of extractScopeEntries(normalized.scope)) {
        const list = byCapability.get(key) ?? [];
        list.push({ groupName, scopeType: entry.type, items: entry.items });
        byCapability.set(key, list);
      }
    }
  }

  const meetsOverlapThreshold = (a: string[], b: string[]) => {
    const aSet = new Set(a);
    const bSet = new Set(b);
    let common = 0;
    for (const v of aSet) if (bSet.has(v)) common += 1;
    if (common < 3) return false;
    return common / Math.max(aSet.size, bSet.size) >= 0.8;
  };

  for (const [capabilityKey, entries] of byCapability.entries()) {
    const groupedByType = entries.reduce<Record<string, typeof entries>>(
      (acc, entry) => {
        acc[entry.scopeType] = acc[entry.scopeType] ?? [];
        acc[entry.scopeType].push(entry);
        return acc;
      },
      {}
    );

    for (const [scopeType, scopeEntries] of Object.entries(groupedByType)) {
      for (let i = 0; i < scopeEntries.length; i += 1) {
        for (let j = i + 1; j < scopeEntries.length; j += 1) {
          const left = scopeEntries[i];
          const right = scopeEntries[j];
          if (left.groupName === right.groupName) continue;
          if (left.items.join("|") === right.items.join("|")) continue;
          if (!meetsOverlapThreshold(left.items, right.items)) continue;

          const readableName = capabilityKey.split("::")[0];
          const actionSuffix = capabilityKey.split("::")[1];
          const leftSet = new Set(left.items);
          const rightSet = new Set(right.items);
          const common = left.items.filter((v) => rightSet.has(v));
          const leftOnly = left.items.filter((v) => !rightSet.has(v));
          const rightOnly = right.items.filter((v) => !leftSet.has(v));
          const summary = `${readableName} [${actionSuffix || "no actions"}] · ${scopeType}: ${left.groupName} ↔ ${right.groupName}`;
          const id = [
            capabilityKey,
            scopeType,
            left.groupName,
            right.groupName,
            left.items.join("|"),
            right.items.join("|"),
          ].join("::");
          if (seen.has(id)) continue;
          seen.add(id);
          findings.push({
            id,
            summary,
            scopeType,
            leftGroup: left.groupName,
            rightGroup: right.groupName,
            common,
            leftOnly,
            rightOnly,
          });
        }
      }
    }
  }
  return findings;
}

export function classifyCompliantGroups(
  groups: GroupSummary[],
  driftEntries: PermissionScopeDriftEntry[]
): CompliantGroupEntry[] {
  const groupsWithDrift = new Set<string>();
  for (const d of driftEntries) {
    groupsWithDrift.add(d.leftGroup);
    groupsWithDrift.add(d.rightGroup);
  }

  const allScopeEntries = new Map<
    string,
    Array<{ capKey: string; scopeType: string; items: string[] }>
  >();
  for (const group of groups) {
    const groupName = group.name ?? `Group ${group.id}`;
    const entries: Array<{ capKey: string; scopeType: string; items: string[] }> = [];
    for (const cap of group.capabilities ?? []) {
      const normalized = normalizeCapability(cap);
      const actions = (normalized.actions ?? []).slice().sort().join("|");
      const capKey = `${normalized.name}::${actions}`;
      for (const se of extractScopeEntries(normalized.scope)) {
        entries.push({ capKey, scopeType: se.type, items: se.items });
      }
    }
    allScopeEntries.set(groupName, entries);
  }

  const results: CompliantGroupEntry[] = [];

  for (const group of groups) {
    const groupName = group.name ?? `Group ${group.id}`;
    if (groupsWithDrift.has(groupName)) continue;

    const caps = group.capabilities ?? [];
    const capCount = caps.length;

    if (capCount === 0) {
      results.push({
        groupName,
        capabilityCount: 0,
        reason: "no_capabilities",
        label: "No capabilities defined",
        details: "This group has no capabilities configured.",
      });
      continue;
    }

    const hasAllScope = caps.some((cap) => {
      const normalized = normalizeCapability(cap);
      if (!normalized.scope) return true;
      const keys = Object.keys(normalized.scope);
      if (keys.length === 0) return true;
      if (keys.includes("all") || keys.includes("allScope")) return true;
      return false;
    });

    const myEntries = allScopeEntries.get(groupName) ?? [];

    if (myEntries.length === 0 && hasAllScope) {
      results.push({
        groupName,
        capabilityCount: capCount,
        reason: "all_scope",
        label: "No scoping, all permissions granted",
        details: `All ${capCount} capabilities use unrestricted (all) scope — nothing to compare.`,
      });
      continue;
    }

    if (myEntries.length === 0) {
      results.push({
        groupName,
        capabilityCount: capCount,
        reason: "no_scope_entries",
        label: "No comparable scope entries",
        details: "Capabilities have no dataset, space, table, or ID scoping that could be compared.",
      });
      continue;
    }

    let hasIdenticalMatch = false;
    let allBelowThreshold = true;
    let allUnique = true;

    for (const myEntry of myEntries) {
      for (const [otherName, otherEntries] of allScopeEntries) {
        if (otherName === groupName) continue;
        for (const other of otherEntries) {
          if (myEntry.capKey !== other.capKey) continue;
          if (myEntry.scopeType !== other.scopeType) continue;

          if (myEntry.items.join("|") === other.items.join("|")) {
            hasIdenticalMatch = true;
            allUnique = false;
            continue;
          }

          const mySet = new Set(myEntry.items);
          const otherSet = new Set(other.items);
          let common = 0;
          for (const v of mySet) if (otherSet.has(v)) common += 1;

          if (common > 0) allUnique = false;

          if (common >= 3) {
            allBelowThreshold = false;
            // overlap >= 80% would have been caught by drift detection
          }
        }
      }
    }

    if (allUnique) {
      results.push({
        groupName,
        capabilityCount: capCount,
        reason: "unique_scoping",
        label: "Completely unique scoping",
        details: `None of the ${myEntries.length} scope entries share any items with other groups.`,
      });
    } else if (hasIdenticalMatch && allBelowThreshold) {
      results.push({
        groupName,
        capabilityCount: capCount,
        reason: "identical_scoping",
        label: "Scoping matches other groups exactly",
        details: "Where scopes overlap with other groups, they are identical — no drift.",
      });
    } else if (allBelowThreshold) {
      results.push({
        groupName,
        capabilityCount: capCount,
        reason: "below_threshold",
        label: "Overlap below drift threshold",
        details:
          "Some scope entries share items with other groups, but fewer than 3 common entries or less than 80% overlap — not enough similarity to flag as drift.",
      });
    } else {
      results.push({
        groupName,
        capabilityCount: capCount,
        reason: "unique_scoping",
        label: "Scoping does not meet drift criteria",
        details: "Scope entries were compared but did not meet the drift detection threshold.",
      });
    }
  }

  results.sort((a, b) => a.groupName.localeCompare(b.groupName));
  return results;
}
