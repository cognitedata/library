import type { JsonObject } from "../types/jsonConfig";

export type PolicyRow = {
  property: string;
  strategy: "tie_break" | "merge_list";
  merge_unique: boolean;
  branch_order: "by_score" | "by_dependency";
};

export function defaultPolicyRow(): PolicyRow {
  return {
    property: "",
    strategy: "tie_break",
    merge_unique: true,
    branch_order: "by_score",
  };
}

export function rowsFromConfig(policies: unknown): PolicyRow[] {
  if (!Array.isArray(policies) || policies.length === 0) return [];
  const out: PolicyRow[] = [];
  for (const item of policies) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const o = item as Record<string, unknown>;
    const property = String(o.property ?? "").trim();
    if (!property) continue;
    const strat = String(o.strategy ?? "tie_break").trim();
    const strategy: PolicyRow["strategy"] = strat === "merge_list" ? "merge_list" : "tie_break";
    const ml =
      o.merge_list && typeof o.merge_list === "object" && !Array.isArray(o.merge_list)
        ? (o.merge_list as Record<string, unknown>)
        : {};
    const branch = String(ml.branch_order ?? "by_score").trim();
    out.push({
      property,
      strategy,
      merge_unique: ml.unique !== false,
      branch_order: branch === "by_dependency" ? "by_dependency" : "by_score",
    });
  }
  return out;
}

export function policiesToConfig(rows: PolicyRow[]): JsonObject[] {
  return rows.map((r) => {
    if (r.strategy === "merge_list") {
      return {
        property: r.property.trim(),
        strategy: "merge_list",
        merge_list: {
          unique: r.merge_unique,
          branch_order: r.branch_order,
        },
      };
    }
    return { property: r.property.trim(), strategy: "tie_break" };
  });
}

function parsePolicyRowFromJsonEntry(entry: unknown): PolicyRow | null {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
  const o = entry as Record<string, unknown>;
  const property = String(o.property ?? "").trim();
  if (!property) return null;
  const stratRaw = String(o.strategy ?? "tie_break").trim() || "tie_break";
  if (stratRaw !== "tie_break" && stratRaw !== "merge_list") return null;
  if (stratRaw === "tie_break") {
    return { property, strategy: "tie_break", merge_unique: false, branch_order: "by_score" };
  }
  const ml =
    o.merge_list && typeof o.merge_list === "object" && !Array.isArray(o.merge_list)
      ? (o.merge_list as Record<string, unknown>)
      : {};
  const bo = String(ml.branch_order ?? "by_score").trim() || "by_score";
  if (bo !== "by_score" && bo !== "by_dependency") return null;
  return {
    property,
    strategy: "merge_list",
    merge_unique: Boolean(ml.unique),
    branch_order: bo,
  };
}

export function parsePoliciesJson(raw: string): PolicyRow[] | null {
  const trimmed = raw.trim();
  if (!trimmed) return [];
  try {
    const v = JSON.parse(trimmed) as unknown;
    if (!Array.isArray(v)) return null;
    const rows: PolicyRow[] = [];
    for (const item of v) {
      const row = parsePolicyRowFromJsonEntry(item);
      if (!row) return null;
      rows.push(row);
    }
    return rows;
  } catch {
    return null;
  }
}
