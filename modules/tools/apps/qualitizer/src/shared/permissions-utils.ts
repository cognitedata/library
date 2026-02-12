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

export function getCapability(
  group: GroupSummary,
  name: string
): Record<string, unknown> | undefined {
  return group.capabilities.find((cap) => normalizeCapability(cap).name === name);
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
