/**
 * Transform CDF Data Model (with inline ViewDefinitions) to our DocModel
 * for documentation rendering. Mirrors NEAT YAML structure.
 */

import type { DataModel } from "@cognite/sdk";
import type {
  DocModel,
  ViewInfo,
  PropertyInfo,
  DirectRelation,
  DocModelMetadata,
  CategoriesMap,
  CategoryLabel,
  ViewDomainsMap,
} from "@/types/dataModel";

interface ViewItem {
  externalId?: string;
  name?: string;
  description?: string;
  implements?: Array<{ externalId?: string; space?: string }>;
  properties?: Record<string, unknown>;
}

function isViewDefinition(v: ViewItem): v is ViewItem & { properties: Record<string, unknown> } {
  return v != null && "properties" in v && typeof (v as { properties?: unknown }).properties === "object";
}

function viewId(v: ViewItem): string {
  return v.externalId ?? "";
}

function getImplementsStr(v: ViewItem): string {
  const impl = v.implements;
  if (!Array.isArray(impl)) return "";
  return impl.map((i) => i.externalId ?? "").filter(Boolean).join(", ");
}

const PRIMITIVE_TYPES = ["int32", "int64", "float32", "float64", "boolean", "timestamp", "date", "json", "text"];

/** Map API type aliases to canonical primitive so we store consistent types for UI coloring. */
const TYPE_ALIASES: Record<string, string> = {
  float: "float64",
  double: "float64",
  number: "float64",
  int: "int64",
  integer: "int64",
  long: "int64",
  bool: "boolean",
};

function normalizeStoredType(s: string): string {
  const lower = s.toLowerCase();
  if (PRIMITIVE_TYPES.includes(lower)) return lower;
  if (TYPE_ALIASES[lower] != null) return TYPE_ALIASES[lower];
  return s;
}

function propTypeToString(t: unknown): string {
  if (t == null) return "text";
  if (typeof t === "string") {
    if (t === "direct") return "direct";
    return normalizeStoredType(t);
  }
  if (typeof t === "object" && t !== null && "type" in t) {
    const tt = (t as { type: string }).type;
    if (typeof tt !== "string") return "text";
    if (tt === "direct") return "direct";
    return normalizeStoredType(tt);
  }
  return "text";
}

function isDirectRelation(prop: { type?: string | { type?: string; source?: { externalId?: string } } }): boolean {
  const t = prop.type;
  if (t === "direct") return true;
  if (typeof t === "object" && t !== null && (t as { type?: string }).type === "direct") return true;
  return false;
}

function getDirectRelationTarget(prop: {
  type?: string | { type?: string; source?: { externalId?: string } };
  source?: { externalId?: string };
}): string {
  const t = prop.type;
  if (typeof t === "object" && t !== null && "source" in t) return (t as { source?: { externalId?: string } }).source?.externalId ?? "node";
  return prop.source?.externalId ?? "node";
}

const DOMAIN_CATEGORIES: Record<string, { icon: string; displayName: string; description: string }> = {
  location_geography: { icon: "🌍", displayName: "Location & Geography", description: "Sites, facilities, areas" },
  wells_completions: { icon: "🛢️", displayName: "Wells & Completions", description: "Wells, wellbores, completions" },
  rotating_equipment: { icon: "🔄", displayName: "Rotating Equipment", description: "Pumps, compressors, turbines" },
  static_equipment: { icon: "⚗️", displayName: "Static Equipment", description: "Vessels, tanks, separators" },
  electrical_equipment: { icon: "⚡", displayName: "Electrical Equipment", description: "Transformers, switchgear" },
  instrumentation_control: { icon: "📊", displayName: "Instrumentation & Control", description: "Sensors, meters" },
  timeseries_measurements: { icon: "📈", displayName: "Time Series & Measurements", description: "Time series, signals" },
  activities_work: { icon: "📋", displayName: "Activities & Work", description: "Maintenance, work orders" },
  documents_files: { icon: "📄", displayName: "Documents & Files", description: "Documents, drawings" },
  reference_classification: { icon: "📚", displayName: "Reference & Classification", description: "Types, codes" },
  cdm_core: { icon: "🔷", displayName: "CDM Core", description: "Cognite Core Data Model" },
  cdm_features: { icon: "🧩", displayName: "CDM Features", description: "Reusable CDM patterns" },
  default: { icon: "📦", displayName: "Other", description: "Other views" },
};

function classifyDomain(viewId: string, viewInfo: { name?: string; description?: string }): string {
  const combined = `${viewId} ${viewInfo.name ?? ""} ${viewInfo.description ?? ""}`.toLowerCase();
  if (viewId.startsWith("Cognite")) {
    if (combined.includes("describable") || combined.includes("sourceable") || combined.includes("visualizable"))
      return "cdm_features";
    return "cdm_core";
  }
  if (/\b(well|wellbore|wellhead|completion)\b/.test(combined)) return "wells_completions";
  if (/\b(site|plant|facility|area|location|platform)\b/.test(combined)) return "location_geography";
  if (/\b(pump|compressor|turbine|motor|generator)\b/.test(combined)) return "rotating_equipment";
  if (/\b(vessel|tank|separator|exchanger|filter|column)\b/.test(combined)) return "static_equipment";
  if (/\b(transformer|switchgear|vsd|vfd|ups|battery|electrical)\b/.test(combined)) return "electrical_equipment";
  if (/\b(instrument|sensor|meter|transmitter|gauge)\b/.test(combined)) return "instrumentation_control";
  if (/\b(timeseries|time series|forecast|measurement)\b/.test(combined)) return "timeseries_measurements";
  if (/\b(activity|work order|maintenance|inspection|event)\b/.test(combined)) return "activities_work";
  if (/\b(document|file|drawing|revision)\b/.test(combined)) return "documents_files";
  if (/\b(type|class|category|code|status)\b/.test(combined)) return "reference_classification";
  return "default";
}

function getInheritanceDepth(
  viewId: string,
  allViews: Record<string, { implements: string }>,
  cache: Record<string, number>
): number {
  if (cache[viewId] !== undefined) return cache[viewId];
  const view = allViews[viewId];
  if (!view?.implements) {
    cache[viewId] = 0;
    return 0;
  }
  const parents = view.implements.split(",").map((p) => p.trim()).filter(Boolean);
  let maxDepth = 0;
  for (const p of parents) {
    if (allViews[p]) maxDepth = Math.max(maxDepth, getInheritanceDepth(p, allViews, cache) + 1);
  }
  cache[viewId] = maxDepth;
  return maxDepth;
}

function getAllPropertiesForView(
  viewId: string,
  propertiesByView: Record<string, PropertyInfo[]>,
  allViews: Record<string, { implements: string }>
): PropertyInfo[] {
  const own = propertiesByView[viewId] ?? [];
  const view = allViews[viewId];
  if (!view?.implements) return own.map((p) => ({ ...p, inherited_from: null as string | null }));

  const inherited = new Map<string, PropertyInfo>();
  for (const p of own) {
    inherited.set(p.name, { ...p, inherited_from: null });
  }
  for (const parent of view.implements.split(",").map((p) => p.trim()).filter(Boolean)) {
    if (!parent || !allViews[parent]) continue;
    const parentProps = getAllPropertiesForView(parent, propertiesByView, allViews);
    for (const p of parentProps) {
      if (!inherited.has(p.name)) {
        inherited.set(p.name, { ...p, inherited_from: p.inherited_from ?? parent });
      }
    }
  }
  return Array.from(inherited.values());
}

/** Extract primitive type string from container property def (API may return type as string or nested object). */
function getContainerPropTypeString(def: { type?: unknown }): string | null {
  const t = def.type;
  if (t === "direct") return null;
  const str = typeof t === "string" ? t : (typeof t === "object" && t !== null && "type" in t && typeof (t as { type: unknown }).type === "string" ? (t as { type: string }).type : null);
  if (str == null) return null;
  if (str === "text" || str === "boolean" || str === "float32" || str === "float64" || str === "int32" || str === "int64" || str === "timestamp" || str === "date" || str === "json")
    return normalizeStoredType(str);
  return null;
}

/** Resolve scalar type from container property definition. Returns null for direct relations or if not found. */
function getContainerPropertyValueType(
  containersByKey: Record<string, { properties: Record<string, { type?: unknown }> }>,
  space: string,
  containerExternalId: string,
  containerPropertyIdentifier: string
): string | null {
  const key = `${space}|${containerExternalId}`;
  const container = containersByKey[key];
  if (!container?.properties) return null;
  const def = container.properties[containerPropertyIdentifier];
  if (!def || typeof def !== "object") return null;
  return getContainerPropTypeString(def);
}

export function cdfDataModelToDocModel(
  dm: DataModel,
  containersByKey: Record<string, { properties: Record<string, { type?: string }> }> = {}
): DocModel {
  const metadata: DocModelMetadata = {
    name: dm.name ?? dm.externalId,
    space: dm.space,
    description: dm.description ?? "",
    externalId: dm.externalId,
    version: dm.version,
  };

  const rawViews: ViewItem[] = (dm.views ?? []) as ViewItem[];
  const all_views: DocModel["all_views"] = {};
  const propertiesByView: Record<string, PropertyInfo[]> = {};
  const direct_relations: DirectRelation[] = [];

  for (const v of rawViews) {
    const id = viewId(v);
    if (!id) continue;
    all_views[id] = {
      name: id,
      display_name: v.name,
      description: v.description,
      implements: getImplementsStr(v),
    };

    if (!isViewDefinition(v)) continue;

    const props: PropertyInfo[] = [];
    for (const [propKey, propVal] of Object.entries(v.properties ?? {})) {
      if (propVal == null || typeof propVal !== "object") continue;
      const p = propVal as {
        type?: string | { type?: string; source?: { externalId?: string } };
        name?: string;
        description?: string;
        container?: { externalId?: string; space?: string };
        containerPropertyIdentifier?: string;
        list?: boolean;
        maxListSize?: number;
        source?: { externalId?: string };
      };
      const containerId = p.container?.externalId ?? "";
      const space = p.container?.space ?? dm.space ?? "";
      const containerPropId = p.containerPropertyIdentifier ?? "";

      let typeStr: string;
      if (isDirectRelation(p)) {
        typeStr = getDirectRelationTarget(p);
      } else if (containerId && containerPropId && space) {
        const containerType = getContainerPropertyValueType(containersByKey, space, containerId, containerPropId);
        typeStr = containerType ?? propTypeToString(propVal);
      } else {
        typeStr = propTypeToString(propVal);
      }
      const connection = isDirectRelation(p) ? "direct" : null;
      // Direct relations: cardinality is typically unknown (e.g. reverse side); use null
      const minCount = connection === "direct" ? "null" : "0";
      const maxCount = connection === "direct" ? "null" : (p.list ? String(p.maxListSize ?? 1000) : "1");

      props.push({
        name: propKey,
        display_name: p.name,
        description: p.description,
        type: typeStr,
        connection,
        min_count: minCount,
        max_count: maxCount,
        container: containerId || undefined,
        container_property: p.containerPropertyIdentifier,
        container_property_name: p.name,
        true_source: containerId || id,
      });

      if (connection === "direct" && typeStr && typeStr !== "node") {
        direct_relations.push({
          source: id,
          property: propKey,
          display_name: p.name ?? propKey,
          target: typeStr,
          min_count: minCount,
          max_count: maxCount,
        });
      }
    }
    propertiesByView[id] = props;
  }

  const inheritance_depths: Record<string, number> = {};
  for (const id of Object.keys(all_views)) {
    getInheritanceDepth(id, all_views, inheritance_depths);
  }

  const views: Record<string, ViewInfo> = {};
  for (const id of Object.keys(all_views)) {
    const fullProps = getAllPropertiesForView(id, propertiesByView, all_views);
    const ownCount = fullProps.filter((p) => !p.inherited_from).length;
    views[id] = {
      ...all_views[id],
      name: id,
      implements: all_views[id].implements,
      properties: fullProps,
      own_property_count: ownCount,
      inherited_property_count: fullProps.length - ownCount,
    };
  }

  return {
    metadata,
    views,
    all_views,
    direct_relations,
    inheritance_depths: inheritance_depths as Record<string, number>,
  };
}

export function getCategoriesAndLabels(doc: DocModel): {
  categories: CategoriesMap;
  categoryLabels: Record<string, CategoryLabel>;
  viewDomains: ViewDomainsMap;
} {
  const categories: CategoriesMap = {};
  const categoryLabels: Record<string, CategoryLabel> = {};
  const viewDomains: ViewDomainsMap = {};

  for (const [id, viewInfo] of Object.entries(doc.views)) {
    const domain = classifyDomain(id, viewInfo);
    viewDomains[id] = { domain, classification_method: "pattern" };
    if (!categories[domain]) categories[domain] = [];
    categories[domain].push(id);
    if (!categoryLabels[domain] && DOMAIN_CATEGORIES[domain]) {
      categoryLabels[domain] = {
        icon: DOMAIN_CATEGORIES[domain].icon,
        displayName: DOMAIN_CATEGORIES[domain].displayName,
        description: DOMAIN_CATEGORIES[domain].description,
      };
    }
  }
  for (const domain of Object.keys(categories)) {
    if (!categoryLabels[domain]) {
      categoryLabels[domain] = {
        icon: DOMAIN_CATEGORIES.default.icon,
        displayName: domain.replace(/_/g, " "),
        description: "",
      };
    }
  }
  return { categories, categoryLabels, viewDomains };
}
