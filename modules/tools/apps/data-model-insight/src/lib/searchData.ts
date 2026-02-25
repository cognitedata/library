/**
 * Build search index for dual search (View Types vs Properties) like the original NEAT doc.
 */

import type { DocModel } from "@/types/dataModel";
import type { ViewDomainsMap } from "@/types/dataModel";
import type { CategoryLabel } from "@/types/dataModel";

export interface ViewSearchHit {
  viewId: string;
  displayName: string;
  description: string;
  domain: string;
  domainDisplay: string;
}

export interface PropertySearchHit {
  viewId: string;
  viewDisplayName: string;
  viewProperty: string;
  viewPropertyName: string;
  containerProperty: string;
  containerPropertyName: string;
  type: string;
  typeDisplay: string;
  isRelation: boolean;
  inherited: boolean;
}

export interface SearchDataByView {
  displayName: string;
  description: string;
  domain: string;
  domainDisplay: string;
  properties: {
    viewProperty: string;
    viewPropertyName: string;
    containerProperty: string;
    containerPropertyName: string;
    type: string;
    typeDisplay: string;
    isRelation: boolean;
    inherited: boolean;
  }[];
}

export function buildSearchData(
  doc: DocModel,
  viewDomains: ViewDomainsMap,
  categoryLabels: Record<string, CategoryLabel>
): Record<string, SearchDataByView> {
  const out: Record<string, SearchDataByView> = {};
  for (const [viewId, view] of Object.entries(doc.views)) {
    const domain = viewDomains[viewId]?.domain ?? "default";
    const domainDisplay = categoryLabels[domain]?.displayName ?? domain;
    out[viewId] = {
      displayName: view.display_name ?? view.name,
      description: view.description ?? "",
      domain,
      domainDisplay,
      properties: view.properties.map((p) => ({
        viewProperty: p.name,
        viewPropertyName: p.display_name ?? "",
        containerProperty: p.container_property ?? "",
        containerPropertyName: p.container_property_name ?? "",
        type: p.type,
        typeDisplay: doc.all_views[p.type]?.display_name ?? p.type,
        isRelation: p.connection === "direct" || (p.connection?.length ?? 0) > 0,
        inherited: !!p.inherited_from,
      })),
    };
  }
  return out;
}

export function searchViews(
  searchData: Record<string, SearchDataByView>,
  query: string,
  filterByProperty?: string
): ViewSearchHit[] {
  const q = query.trim().toLowerCase();
  const filterByPropLower = filterByProperty?.trim().toLowerCase();
  const results: ViewSearchHit[] = [];
  for (const [viewId, data] of Object.entries(searchData)) {
    if (filterByPropLower) {
      const hasProp = data.properties.some(
        (p) =>
          p.viewProperty.toLowerCase().includes(filterByPropLower) ||
          p.viewPropertyName.toLowerCase().includes(filterByPropLower) ||
          p.containerProperty.toLowerCase().includes(filterByPropLower) ||
          p.containerPropertyName.toLowerCase().includes(filterByPropLower)
      );
      if (!hasProp) continue;
    }
    if (!q) {
      results.push({
        viewId,
        displayName: data.displayName,
        description: data.description,
        domain: data.domain,
        domainDisplay: data.domainDisplay,
      });
      continue;
    }
    const match =
      data.displayName.toLowerCase().includes(q) ||
      viewId.toLowerCase().includes(q) ||
      data.description.toLowerCase().includes(q) ||
      data.domainDisplay.toLowerCase().includes(q);
    if (match) {
      results.push({
        viewId,
        displayName: data.displayName,
        description: data.description,
        domain: data.domain,
        domainDisplay: data.domainDisplay,
      });
    }
  }
  return results;
}

export function searchProperties(
  searchData: Record<string, SearchDataByView>,
  query: string,
  filterByViewId?: string,
  filterByViewIds?: Set<string>
): PropertySearchHit[] {
  const q = query.trim().toLowerCase();
  const results: PropertySearchHit[] = [];
  const viewIdSet = filterByViewIds ? filterByViewIds : filterByViewId ? new Set([filterByViewId]) : undefined;
  for (const [viewId, data] of Object.entries(searchData)) {
    if (viewIdSet && !viewIdSet.has(viewId)) continue;
    for (const p of data.properties) {
      if (!q) {
        results.push({
          viewId,
          viewDisplayName: data.displayName,
          viewProperty: p.viewProperty,
          viewPropertyName: p.viewPropertyName,
          containerProperty: p.containerProperty,
          containerPropertyName: p.containerPropertyName,
          type: p.type,
          typeDisplay: p.typeDisplay,
          isRelation: p.isRelation,
          inherited: p.inherited,
        });
        continue;
      }
      const match =
        p.viewProperty.toLowerCase().includes(q) ||
        p.viewPropertyName.toLowerCase().includes(q) ||
        p.containerProperty.toLowerCase().includes(q) ||
        p.containerPropertyName.toLowerCase().includes(q) ||
        p.type.toLowerCase().includes(q) ||
        p.typeDisplay.toLowerCase().includes(q);
      if (match) {
        results.push({
          viewId,
          viewDisplayName: data.displayName,
          viewProperty: p.viewProperty,
          viewPropertyName: p.viewPropertyName,
          containerProperty: p.containerProperty,
          containerPropertyName: p.containerPropertyName,
          type: p.type,
          typeDisplay: p.typeDisplay,
          isRelation: p.isRelation,
          inherited: p.inherited,
        });
      }
    }
  }
  return results;
}
