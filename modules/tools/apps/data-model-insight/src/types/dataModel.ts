/**
 * Internal documentation model (aligned with NEAT doc generator).
 * Used for rendering view cards, property tables, and ER diagrams.
 */

export interface PropertyInfo {
  name: string;
  display_name?: string;
  description?: string;
  type: string;
  connection: string | null;
  min_count: string;
  max_count: string;
  container?: string;
  container_property?: string;
  container_property_name?: string;
  inherited_from?: string | null;
  true_source?: string;
}

export interface ViewInfo {
  name: string;
  display_name?: string;
  description?: string;
  implements: string;
  properties: PropertyInfo[];
  own_property_count: number;
  inherited_property_count: number;
}

export interface DirectRelation {
  source: string;
  property: string;
  display_name: string;
  target: string;
  min_count: string;
  max_count: string;
}

export interface DocModelMetadata {
  name: string;
  space: string;
  description: string;
  externalId?: string;
  version?: string;
}

export interface DocModel {
  metadata: DocModelMetadata;
  views: Record<string, ViewInfo>;
  all_views: Record<string, { name: string; display_name?: string; description?: string; implements: string }>;
  direct_relations: DirectRelation[];
  inheritance_depths?: Record<string, number>;
}

export interface CategoryLabel {
  icon: string;
  displayName: string;
  description: string;
}

export type CategoriesMap = Record<string, string[]>;
export type ViewDomainsMap = Record<
  string,
  { domain: string; classification_method: string; industry_standard?: string }
>;
