/**
 * Generate Mermaid classDiagram and flowchart source for doc model.
 * Multiple topic-based diagrams (Level 1–4) like the original NEAT Python generator.
 * Uses unique sanitized IDs and safe labels to avoid parse/syntax errors in Mermaid and HTML.
 */

import type { DocModel } from "@/types/dataModel";
import type { CategoriesMap } from "@/types/dataModel";
import type { CategoryLabel } from "@/types/dataModel";

/** Base sanitization: only safe chars for Mermaid node/class IDs (no leading digit). */
function sanitizeIdBase(s: string): string {
  const t = s.replace(/[^a-zA-Z0-9]/g, "_").replace(/^(\d)/, "_$1").slice(0, 28);
  return t || "n";
}

/** Build a map from viewId to unique Mermaid ID (no collisions). */
function buildUniqueIdMap(viewIds: string[]): Map<string, string> {
  const map = new Map<string, string>();
  const used = new Map<string, number>();
  for (const id of viewIds) {
    const base = sanitizeIdBase(id);
    const count = (used.get(base) ?? 0) + 1;
    used.set(base, count);
    map.set(id, count === 1 ? base : `${base}_${count}`);
  }
  return map;
}

/** Safe relation label for Mermaid: only safe chars so syntax never breaks. */
function safeRelationLabel(s: string): string {
  return (String(s ?? "")
    .replace(/[^\w\s\-.]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 36) || "rel");
}

const FLOWCHART_COLORS = ["#059669", "#b45309", "#2563eb", "#7c3aed", "#db2777", "#0891b2"];

/** Max views in Level 1 overview so Mermaid can layout a readable classDiagram */
const LEVEL1_CAP = 50;

/** Level 1 overview: UML classDiagram with inheritance and relations (like NEAT). Ensures edges so layout is vertical/readable. */
export function buildOverviewMermaid(doc: DocModel, categoryByViewId?: Record<string, string>): string {
  const viewIds = Object.keys(doc.views);
  const withEdges = new Set<string>();
  for (const rel of doc.direct_relations) {
    if (viewIds.includes(rel.source)) withEdges.add(rel.source);
    if (viewIds.includes(rel.target)) withEdges.add(rel.target);
  }
  for (const id of viewIds) {
    const v = doc.views[id];
    if (v?.implements?.trim()) withEdges.add(id);
    for (const p of (v?.implements?.split(",").map((s) => s.trim()).filter(Boolean) ?? [])) {
      if (viewIds.includes(p)) withEdges.add(p);
    }
  }
  let level1Ids = Array.from(withEdges);
  const rest = viewIds.filter((id) => !withEdges.has(id));
  if (level1Ids.length + rest.length > LEVEL1_CAP) {
    level1Ids = [...level1Ids, ...rest].slice(0, LEVEL1_CAP);
  } else {
    level1Ids = [...level1Ids, ...rest];
  }
  if (level1Ids.length === 0) level1Ids = viewIds.slice(0, LEVEL1_CAP);
  return buildUmlClassDiagram(doc, level1Ids, categoryByViewId);
}

/** Flowchart for view IDs: relations + inheritance edges. Uses unique IDs to avoid syntax errors. */
export function buildFlowchartForViewIds(doc: DocModel, viewIds: string[]): string {
  const set = new Set(viewIds);
  const idMap = buildUniqueIdMap(viewIds);
  const lines: string[] = ["flowchart TB"];
  const added = new Set<string>();
  for (const rel of doc.direct_relations) {
    if (!set.has(rel.source) || !set.has(rel.target)) continue;
    const src = idMap.get(rel.source)!;
    const tgt = idMap.get(rel.target)!;
    if (added.has(`${src}-${tgt}`)) continue;
    added.add(`${src}-${tgt}`);
    lines.push(`    ${src} --> ${tgt}`);
  }
  for (const id of viewIds) {
    const view = doc.views[id];
    const parents = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    for (const p of parents) {
      if (!set.has(p)) continue;
      const child = idMap.get(id)!;
      const parent = idMap.get(p)!;
      if (added.has(`${child}-${parent}`)) continue;
      added.add(`${child}-${parent}`);
      lines.push(`    ${child} --> ${parent}`);
    }
  }
  for (const id of viewIds) {
    const sid = idMap.get(id)!;
    if (!lines.some((l) => l.includes(sid))) lines.push(`    ${sid}`);
  }
  for (let i = 0; i < viewIds.length; i++) {
    const sid = idMap.get(viewIds[i])!;
    const color = FLOWCHART_COLORS[i % FLOWCHART_COLORS.length];
    lines.push(`    style ${sid} fill:${color},stroke:#1e293b,color:#fff`);
  }
  return lines.filter((l) => l.length > 0).join("\n").trim();
}

/** UML-style class diagram: classes as boxes only, inheritance (--|>) and relations (-->). Unique IDs and safe labels. */
export function buildUmlClassDiagram(
  doc: DocModel,
  viewIds: string[],
  categoryByViewId?: Record<string, string>
): string {
  const set = new Set(viewIds);
  const idMap = buildUniqueIdMap(viewIds);
  const lines: string[] = ["classDiagram"];

  // Use "other" not "default" - "default" is reserved in Mermaid and applies to all nodes (breaks parser in v11)
  const colorByCategory: Record<string, { fill: string; stroke: string }> = {
    location_geography: { fill: "#059669", stroke: "#047857" },
    wells_completions: { fill: "#0e7490", stroke: "#0c6b7a" },
    rotating_equipment: { fill: "#b45309", stroke: "#92400e" },
    static_equipment: { fill: "#7c3aed", stroke: "#6d28d9" },
    electrical_equipment: { fill: "#eab308", stroke: "#ca8a04" },
    instrumentation_control: { fill: "#0891b2", stroke: "#0e7490" },
    timeseries_measurements: { fill: "#db2777", stroke: "#be185d" },
    activities_work: { fill: "#65a30d", stroke: "#4d7c0f" },
    documents_files: { fill: "#2563eb", stroke: "#1d4ed8" },
    reference_classification: { fill: "#4b5563", stroke: "#374151" },
    cdm_core: { fill: "#1e3a5f", stroke: "#1e293b" },
    cdm_features: { fill: "#334155", stroke: "#1e293b" },
    default: { fill: "#64748b", stroke: "#475569" },
  };

  const usedStyles = new Set<string>();
  for (const viewId of viewIds) {
    const cid = idMap.get(viewId)!;
    lines.push(`    class ${cid}`);
    const cat = categoryByViewId?.[viewId] ?? "default";
    usedStyles.add(cat);
  }

  const toStyleName = (cat: string) => (cat === "default" ? "other" : (cat.replace(/_/g, "") || "other").replace(/^\d/, "c"));
  for (const [cat, colors] of Object.entries(colorByCategory)) {
    if (!usedStyles.has(cat)) continue;
    lines.push(`    classDef ${toStyleName(cat)} fill:${colors.fill},stroke:${colors.stroke},color:#fff`);
  }

  const byStyle = new Map<string, string[]>();
  for (const viewId of viewIds) {
    const cid = idMap.get(viewId)!;
    const cat = categoryByViewId?.[viewId] ?? "default";
    const styleName = toStyleName(cat);
    if (!byStyle.has(styleName)) byStyle.set(styleName, []);
    byStyle.get(styleName)!.push(cid);
  }
  for (const [styleName, ids] of byStyle) {
    if (ids.length > 0) lines.push(`    class ${ids.join(",")} ${styleName}`);
  }

  const inhSet = new Set<string>();
  for (const viewId of viewIds) {
    const view = doc.views[viewId];
    if (!view?.implements) continue;
    const parents = view.implements.split(",").map((p) => p.trim()).filter(Boolean);
    const childId = idMap.get(viewId)!;
    for (const parent of parents) {
      if (!set.has(parent)) continue;
      const parentId = idMap.get(parent)!;
      const key = `${childId}--|>${parentId}`;
      if (inhSet.has(key)) continue;
      inhSet.add(key);
      lines.push(`    ${childId} --|> ${parentId}`);
    }
  }

  const relSet = new Set<string>();
  for (const rel of doc.direct_relations) {
    if (!set.has(rel.source) || !set.has(rel.target)) continue;
    const key = `${rel.source}->${rel.target}:${rel.property}`;
    if (relSet.has(key)) continue;
    relSet.add(key);
    const src = idMap.get(rel.source)!;
    const tgt = idMap.get(rel.target)!;
    const label = safeRelationLabel(rel.display_name || rel.property);
    const quoted = `"${label.replace(/"/g, "''")}"`;
    lines.push(`    ${src} --> ${tgt} : ${quoted}`);
  }
  return lines.filter((l) => l.length > 0).join("\n").trim();
}

export function buildClassDiagramMermaid(doc: DocModel, viewIds: string[]): string {
  return buildUmlClassDiagram(doc, viewIds);
}

export interface DiagramSpec {
  title: string;
  description: string;
  source: string;
  accent: "amber" | "emerald" | "blue" | "violet" | "rose" | "cyan";
}

const LEVEL2_CAP = 50;
const LEVEL3_CAP = 35;

/** Build multiple topic-based ER diagrams (Level 1–4, by category). All diagrams are always generated (with caps). */
export function getDiagramSpecs(
  doc: DocModel,
  categories: CategoriesMap,
  categoryLabels: Record<string, CategoryLabel>
): DiagramSpec[] {
  const viewIds = Object.keys(doc.views);
  const specs: DiagramSpec[] = [];
  const accents: DiagramSpec["accent"][] = ["amber", "emerald", "blue", "violet", "rose", "cyan"];
  let accIdx = 0;
  const nextAccent = () => accents[accIdx++ % accents.length];

  const categoryByViewId: Record<string, string> = {};
  for (const [catId, ids] of Object.entries(categories)) {
    for (const id of ids ?? []) categoryByViewId[id] = catId;
  }

  const categoryOrder = [
    "location_geography",
    "wells_completions",
    "rotating_equipment",
    "static_equipment",
    "electrical_equipment",
    "instrumentation_control",
    "timeseries_measurements",
    "activities_work",
    "documents_files",
    "reference_classification",
    "cdm_core",
    "cdm_features",
    "default",
  ];
  const orderedCatIds = [
    ...categoryOrder.filter((c) => categories[c]?.length),
    ...Object.keys(categories).filter((c) => !categoryOrder.includes(c)),
  ];

  const modelName = doc.metadata.name ?? "Model";

  specs.push({
    title: "Level 1: Model overview",
    description: `${modelName} – main structure and relationships (inheritance + relations)`,
    source: buildOverviewMermaid(doc, categoryByViewId),
    accent: nextAccent(),
  });

  const assetLike = viewIds.filter(
    (id) =>
      /asset|Asset/.test(id) ||
      (doc.views[id]?.implements?.toLowerCase().includes("asset") ?? false)
  );
  if (assetLike.length > 0) {
    specs.push({
      title: "Level 2: Asset-type hierarchy",
      description: "Views extending or relating to CogniteAsset",
      source: buildFlowchartForViewIds(doc, assetLike.slice(0, LEVEL2_CAP)),
      accent: nextAccent(),
    });
  }

  const equipmentLike = viewIds.filter(
    (id) =>
      /equipment|Equipment|pump|compressor|motor/.test(id) ||
      (doc.views[id]?.implements?.toLowerCase().includes("equipment") ?? false)
  );
  if (equipmentLike.length > 0) {
    specs.push({
      title: "Level 2: Equipment-type hierarchy",
      description: "Views extending or relating to equipment",
      source: buildFlowchartForViewIds(doc, equipmentLike.slice(0, LEVEL2_CAP)),
      accent: nextAccent(),
    });
  }

  for (const catId of orderedCatIds) {
    const ids = categories[catId];
    if (!ids || ids.length === 0) continue;
    const label = categoryLabels[catId];
    specs.push({
      title: `Level 3: ${label?.displayName ?? catId.replace(/_/g, " ")}`,
      description: label?.description ?? `Views in ${catId}`,
      source: buildUmlClassDiagram(doc, ids.slice(0, LEVEL3_CAP), categoryByViewId),
      accent: nextAccent(),
    });
  }

  const sample = viewIds.slice(0, 25);
  if (sample.length > 0) {
    specs.push({
      title: "Level 4: Class diagram (sample)",
      description: "UML-style: inheritance and relations, colored by domain",
      source: buildUmlClassDiagram(doc, sample, categoryByViewId),
      accent: nextAccent(),
    });
  }

  return specs;
}
