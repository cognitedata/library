import YAML from "yaml";
import type { PatternEntry, PatternsData, ScopeHierarchyData, ScopeNode } from "../types/assetConfig";
import { emptyPattern, emptyScopeNode } from "../types/assetConfig";

export const DEFAULT_CONFIG_REL = "default.config.yaml";
export const FILE_ASSET_SOURCE_KEY = "file_asset_source";

export type ConfigStep = "scope" | "extract";

export const CONFIG_STEPS: { id: ConfigStep; label: string }[] = [
  { id: "scope", label: "Scope" },
  { id: "extract", label: "Extract" },
];

type PipelineStep = "extract" | "create" | "write";

/** YAML block under ``file_asset_source`` for editor steps (``scope`` uses ``create`` data). */
export function yamlStepForEditor(step: ConfigStep): PipelineStep {
  if (step === "scope") return "create";
  return step;
}

const PIPELINE_STEPS: PipelineStep[] = ["extract", "create", "write"];

export function parseDefaultDocument(content: string): Record<string, unknown> {
  const doc = YAML.parse(content);
  if (doc == null) return {};
  if (typeof doc !== "object" || Array.isArray(doc)) {
    throw new Error("default.config.yaml root must be a mapping");
  }
  return doc as Record<string, unknown>;
}

export function stringifyDefaultDocument(doc: Record<string, unknown>): string {
  return YAML.stringify(doc, { lineWidth: 0 });
}

function fileAssetSource(doc: Record<string, unknown>): Record<string, unknown> {
  const fas = doc[FILE_ASSET_SOURCE_KEY];
  if (fas == null || typeof fas !== "object" || Array.isArray(fas)) {
    return {};
  }
  return fas as Record<string, unknown>;
}

function stepBlock(doc: Record<string, unknown>, step: PipelineStep): Record<string, unknown> {
  const block = fileAssetSource(doc)[step];
  if (block == null || typeof block !== "object" || Array.isArray(block)) {
    return { parameters: {}, data: {} };
  }
  return block as Record<string, unknown>;
}

function dataFromStepBlock(block: Record<string, unknown>): Record<string, unknown> {
  const data = block.data;
  if (data == null || typeof data !== "object" || Array.isArray(data)) {
    return {};
  }
  return data as Record<string, unknown>;
}

export function stepYamlFromDefault(content: string, step: ConfigStep): string {
  const doc = parseDefaultDocument(content);
  const yamlStep = yamlStepForEditor(step);
  return stringifyDefaultDocument(stepBlock(doc, yamlStep));
}

export function mergeStepYamlIntoDefault(
  content: string,
  step: ConfigStep,
  sliceYaml: string
): string {
  const doc = parseDefaultDocument(content);
  const slice = parseDefaultDocument(sliceYaml);
  const yamlStep = yamlStepForEditor(step);
  const fas = { ...fileAssetSource(doc) };
  fas[yamlStep] = slice;
  doc[FILE_ASSET_SOURCE_KEY] = fas;
  return stringifyDefaultDocument(doc);
}

function normalizeScopeNode(raw: unknown): ScopeNode {
  if (raw == null || typeof raw !== "object" || Array.isArray(raw)) {
    return emptyScopeNode();
  }
  const o = raw as Record<string, unknown>;
  const files = Array.isArray(o.files)
    ? o.files.map((f) => String(f).trim()).filter(Boolean)
    : [];
  const child = Array.isArray(o.locations)
    ? o.locations.map((c) => normalizeScopeNode(c))
    : [];
  return {
    name: o.name != null ? String(o.name) : "",
    description: o.description != null ? String(o.description) : "",
    locations: child,
    files,
  };
}

export function scopeFromStepYaml(content: string): ScopeHierarchyData {
  const block = parseDefaultDocument(content);
  const data = dataFromStepBlock(block);
  const levels = Array.isArray(data.hierarchy_levels)
    ? data.hierarchy_levels.map((l) => String(l).trim()).filter(Boolean)
    : [];
  const scopeRaw = Array.isArray(data.scope) ? data.scope : [];
  const scope = scopeRaw.map((n) => normalizeScopeNode(n));
  return { hierarchy_levels: levels, scope };
}

export function mergeScopeIntoStepYaml(
  content: string,
  hierarchy: ScopeHierarchyData
): string {
  const block = parseDefaultDocument(content);
  const data = { ...dataFromStepBlock(block) };
  data.hierarchy_levels = hierarchy.hierarchy_levels;
  data.scope = hierarchy.scope;
  block.data = data;
  return stringifyDefaultDocument(block);
}

function normalizePattern(raw: unknown): PatternEntry {
  if (raw == null || typeof raw !== "object" || Array.isArray(raw)) {
    return emptyPattern();
  }
  const o = raw as Record<string, unknown>;
  const samplesRaw = o.sample ?? o.samples;
  const sample = Array.isArray(samplesRaw)
    ? samplesRaw.map((s) => String(s)).filter(Boolean)
    : [];
  return {
    category: o.category != null ? String(o.category) : "general",
    resourceType: o.resourceType != null ? String(o.resourceType) : undefined,
    resourceSubType: o.resourceSubType != null ? String(o.resourceSubType) : undefined,
    standard: o.standard != null ? String(o.standard) : undefined,
    sample,
  };
}

export function patternsFromStepYaml(content: string): PatternsData {
  const block = parseDefaultDocument(content);
  const data = dataFromStepBlock(block);
  const patterns = Array.isArray(data.patterns)
    ? data.patterns.map((p) => normalizePattern(p))
    : [];
  return { patterns };
}

export function mergePatternsIntoStepYaml(content: string, data: PatternsData): string {
  const block = parseDefaultDocument(content);
  const section = { ...dataFromStepBlock(block) };
  section.patterns = data.patterns.map((p) => {
    const row: Record<string, unknown> = {
      category: p.category || "general",
      sample: p.sample,
    };
    if (p.resourceType) row.resourceType = p.resourceType;
    if (p.resourceSubType) row.resourceSubType = p.resourceSubType;
    if (p.standard) row.standard = p.standard;
    return row;
  });
  block.data = section;
  return stringifyDefaultDocument(block);
}

export function stepKind(step: ConfigStep): ConfigStep {
  return step;
}

export function listPipelineStepsInDoc(content: string): PipelineStep[] {
  const doc = parseDefaultDocument(content);
  const fas = fileAssetSource(doc);
  return PIPELINE_STEPS.filter((s) => fas[s] != null);
}
