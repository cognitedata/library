import type { MessageKey } from "../i18n";

export type JsonMappingTemplateId =
  | "passthrough"
  | "upstreamOutput"
  | "pickField"
  | "diagramDetectToDm"
  | "diagramDetectToClassic";

export type JsonMappingMapperKind =
  | "custom"
  | "diagram_detect_to_dm"
  | "diagram_detect_to_classic";

export type JsonMappingTemplate = {
  id: JsonMappingTemplateId;
  labelKey: MessageKey;
  input: Record<string, unknown>;
  expression: string;
};

export const JSON_MAPPING_TEMPLATES: readonly JsonMappingTemplate[] = [
  {
    id: "passthrough",
    labelKey: "transform.jsonMapping.templatePassthrough",
    input: {},
    expression: "input",
  },
  {
    id: "upstreamOutput",
    labelKey: "transform.jsonMapping.templateUpstreamOutput",
    input: { data: "${UPSTREAM.output}" },
    expression: "input.data",
  },
  {
    id: "pickField",
    labelKey: "transform.jsonMapping.templatePickField",
    input: { rows: "${UPSTREAM.output}" },
    expression: "input.rows",
  },
  {
    id: "diagramDetectToDm",
    labelKey: "transform.jsonMapping.templateDiagramDetectToDm",
    input: { rows: "${UPSTREAM.output}" },
    expression: "input.rows",
  },
  {
    id: "diagramDetectToClassic",
    labelKey: "transform.jsonMapping.templateDiagramDetectToClassic",
    input: { rows: "${UPSTREAM.output}" },
    expression: "input.rows",
  },
];

export function applyMapperKindTemplate(
  mapperKind: JsonMappingMapperKind,
  predecessorTaskId: string | null
): { input: Record<string, unknown>; expression: string; config: Record<string, unknown> } {
  const base =
    mapperKind === "diagram_detect_to_dm"
      ? defaultDiagramDetectToDmNodeConfig()
      : mapperKind === "diagram_detect_to_classic"
        ? defaultDiagramDetectToClassicNodeConfig()
        : defaultJsonMappingNodeConfig();
  const applied = applyJsonMappingTemplate(
    {
      id: "passthrough",
      labelKey: "transform.jsonMapping.templatePassthrough",
      input: base.input as Record<string, unknown>,
      expression: String(base.expression ?? "input"),
    },
    predecessorTaskId
  );
  return {
    ...applied,
    config: {
      ...base,
      input: applied.input,
      expression: applied.expression,
    },
  };
}

export function readMapperKind(cfg: Record<string, unknown>): JsonMappingMapperKind {
  const raw = String(cfg.mapper_kind ?? "custom").trim();
  if (raw === "diagram_detect_to_dm" || raw === "diagram_detect_to_classic") return raw;
  return "custom";
}

export function defaultDiagramDetectToDmNodeConfig(): Record<string, unknown> {
  return {
    description: "Map annotations to DM staging",
    mapper_kind: "diagram_detect_to_dm",
    annotation_space: "discovery-annotations",
    default_status: "Suggested",
    input: { rows: "${fanout.output}" },
    expression: "input.rows",
  };
}

export function defaultDiagramDetectToClassicNodeConfig(): Record<string, unknown> {
  return {
    description: "Map annotations to classic staging",
    mapper_kind: "diagram_detect_to_classic",
    input: { rows: "${fanout.output}" },
    expression: "input.rows",
  };
}

export function defaultJsonMappingNodeConfig(): Record<string, unknown> {
  return {
    description: "JSON mapping",
    input: {},
    expression: "input",
  };
}

export function readJsonMappingInput(cfg: Record<string, unknown>): Record<string, unknown> {
  const inp = cfg.input;
  if (inp && typeof inp === "object" && !Array.isArray(inp)) {
    return { ...(inp as Record<string, unknown>) };
  }
  return {};
}

export function readJsonMappingExpression(cfg: Record<string, unknown>): string {
  return String(cfg.expression ?? "");
}

export function jsonMappingSummary(cfg: Record<string, unknown>): string {
  const expr = readJsonMappingExpression(cfg).trim();
  if (!expr) return "";
  return expr.length > 40 ? `${expr.slice(0, 37)}…` : expr;
}

export type JsonMappingValidationIssue = "expressionRequired" | "inputNotObject";

export function validateJsonMappingConfig(
  cfg: Record<string, unknown>
): JsonMappingValidationIssue[] {
  const issues: JsonMappingValidationIssue[] = [];
  if (!readJsonMappingExpression(cfg).trim()) {
    issues.push("expressionRequired");
  }
  const inp = cfg.input;
  if (inp != null && (typeof inp !== "object" || Array.isArray(inp))) {
    issues.push("inputNotObject");
  }
  return issues;
}

export function parseJsonMappingInputText(text: string): {
  ok: true;
  value: Record<string, unknown>;
} | {
  ok: false;
  error: "empty" | "notObject" | "parse";
} {
  const trimmed = text.trim();
  if (!trimmed) {
    return { ok: true, value: {} };
  }
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return { ok: false, error: "notObject" };
    }
    return { ok: true, value: parsed as Record<string, unknown> };
  } catch {
    return { ok: false, error: "parse" };
  }
}

export function applyJsonMappingTemplate(
  template: JsonMappingTemplate,
  predecessorTaskId: string | null
): { input: Record<string, unknown>; expression: string } {
  const ref = predecessorTaskId ? `\${${predecessorTaskId}.output}` : "${UPSTREAM.output}";
  const inputJson = JSON.stringify(template.input).replace(/\$\{UPSTREAM\.output\}/g, ref);
  const input = JSON.parse(inputJson) as Record<string, unknown>;
  return { input, expression: template.expression };
}
