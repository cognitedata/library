/** Defaults sized for CDF Function 15-minute execution limit (one detect job per child task). */
export const DEFAULT_MAX_PAGES_PER_FILE_REFERENCE = 15;
export const DEFAULT_MAX_PAGES_PER_DETECT_REQUEST = 15;
export const DEFAULT_FANOUT_BATCH_SIZE = 1;
export const DEFAULT_MAX_PATTERN_SAMPLES = 100;
export const DEFAULT_MIN_TOKENS = 2;
export const DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC = 840;
export const CDF_FUNCTION_MAX_RUNTIME_SEC = 900;

export function readOptionalPositiveInt(raw: unknown): number | undefined {
  if (raw === "" || raw === null || raw === undefined) return undefined;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

export function workflowFanoutPlanSummary(config: Record<string, unknown>): string {
  const parts: string[] = [];
  const desc = String(config.description ?? "").trim();
  const mode = config.pattern_mode === false ? "annotate" : "pattern";
  parts.push(mode);
  const batch = readOptionalPositiveInt(config.batch_size);
  if (batch) parts.push(`batch ${batch}`);
  const pagesCall = readOptionalPositiveInt(config.max_pages_per_detect_request);
  if (pagesCall) parts.push(`${pagesCall} pp/call`);
  const pagesRef = readOptionalPositiveInt(config.max_pages_per_file_reference);
  if (pagesRef && pagesRef !== DEFAULT_MAX_PAGES_PER_DETECT_REQUEST) {
    parts.push(`${pagesRef} pp/ref`);
  }
  const mime = String(config.mime_type ?? "").trim();
  if (mime) parts.push(mime);
  const child = String(config.child_function_external_id ?? "").trim();
  if (child) parts.push(child.replace(/^fn_/, ""));
  if (parts.length) return parts.join(" · ");
  return desc;
}

export function dynamicFanoutSummary(config: Record<string, unknown>): string {
  const parts: string[] = [];
  const desc = String(config.description ?? "").trim();
  const gen = String(config.generator_task_id ?? "").trim();
  if (gen) parts.push(`← ${gen}`);
  const child = String(config.child_function_external_id ?? "").trim();
  if (child) parts.push(child.replace(/^fn_/, ""));
  if (parts.length) return parts.join(" · ");
  return desc;
}
