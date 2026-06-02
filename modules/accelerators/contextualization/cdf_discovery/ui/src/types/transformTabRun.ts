/** Local pipeline run UI state stored on document tabs (not persisted to CDF). */

export type TransformPipelineEditorSubTab = "flow" | "console" | "results";

export type TransformPipelineRunResult = {
  ok: boolean;
  run_id?: string;
  detail?: string;
  task_summaries?: Record<string, unknown>;
};

export type TransformTabRunSession = {
  editorSubTab: TransformPipelineEditorSubTab;
  runLog: string;
  cdfLog: string;
  lastRun: TransformPipelineRunResult | null;
  /** True while a local run stream is active (survives document tab switches). */
  runBusy?: boolean;
};

export type TransformTabRunSessionPatch =
  | Partial<TransformTabRunSession>
  | ((prev: TransformTabRunSession) => Partial<TransformTabRunSession>);

export function readTransformTabRunSession(tab: {
  runSession?: TransformTabRunSession | null;
}): TransformTabRunSession {
  return {
    editorSubTab: tab.runSession?.editorSubTab ?? "flow",
    runLog: tab.runSession?.runLog ?? "",
    cdfLog: tab.runSession?.cdfLog ?? "",
    lastRun: tab.runSession?.lastRun ?? null,
    runBusy: tab.runSession?.runBusy ?? false,
  };
}

export function resolveTransformTabRunSessionPatch(
  tab: { runSession?: TransformTabRunSession | null },
  patch: TransformTabRunSessionPatch
): Partial<TransformTabRunSession> {
  const prev = readTransformTabRunSession(tab);
  return typeof patch === "function" ? patch(prev) : patch;
}

export function withTransformTabRunSession<T extends { runSession?: TransformTabRunSession | null }>(
  tab: T,
  patch: TransformTabRunSessionPatch
): T {
  const prev = readTransformTabRunSession(tab);
  const delta = typeof patch === "function" ? patch(prev) : patch;
  return {
    ...tab,
    runSession: { ...prev, ...delta },
  };
}
