import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  buildTransformWorkflow,
  buildTransformTemplate,
  deployTransformWorkflowCdf,
  fetchTransformWorkflow,
  fetchTransformTemplate,
  runTransformWorkflowCdf,
  saveTransformWorkflowCanvas,
  saveTransformTemplateCanvas,
  validateTransformWorkflow,
  validateTransformTemplate,
  type TransformWorkflowBuildResult,
  type TransformWorkflowCdfCliResult,
} from "../../api";
import type { MessageKey } from "../../i18n/types";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { Node } from "@xyflow/react";
import type { EtlPipelineDocumentTab, EtlTemplateDocumentTab } from "../../types/discoveryNodes";
import {
  readTransformTabRunSession,
  type TransformPipelineEditorSubTab,
  type TransformTabRunSessionPatch,
} from "../../types/transformTabRun";
import {
  emptyTransformCanvasDocument,
  type TransformCanvasDocument,
} from "../../types/transformCanvas";
import { pipelineDocumentToTab, templateDocumentToTab } from "../../utils/transformTabs";
import { usePipelineDryRun, usePipelineRunScope } from "../../utils/pipelineRunScope";
import { TransformLocalRunDryRunField } from "./TransformLocalRunDryRunField";
import { TransformFlowPanel } from "./TransformFlowPanel";
import { canvasValidationNodeIds } from "../../utils/canvasValidationNodeIds";
import { TransformRunResultsPanel } from "./TransformRunResultsPanel";
import {
  TransformSaveAsDialog,
  type TransformSaveAsResult,
  type TransformSaveAsSource,
} from "./TransformSaveAsDialog";
import {
  initialTransformFlowRunProgress,
  streamTransformPipelineRun,
  type TransformFlowRunProgress,
} from "./transformPipelineRunStream";

type BaseProps = {
  onDelete?: () => void;
  onRename?: () => void;
  onRunSessionPatch: (tabId: string, patch: TransformTabRunSessionPatch) => void;
  onCopyCreated?: (result: TransformSaveAsResult) => void;
  onOpenNodePreviewQuery?: (node: Node) => void;
  /** Refresh Transform → Workflows tree after a successful build (new workflow YAML children). */
  onBuildComplete?: (result: TransformWorkflowBuildResult) => void;
};

type PipelineProps = BaseProps & {
  editorKind?: "pipeline";
  tab: EtlPipelineDocumentTab;
  onTabUpdate: (tab: EtlPipelineDocumentTab) => void;
};

type TemplateProps = BaseProps & {
  editorKind: "template";
  tab: EtlTemplateDocumentTab;
  onTabUpdate: (tab: EtlTemplateDocumentTab) => void;
};

type Props = PipelineProps | TemplateProps;

function subtabClass(active: boolean): string {
  return `disc-gov-subtab${active ? " disc-gov-subtab--active" : ""}`;
}

function isTemplateProps(props: Props): props is TemplateProps {
  return props.editorKind === "template";
}

function formatTransformBuildStatus(
  result: TransformWorkflowBuildResult,
  t: (key: MessageKey, vars?: Record<string, string | number>) => string
): string {
  if (!result.ok) {
    const log = (result.stderr || result.stdout || "").trim();
    return log || t("transform.toolbar.buildFailed");
  }
  return t("transform.toolbar.buildOk", { count: String(result.task_count ?? 0) });
}

function formatCdfCliLog(result: TransformWorkflowCdfCliResult): string {
  const parts = [`exit_code: ${result.exit_code}`];
  if (result.stdout?.trim()) {
    parts.push("", "--- stdout ---", result.stdout.trimEnd());
  }
  if (result.stderr?.trim()) {
    parts.push("", "--- stderr ---", result.stderr.trimEnd());
  }
  return parts.join("\n");
}

export function TransformPipelinePane(props: Props) {
  const { onDelete, onRename, onRunSessionPatch, onOpenNodePreviewQuery, onBuildComplete } = props;
  const isTemplate = isTemplateProps(props);
  const resourceId = isTemplate ? props.tab.templateId : props.tab.pipelineId;

  const { t } = useAppSettings();
  const [canvas, setCanvas] = useState<TransformCanvasDocument>(
    props.tab.canvas ?? emptyTransformCanvasDocument()
  );
  const [reloadNonce, setReloadNonce] = useState(0);
  const [reloading, setReloading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [runProgress, setRunProgress] = useState<TransformFlowRunProgress>(initialTransformFlowRunProgress);
  const [saveAsOpen, setSaveAsOpen] = useState(false);
  const canvasRef = useRef(canvas);
  canvasRef.current = canvas;
  const consoleLogRef = useRef<HTMLTextAreaElement | null>(null);
  const localRunAbortRef = useRef<AbortController | null>(null);
  const localRunGenerationRef = useRef(0);
  const [localRunInFlight, setLocalRunInFlight] = useState(false);

  const pipelineTab = !isTemplate ? props.tab : null;
  const templateTab = isTemplate ? props.tab : null;
  const activeTabRef = useRef(pipelineTab ?? templateTab);
  activeTabRef.current = pipelineTab ?? templateTab;
  const pipelineScopeSuffix = pipelineTab?.scopeSuffix ?? "";

  const activeTab = templateTab ?? pipelineTab;
  const runSession = useMemo(
    () => (activeTab ? readTransformTabRunSession(activeTab) : readTransformTabRunSession({})),
    [activeTab]
  );
  const { editorSubTab, runLog, lastRun, runBusy } = runSession;
  const effectiveRunBusy = runBusy || localRunInFlight;
  const [cdfLog, setCdfLog] = useState("");
  const [cdfBusy, setCdfBusy] = useState(false);
  const [cdfInstanceSpace, setCdfInstanceSpace] = useState("");
  const [validationFailedNodeIds, setValidationFailedNodeIds] = useState<string[]>([]);

  const activeParameters = pipelineTab?.document?.parameters ?? templateTab?.document?.parameters;
  const [runScope, setRunScope] = usePipelineRunScope(resourceId, activeParameters);
  const [dryRun, setDryRun] = usePipelineDryRun(resourceId);
  const combinedConsoleLog = useMemo(() => {
    const parts: string[] = [];
    if (runLog.trim()) {
      parts.push(runLog.trimEnd());
    }
    if (cdfLog.trim()) {
      parts.push(cdfLog.trimEnd());
    }
    return parts.join("\n\n");
  }, [runLog, cdfLog]);

  useEffect(() => {
    if (editorSubTab !== "console") return;
    const logNode = consoleLogRef.current;
    if (!logNode) return;
    logNode.scrollTop = logNode.scrollHeight;
  }, [editorSubTab, combinedConsoleLog]);

  const updateDocumentTab = useCallback(
    (tab: EtlPipelineDocumentTab | EtlTemplateDocumentTab) => {
      if (isTemplateProps(props)) {
        props.onTabUpdate(tab as EtlTemplateDocumentTab);
      } else {
        props.onTabUpdate(tab as EtlPipelineDocumentTab);
      }
    },
    [props]
  );
  const onTabUpdateRef = useRef(updateDocumentTab);
  onTabUpdateRef.current = updateDocumentTab;
  const onRunSessionPatchRef = useRef(onRunSessionPatch);
  onRunSessionPatchRef.current = onRunSessionPatch;
  const tabIdRef = useRef(props.tab.id);
  tabIdRef.current = props.tab.id;
  const loadGen = useRef(0);

  const patchRunSession = useCallback((patch: TransformTabRunSessionPatch) => {
    onRunSessionPatchRef.current(tabIdRef.current, patch);
  }, []);

  const setEditorSubTab = useCallback(
    (subtab: TransformPipelineEditorSubTab) => patchRunSession({ editorSubTab: subtab }),
    [patchRunSession]
  );

  useEffect(() => {
    const tab = templateTab ?? pipelineTab;
    if (!tab || tab.document != null || !tab.loading) return;

    const expectedTabId = tab.id;
    const gen = ++loadGen.current;
    let cancelled = false;
    const onTabUpdate = onTabUpdateRef.current;

    const load = async () => {
      if (templateTab) {
        const templateId = templateTab.templateId;
        try {
          const { template } = await fetchTransformTemplate(templateId);
          if (cancelled || gen !== loadGen.current || tabIdRef.current !== expectedTabId) return;
          const next = templateDocumentToTab(template, templateTab);
          onTabUpdate(next);
          setCanvas(next.canvas ?? emptyTransformCanvasDocument());
          setReloadNonce((n) => n + 1);
        } catch (e) {
          if (cancelled || gen !== loadGen.current || tabIdRef.current !== expectedTabId) return;
          onTabUpdate({ ...templateTab, loading: false, error: String(e) });
        }
        return;
      }
      if (pipelineTab) {
        const pipelineId = pipelineTab.pipelineId;
        const scopeSuffix = pipelineTab.scopeSuffix ?? "";
        try {
          const { workflow: workflowDoc } = await fetchTransformWorkflow(pipelineId, scopeSuffix);
          if (cancelled || gen !== loadGen.current || tabIdRef.current !== expectedTabId) return;
          const next = pipelineDocumentToTab(workflowDoc, pipelineTab);
          onTabUpdate(next);
          setCanvas(next.canvas ?? emptyTransformCanvasDocument());
          setReloadNonce((n) => n + 1);
        } catch (e) {
          if (cancelled || gen !== loadGen.current || tabIdRef.current !== expectedTabId) return;
          onTabUpdate({ ...pipelineTab, loading: false, error: String(e) });
        }
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [
    pipelineTab?.id,
    pipelineTab?.pipelineId,
    pipelineTab?.scopeSuffix,
    pipelineTab?.loading,
    pipelineTab?.document,
    templateTab?.id,
    templateTab?.templateId,
    templateTab?.loading,
    templateTab?.document,
  ]);

  useEffect(() => {
    const docCanvas =
      pipelineTab?.document?.canvas ?? templateTab?.document?.canvas ?? null;
    const next = props.tab.canvas ?? docCanvas;
    setCanvas(next ?? emptyTransformCanvasDocument());
  }, [props.tab.id, props.tab.canvas, pipelineTab?.document, templateTab?.document]);

  const onChange = useCallback(
    (doc: TransformCanvasDocument) => {
      canvasRef.current = doc;
      setCanvas(doc);
      setValidationFailedNodeIds([]);
      if (templateTab) {
        updateDocumentTab({ ...templateTab, canvas: doc, dirty: true });
      } else if (pipelineTab) {
        updateDocumentTab({ ...pipelineTab, canvas: doc, dirty: true });
      }
    },
    [pipelineTab, templateTab, updateDocumentTab]
  );

  const onReload = useCallback(async () => {
    setReloading(true);
    setStatusMessage(null);
    setValidationFailedNodeIds([]);
    try {
      if (templateTab) {
        const { template } = await fetchTransformTemplate(templateTab.templateId);
        const next = templateDocumentToTab(template, templateTab);
        updateDocumentTab({ ...next, dirty: false, error: null });
        setCanvas(next.canvas ?? emptyTransformCanvasDocument());
        setReloadNonce((n) => n + 1);
      } else if (pipelineTab) {
        const { workflow: workflowDoc } = await fetchTransformWorkflow(
          pipelineTab.pipelineId,
          pipelineTab.scopeSuffix ?? ""
        );
        const next = pipelineDocumentToTab(workflowDoc, pipelineTab);
        updateDocumentTab({ ...next, dirty: false, error: null });
        setCanvas(next.canvas ?? emptyTransformCanvasDocument());
        setReloadNonce((n) => n + 1);
      }
    } catch (e) {
      if (templateTab) {
        updateDocumentTab({ ...templateTab, error: String(e) });
      } else if (pipelineTab) {
        updateDocumentTab({ ...pipelineTab, error: String(e) });
      }
    } finally {
      setReloading(false);
    }
  }, [pipelineTab, templateTab, updateDocumentTab]);

  const onSave = useCallback(async () => {
    setSaving(true);
    setStatusMessage(null);
    const latestCanvas = canvasRef.current;
    try {
      if (templateTab) {
        await saveTransformTemplateCanvas(templateTab.templateId, latestCanvas);
        updateDocumentTab({ ...templateTab, canvas: latestCanvas, dirty: false, error: null });
      } else if (pipelineTab) {
        await saveTransformWorkflowCanvas(pipelineTab.pipelineId, latestCanvas, pipelineScopeSuffix);
        updateDocumentTab({ ...pipelineTab, canvas: latestCanvas, dirty: false, error: null });
      }
      setStatusMessage(t("transform.toolbar.saved"));
    } catch (e) {
      if (templateTab) {
        updateDocumentTab({ ...templateTab, error: String(e) });
      } else if (pipelineTab) {
        updateDocumentTab({ ...pipelineTab, error: String(e) });
      }
    } finally {
      setSaving(false);
    }
  }, [pipelineTab, templateTab, updateDocumentTab, pipelineScopeSuffix, t]);

  const onValidate = useCallback(async () => {
    setStatusMessage(null);
    try {
      const canvasDoc = canvasRef.current as unknown as Record<string, unknown>;
      const result = templateTab
        ? await validateTransformTemplate(templateTab.templateId, canvasDoc)
        : pipelineTab
          ? await validateTransformWorkflow(
              pipelineTab.pipelineId,
              pipelineScopeSuffix,
              canvasDoc
            )
          : { ok: false };
      if (result.ok) {
        setValidationFailedNodeIds([]);
        setStatusMessage(t("transform.toolbar.validateOk"));
        return;
      }
      const errors = result.errors ?? [];
      setValidationFailedNodeIds(canvasValidationNodeIds(errors));
      const detail = errors.join("\n").trim();
      setStatusMessage(
        detail ? `${t("transform.toolbar.validateFailed")}\n${detail}` : t("transform.toolbar.validateFailed")
      );
    } catch (e) {
      setValidationFailedNodeIds([]);
      setStatusMessage(String(e));
    }
  }, [pipelineTab, templateTab, pipelineScopeSuffix, t]);

  const onBuild = useCallback(async () => {
    const target = templateTab
      ? ({ kind: "template" as const, id: templateTab.templateId, tab: templateTab })
      : pipelineTab
        ? ({ kind: "pipeline" as const, id: pipelineTab.pipelineId, tab: pipelineTab })
        : null;
    if (!target) return;

    setStatusMessage(null);
    setSaving(true);
    try {
      if (target.tab.dirty) {
        if (target.kind === "template") {
          await saveTransformTemplateCanvas(target.id, canvasRef.current);
          updateDocumentTab({ ...target.tab, canvas: canvasRef.current, dirty: false, error: null });
        } else {
          await saveTransformWorkflowCanvas(target.id, canvasRef.current, pipelineScopeSuffix);
          updateDocumentTab({ ...target.tab, canvas: canvasRef.current, dirty: false, error: null });
        }
        setStatusMessage(t("transform.toolbar.buildSavedFirst"));
      }
      const result =
        target.kind === "template"
          ? await buildTransformTemplate(target.id)
          : await buildTransformWorkflow(target.id, pipelineScopeSuffix);
      setStatusMessage(formatTransformBuildStatus(result, t));
      if (!result.ok) {
        const log = (result.stderr || result.stdout || "").trim();
        if (log) patchRunSession({ runLog: log });
      } else {
        onBuildComplete?.(result);
      }
    } catch (e) {
      setStatusMessage(String(e));
    } finally {
      setSaving(false);
    }
  }, [
    pipelineTab,
    templateTab,
    pipelineScopeSuffix,
    updateDocumentTab,
    patchRunSession,
    onBuildComplete,
    t,
  ]);

  const runDeployCdf = useCallback(
    async (dryRun: boolean) => {
      if (!pipelineTab) return;
      setCdfBusy(true);
      setCdfLog(`${t("status.running")}\n`);
      try {
        const result = await deployTransformWorkflowCdf(pipelineTab.pipelineId, pipelineScopeSuffix, {
          dryRun,
        });
        setCdfLog(formatCdfCliLog(result));
      } catch (e) {
        setCdfLog(String(e));
      } finally {
        setCdfBusy(false);
      }
    },
    [pipelineTab, pipelineScopeSuffix, t]
  );

  const runCdfWorkflow = useCallback(
    async (dryRun: boolean) => {
      if (!pipelineTab) return;
      setCdfBusy(true);
      setCdfLog(`${t("status.running")}\n`);
      try {
        const result = await runTransformWorkflowCdf(pipelineTab.pipelineId, pipelineScopeSuffix, {
          dryRun,
          instanceSpace: cdfInstanceSpace,
        });
        setCdfLog(formatCdfCliLog(result));
      } catch (e) {
        setCdfLog(String(e));
      } finally {
        setCdfBusy(false);
      }
    },
    [pipelineTab, pipelineScopeSuffix, cdfInstanceSpace, t]
  );

  const cancelLocalRun = useCallback(() => {
    localRunGenerationRef.current += 1;
    localRunAbortRef.current?.abort();
    localRunAbortRef.current = null;
    setLocalRunInFlight(false);
    patchRunSession((prev) => ({
      runBusy: false,
      runLog: `${prev.runLog}${t("run.localCancelled")}\n`,
    }));
    setRunProgress(initialTransformFlowRunProgress());
  }, [patchRunSession, t]);

  const runLocalStreamed = useCallback(
    async ({
      incrementalChangeProcessing,
      dryRun: dryRunRequested,
    }: {
      incrementalChangeProcessing: boolean;
      dryRun: boolean;
    }) => {
      const tabNow = activeTabRef.current;
      const runTarget = isTemplate && templateTab
        ? ({ kind: "template" as const, id: templateTab.templateId, tab: tabNow ?? templateTab })
        : pipelineTab
          ? ({
              kind: "pipeline" as const,
              id: pipelineTab.pipelineId,
              scopeSuffix: pipelineTab.scopeSuffix ?? "",
              tab: tabNow ?? pipelineTab,
            })
          : null;
      if (!runTarget) return;

      const runGen = ++localRunGenerationRef.current;
      const runCancelled = () => runGen !== localRunGenerationRef.current;

      setLocalRunInFlight(true);
      try {
        patchRunSession({ runBusy: true });
        setRunProgress(initialTransformFlowRunProgress());
        setRunProgress((p) => ({ ...p, busy: true }));
        setStatusMessage(null);

        if (runTarget.tab.dirty) {
          patchRunSession({ runLog: `${t("run.savedBeforeLocalRun")}\n` });
          try {
            if (runTarget.kind === "template") {
              await saveTransformTemplateCanvas(runTarget.id, canvasRef.current);
            } else {
              await saveTransformWorkflowCanvas(
                runTarget.id,
                canvasRef.current,
                runTarget.kind === "pipeline" ? (runTarget.scopeSuffix ?? "") : ""
              );
            }
            const tabAfterSave = activeTabRef.current;
            if (tabAfterSave) {
              updateDocumentTab({
                ...tabAfterSave,
                canvas: canvasRef.current,
                dirty: false,
                error: null,
              });
            }
          } catch (e) {
            patchRunSession((prev) => ({
              runBusy: false,
              runLog: `${prev.runLog}${String(e)}\n`,
            }));
            setRunProgress(initialTransformFlowRunProgress());
            return;
          }
          if (runCancelled()) return;
        } else {
          patchRunSession({ runLog: "" });
        }

        if (runCancelled()) return;

        try {
          const validation =
            runTarget.kind === "template"
              ? await validateTransformTemplate(
                  runTarget.id,
                  canvasRef.current as unknown as Record<string, unknown>
                )
              : await validateTransformWorkflow(
                  runTarget.id,
                  runTarget.scopeSuffix ?? "",
                  canvasRef.current as unknown as Record<string, unknown>
                );
          if (!validation.ok) {
            const errors = validation.errors ?? [];
            setValidationFailedNodeIds(canvasValidationNodeIds(errors));
            const detail = errors.length ? errors.join("\n") : t("transform.toolbar.validateFailed");
            patchRunSession((prev) => ({
              runBusy: false,
              runLog: `${prev.runLog}${detail}\n`,
              lastRun: { ok: false, detail },
            }));
            setRunProgress(initialTransformFlowRunProgress());
            setStatusMessage(t("transform.toolbar.validateFailed"));
            return;
          }
          setValidationFailedNodeIds([]);
        } catch (e) {
          patchRunSession((prev) => ({
            runBusy: false,
            runLog: `${prev.runLog}${String(e)}\n`,
            lastRun: { ok: false, detail: String(e) },
          }));
          setRunProgress(initialTransformFlowRunProgress());
          return;
        }

        if (runCancelled()) return;

        patchRunSession((prev) => ({ runLog: `${prev.runLog}${t("status.running")}\n` }));

        localRunAbortRef.current?.abort();
        const abortController = new AbortController();
        localRunAbortRef.current = abortController;

        try {
          await streamTransformPipelineRun(
            runTarget.kind === "pipeline"
              ? {
                  kind: "pipeline",
                  id: runTarget.id,
                  scopeSuffix: runTarget.scopeSuffix ?? "",
                }
              : { kind: "template", id: runTarget.id },
            {
              incrementalChangeProcessing,
              dryRun: dryRunRequested,
              signal: abortController.signal,
            },
            canvasRef.current,
            t,
            {
              onLogAppend: (chunk) =>
                patchRunSession((prev) => ({ runLog: `${prev.runLog}${chunk}` })),
              onProgress: setRunProgress,
              onComplete: (result) => {
                patchRunSession({ lastRun: result });
                setStatusMessage(result.detail ?? null);
              },
            }
          );
        } catch (e) {
          patchRunSession((prev) => ({
            runLog: `${prev.runLog}${String(e)}\n`,
            lastRun: { ok: false, detail: String(e) },
          }));
        } finally {
          if (localRunAbortRef.current === abortController) {
            localRunAbortRef.current = null;
          }
          patchRunSession((prev) => ({
            runBusy: false,
            editorSubTab: prev.editorSubTab === "console" ? "console" : "results",
          }));
          setRunProgress(initialTransformFlowRunProgress());
        }
      } finally {
        if (runGen === localRunGenerationRef.current) {
          setLocalRunInFlight(false);
        }
      }
    },
    [isTemplate, pipelineTab, templateTab, updateDocumentTab, patchRunSession, t]
  );

  const flowRunProgress = useMemo(
    (): TransformFlowRunProgress => ({
      ...runProgress,
      busy: effectiveRunBusy || runProgress.busy,
    }),
    [effectiveRunBusy, runProgress]
  );

  const saveAsSource: TransformSaveAsSource | null = pipelineTab
    ? {
        kind: "pipeline",
        pipelineId: pipelineTab.pipelineId,
        label: pipelineTab.label,
        scopeSuffix: pipelineTab.scopeSuffix ?? "",
      }
    : templateTab
      ? { kind: "template", templateId: templateTab.templateId, label: templateTab.label }
      : null;

  if (!activeTab) return null;

  if (activeTab.error && activeTab.document == null) {
    return (
      <div className="transform-flow-error" role="alert">
        {activeTab.error}
      </div>
    );
  }

  if (activeTab.loading && activeTab.document == null) {
    return <div className="transform-flow-loading">{t("transform.loading")}</div>;
  }

  return (
    <>
    <div className="transform-pipeline-editor">
      <nav
        className="disc-gov-subtabs transform-pipeline-editor__subtabs"
        role="tablist"
        aria-label={t("transform.editorSubtabs.aria")}
      >
        <button
          type="button"
          role="tab"
          aria-selected={editorSubTab === "flow"}
          className={subtabClass(editorSubTab === "flow")}
          onClick={() => setEditorSubTab("flow")}
        >
          {t("transform.editorSubtabs.flow")}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={editorSubTab === "console"}
          className={subtabClass(editorSubTab === "console")}
          onClick={() => setEditorSubTab("console")}
        >
          {t("transform.editorSubtabs.console")}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={editorSubTab === "results"}
          className={subtabClass(editorSubTab === "results")}
          onClick={() => setEditorSubTab("results")}
        >
          {t("transform.editorSubtabs.results")}
        </button>
      </nav>

      {editorSubTab === "flow" ? (
        <TransformFlowPanel
          t={t}
          pipelineId={resourceId}
          initialDocument={canvas}
          reloadNonce={reloadNonce}
          onChange={onChange}
          onSave={() => void onSave()}
          onSaveAs={saveAsSource ? () => setSaveAsOpen(true) : undefined}
          onReload={() => void onReload()}
          onValidate={() => void onValidate()}
          reloading={reloading}
          onBuild={() => void onBuild()}
          onRun={(options) => void runLocalStreamed(options)}
          runScope={runScope}
          onRunScopeChange={setRunScope}
          runScopeEnabled
          dryRun={dryRun}
          onDryRunChange={setDryRun}
          onDelete={onDelete}
          onRename={onRename}
          saving={saving}
          runBusy={effectiveRunBusy}
          onCancelLocalRun={cancelLocalRun}
          statusMessage={statusMessage}
          runProgress={flowRunProgress}
          validationFailedNodeIds={validationFailedNodeIds}
          onOpenNodePreviewQuery={onOpenNodePreviewQuery}
        />
      ) : null}

      {editorSubTab === "console" ? (
        <div className="transform-pipeline-console">
          <div className="transform-pipeline-console__section">
            <p className="transform-pipeline-console__section-title">{t("transform.editorSubtabs.console")}</p>
            <div className="transform-pipeline-console__controls">
              <div className="transform-flow-toolbar" role="toolbar" aria-label={t("transform.toolbar.aria")}>
                {effectiveRunBusy ? (
                  <button type="button" className="disc-btn disc-btn--primary" onClick={cancelLocalRun}>
                    {t("transform.toolbar.cancelLocalRun")}
                  </button>
                ) : (
                  <button
                    type="button"
                    className="disc-btn disc-btn--primary"
                    disabled={saving || cdfBusy}
                    onClick={() =>
                      void runLocalStreamed({
                        incrementalChangeProcessing: runScope === "incremental",
                        dryRun,
                      })
                    }
                  >
                    {t("transform.toolbar.runLocal")}
                  </button>
                )}
                <TransformLocalRunDryRunField
                  t={t}
                  dryRun={dryRun}
                  onDryRunChange={setDryRun}
                  disabled={effectiveRunBusy || saving || cdfBusy}
                />
                <label className="transform-flow-toolbar__run-scope">
                  <span className="transform-flow-toolbar__run-scope-label">{t("transform.toolbar.runScope")}</span>
                  <select
                    className="gov-input"
                    value={runScope}
                    onChange={(e) => setRunScope(e.target.value as "incremental" | "all")}
                    title={t("transform.toolbar.runScopeHint")}
                    disabled={effectiveRunBusy || saving || cdfBusy}
                  >
                    <option value="incremental">{t("transform.toolbar.runScopeIncremental")}</option>
                    <option value="all">{t("transform.toolbar.runScopeAll")}</option>
                  </select>
                </label>
              </div>
              {!isTemplate ? (
                <div className="transform-flow-toolbar" role="toolbar" aria-label={t("transform.console.cdfToolsTitle")}>
                  <button
                    type="button"
                    className="disc-btn"
                    disabled={cdfBusy || effectiveRunBusy || saving}
                    onClick={() => void runDeployCdf(false)}
                  >
                    {cdfBusy ? t("transform.console.cdfBusy") : t("transform.console.deployCdf")}
                  </button>
                  <button
                    type="button"
                    className="disc-btn"
                    disabled={cdfBusy || effectiveRunBusy || saving}
                    onClick={() => void runCdfWorkflow(false)}
                  >
                    {t("transform.console.runCdf")}
                  </button>
                  <button
                    type="button"
                    className="disc-btn disc-btn--ghost"
                    disabled={cdfBusy || effectiveRunBusy || saving}
                    onClick={() => void runCdfWorkflow(true)}
                  >
                    {t("transform.console.runCdfDryRun")}
                  </button>
                  <button
                    type="button"
                    className="disc-btn disc-btn--ghost"
                    disabled={cdfBusy || effectiveRunBusy || saving}
                    onClick={() => void runDeployCdf(true)}
                  >
                    {t("transform.console.deployCdfDryRun")}
                  </button>
                </div>
              ) : null}
            </div>
            {!isTemplate ? (
              <div className="transform-pipeline-console__cdf-field">
                <label htmlFor={`cdf-instance-space-${resourceId}`}>
                  {t("transform.console.cdfInstanceSpaceLabel")}
                </label>
                <input
                  id={`cdf-instance-space-${resourceId}`}
                  type="text"
                  className="gov-input"
                  value={cdfInstanceSpace}
                  onChange={(e) => setCdfInstanceSpace(e.target.value)}
                  placeholder={t("transform.console.cdfInstanceSpacePlaceholder")}
                  autoComplete="off"
                  spellCheck={false}
                  disabled={cdfBusy || effectiveRunBusy || saving}
                />
              </div>
            ) : null}
            <div className="transform-pipeline-console__hint-row">
              <p className="transform-pipeline-console__hint">
                {isTemplate ? t("transform.console.templateHint") : t("transform.console.hint")}
              </p>
              {!isTemplate ? (
                <p className="transform-pipeline-console__hint">{t("transform.console.cdfHint")}</p>
              ) : null}
            </div>
            <textarea
              ref={consoleLogRef}
              readOnly
              className="gov-textarea gov-textarea--readonly transform-pipeline-console__log"
              value={combinedConsoleLog}
              placeholder={
                isTemplate ? t("run.outputPlaceholder") : t("transform.console.cdfLogPlaceholder")
              }
              aria-label={t("transform.editorSubtabs.console")}
            />
          </div>
        </div>
      ) : null}

      {editorSubTab === "results" ? (
        <TransformRunResultsPanel t={t} canvas={canvas} lastRun={lastRun} />
      ) : null}
    </div>
    {saveAsSource ? (
      <TransformSaveAsDialog
        open={saveAsOpen}
        source={saveAsSource}
        getCanvas={() => canvasRef.current}
        onClose={() => setSaveAsOpen(false)}
        onSaved={(result) => {
          setSaveAsOpen(false);
          props.onCopyCreated?.(result);
        }}
      />
    ) : null}
    </>
  );
}
