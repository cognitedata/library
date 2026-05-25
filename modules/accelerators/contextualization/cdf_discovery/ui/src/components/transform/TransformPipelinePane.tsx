import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  buildTransformPipeline,
  buildTransformTemplate,
  fetchTransformPipeline,
  fetchTransformTemplate,
  saveTransformPipelineCanvas,
  saveTransformTemplateCanvas,
  validateTransformPipeline,
  validateTransformTemplate,
  type TransformBuildResult,
} from "../../api";
import type { MessageKey } from "../../i18n/types";
import { useAppSettings } from "../../context/AppSettingsContext";
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
import { usePipelineRunScope } from "../../utils/pipelineRunScope";
import { TransformFlowPanel } from "./TransformFlowPanel";
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
  result: TransformBuildResult,
  t: (key: MessageKey, vars?: Record<string, string | number>) => string
): string {
  if (!result.ok) {
    const log = (result.stderr || result.stdout || "").trim();
    return log || t("transform.toolbar.buildFailed");
  }
  return t("transform.toolbar.buildOk", { count: String(result.task_count ?? 0) });
}

export function TransformPipelinePane(props: Props) {
  const { onDelete, onRename, onRunSessionPatch } = props;
  const isTemplate = isTemplateProps(props);
  const resourceId = isTemplate ? props.tab.templateId : props.tab.pipelineId;

  const { t } = useAppSettings();
  const [canvas, setCanvas] = useState<TransformCanvasDocument>(
    props.tab.canvas ?? emptyTransformCanvasDocument()
  );
  const [reloadNonce, setReloadNonce] = useState(0);
  const [saving, setSaving] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [runProgress, setRunProgress] = useState<TransformFlowRunProgress>(initialTransformFlowRunProgress);
  const [saveAsOpen, setSaveAsOpen] = useState(false);
  const canvasRef = useRef(canvas);
  canvasRef.current = canvas;

  const pipelineTab = !isTemplate ? props.tab : null;
  const templateTab = isTemplate ? props.tab : null;
  const pipelineScopeSuffix = pipelineTab?.scopeSuffix ?? "all";

  const activeTab = templateTab ?? pipelineTab;
  const runSession = useMemo(
    () => (activeTab ? readTransformTabRunSession(activeTab) : readTransformTabRunSession({})),
    [activeTab]
  );
  const { editorSubTab, runLog, lastRun, runBusy } = runSession;

  const activeParameters = pipelineTab?.document?.parameters ?? templateTab?.document?.parameters;
  const [runScope, setRunScope] = usePipelineRunScope(resourceId, activeParameters);

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
    let cancelled = false;
    const onTabUpdate = onTabUpdateRef.current;
    const load = async () => {
      if (templateTab) {
        try {
          const { template } = await fetchTransformTemplate(templateTab.templateId);
          if (cancelled) return;
          const next = templateDocumentToTab(template, templateTab);
          onTabUpdate(next);
          setCanvas(next.canvas ?? emptyTransformCanvasDocument());
          setReloadNonce((n) => n + 1);
        } catch (e) {
          if (cancelled) return;
          onTabUpdate({ ...templateTab, loading: false, error: String(e) });
        }
        return;
      }
      if (pipelineTab) {
        try {
          const { pipeline } = await fetchTransformPipeline(
            pipelineTab.pipelineId,
            pipelineTab.scopeSuffix ?? "all"
          );
          if (cancelled) return;
          const next = pipelineDocumentToTab(pipeline, pipelineTab);
          onTabUpdate(next);
          setCanvas(next.canvas ?? emptyTransformCanvasDocument());
          setReloadNonce((n) => n + 1);
        } catch (e) {
          if (cancelled) return;
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
    pipelineTab?.loading,
    pipelineTab?.document,
    templateTab?.id,
    templateTab?.loading,
    templateTab?.document,
  ]);

  useEffect(() => {
    const next = props.tab.canvas;
    if (next) setCanvas(next);
  }, [props.tab.canvas]);

  const onChange = useCallback(
    (doc: TransformCanvasDocument) => {
      setCanvas(doc);
      if (templateTab) {
        updateDocumentTab({ ...templateTab, canvas: doc, dirty: true });
      } else if (pipelineTab) {
        updateDocumentTab({ ...pipelineTab, canvas: doc, dirty: true });
      }
    },
    [pipelineTab, templateTab, updateDocumentTab]
  );

  const onSave = useCallback(async () => {
    setSaving(true);
    setStatusMessage(null);
    try {
      if (templateTab) {
        await saveTransformTemplateCanvas(templateTab.templateId, canvas);
        updateDocumentTab({ ...templateTab, canvas, dirty: false, error: null });
      } else if (pipelineTab) {
        await saveTransformPipelineCanvas(pipelineTab.pipelineId, canvas, pipelineScopeSuffix);
        updateDocumentTab({ ...pipelineTab, canvas, dirty: false, error: null });
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
  }, [pipelineTab, templateTab, canvas, updateDocumentTab, pipelineScopeSuffix, t]);

  const onValidate = useCallback(async () => {
    setStatusMessage(null);
    try {
      const result = templateTab
        ? await validateTransformTemplate(templateTab.templateId)
        : pipelineTab
          ? await validateTransformPipeline(pipelineTab.pipelineId, pipelineScopeSuffix)
          : { ok: false };
      setStatusMessage(
        result.ok ? t("transform.toolbar.validateOk") : t("transform.toolbar.validateFailed")
      );
    } catch (e) {
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
          await saveTransformPipelineCanvas(target.id, canvasRef.current, pipelineScopeSuffix);
          updateDocumentTab({ ...target.tab, canvas: canvasRef.current, dirty: false, error: null });
        }
        setStatusMessage(t("transform.toolbar.buildSavedFirst"));
      }
      const result =
        target.kind === "template"
          ? await buildTransformTemplate(target.id)
          : await buildTransformPipeline(target.id, false, pipelineScopeSuffix);
      setStatusMessage(formatTransformBuildStatus(result, t));
      if (!result.ok) {
        const log = (result.stderr || result.stdout || "").trim();
        if (log) patchRunSession({ runLog: log });
      }
    } catch (e) {
      setStatusMessage(String(e));
    } finally {
      setSaving(false);
    }
  }, [pipelineTab, templateTab, pipelineScopeSuffix, updateDocumentTab, patchRunSession, t]);

  const runLocalStreamed = useCallback(
    async ({ incrementalChangeProcessing }: { incrementalChangeProcessing: boolean }) => {
      const runTarget = templateTab
        ? ({ kind: "template" as const, id: templateTab.templateId, tab: templateTab })
        : pipelineTab
          ? ({
              kind: "pipeline" as const,
              id: pipelineTab.pipelineId,
              scopeSuffix: pipelineTab.scopeSuffix ?? "all",
              tab: pipelineTab,
            })
          : null;
      if (!runTarget) return;

      patchRunSession({ runBusy: true });
      setRunProgress(initialTransformFlowRunProgress());
      setRunProgress((p) => ({ ...p, busy: true }));
      setStatusMessage(null);

      if (runTarget.tab.dirty) {
        patchRunSession({ runLog: `${t("run.savedBeforeLocalRun")}\n` });
        try {
          if (runTarget.kind === "template") {
            await saveTransformTemplateCanvas(runTarget.id, canvasRef.current);
            updateDocumentTab({ ...runTarget.tab, canvas: canvasRef.current, dirty: false, error: null });
          } else {
            await saveTransformPipelineCanvas(
              runTarget.id,
              canvasRef.current,
              runTarget.kind === "pipeline" ? (runTarget.scopeSuffix ?? "all") : "all"
            );
            updateDocumentTab({ ...runTarget.tab, canvas: canvasRef.current, dirty: false, error: null });
          }
        } catch (e) {
          patchRunSession((prev) => ({
            runBusy: false,
            runLog: `${prev.runLog}${String(e)}\n`,
          }));
          setRunProgress(initialTransformFlowRunProgress());
          return;
        }
      } else {
        patchRunSession({ runLog: "" });
      }

      patchRunSession((prev) => ({ runLog: `${prev.runLog}${t("status.running")}\n` }));

      try {
        await streamTransformPipelineRun(
          runTarget.kind === "pipeline"
            ? {
                kind: "pipeline",
                id: runTarget.id,
                scopeSuffix: runTarget.scopeSuffix ?? "all",
              }
            : { kind: "template", id: runTarget.id },
          { incrementalChangeProcessing },
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
        patchRunSession({ runBusy: false, editorSubTab: "results" });
        setRunProgress(initialTransformFlowRunProgress());
      }
    },
    [pipelineTab, templateTab, updateDocumentTab, patchRunSession, t]
  );

  const flowRunProgress = useMemo(
    (): TransformFlowRunProgress => ({
      ...runProgress,
      busy: runBusy || runProgress.busy,
    }),
    [runBusy, runProgress]
  );

  const saveAsSource: TransformSaveAsSource | null = pipelineTab
    ? {
        kind: "pipeline",
        pipelineId: pipelineTab.pipelineId,
        label: pipelineTab.label,
        scopeSuffix: pipelineTab.scopeSuffix ?? "all",
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
          onValidate={() => void onValidate()}
          onBuild={() => void onBuild()}
          onRun={(options) => void runLocalStreamed(options)}
          runScope={runScope}
          onRunScopeChange={setRunScope}
          runScopeEnabled
          onDelete={onDelete}
          onRename={onRename}
          saving={saving}
          runBusy={runBusy}
          statusMessage={statusMessage}
          runProgress={flowRunProgress}
        />
      ) : null}

      {editorSubTab === "console" ? (
        <div className="transform-pipeline-console">
          <div className="transform-flow-toolbar" role="toolbar" aria-label={t("transform.toolbar.aria")}>
            <button
              type="button"
              className="disc-btn disc-btn--primary"
              disabled={runBusy || saving}
              onClick={() =>
                void runLocalStreamed({ incrementalChangeProcessing: runScope === "incremental" })
              }
            >
              {runBusy ? t("status.running") : t("transform.toolbar.runLocal")}
            </button>
            <label className="transform-flow-toolbar__run-scope">
              <span className="transform-flow-toolbar__run-scope-label">{t("transform.toolbar.runScope")}</span>
              <select
                className="gov-input"
                value={runScope}
                onChange={(e) => setRunScope(e.target.value as "incremental" | "all")}
                title={t("transform.toolbar.runScopeHint")}
                disabled={runBusy || saving}
              >
                <option value="incremental">{t("transform.toolbar.runScopeIncremental")}</option>
                <option value="all">{t("transform.toolbar.runScopeAll")}</option>
              </select>
            </label>
            <p className="transform-pipeline-console__hint">{t("transform.console.hint")}</p>
          </div>
          <textarea
            readOnly
            className="gov-textarea gov-textarea--readonly transform-pipeline-console__log"
            value={runLog}
            placeholder={t("run.outputPlaceholder")}
            aria-label={t("transform.editorSubtabs.console")}
          />
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
