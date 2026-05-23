import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AdvancedYamlPanel } from "./components/AdvancedYamlPanel";
import { CogniteLogo } from "./components/CogniteLogo";
import {
  IconToolbarReload,
  IconToolbarSave,
} from "./components/ConfigureToolbarIcons";
import { PatternsEditor } from "./components/PatternsEditor";
import { RunWorkflowStrip } from "./components/RunWorkflowStrip";
import { ScopeHierarchyEditor } from "./components/ScopeHierarchyEditor";
import { useAppSettings } from "./context/AppSettingsContext";
import { LOCALES, type MessageKey } from "./i18n";
import { apiUrl } from "./utils/apiBase";
import {
  fetchDefaultConfig,
  fetchHealth,
  fetchRunResults,
  previewRunResult,
  saveDefaultConfig,
  validateConfigs,
} from "./api";
import type { PatternsData, ScopeHierarchyData } from "./types/assetConfig";
import { matchConfigSearch } from "./utils/configPanelSearch";
import {
  CONFIG_STEPS,
  DEFAULT_CONFIG_REL,
  mergePatternsIntoStepYaml,
  mergeScopeIntoStepYaml,
  mergeStepYamlIntoDefault,
  patternsFromStepYaml,
  scopeFromStepYaml,
  stepYamlFromDefault,
  type ConfigStep,
} from "./utils/defaultConfigYaml";
import {
  initialStepStatuses,
  parseProgressLine,
  PIPELINE_STEP_ORDER,
  stepsForRun,
  type ProgressEvent,
  type RunStepId,
  type StepRunStatus,
} from "./utils/runStream";

type Tab = "configure" | "run" | "results";
type PhaseKey = "status.loading" | "status.saving" | "status.saved" | "status.loaded";

function tabClass(active: boolean): string {
  return `fas-tab${active ? " fas-tab--active" : ""}`;
}

export default function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();

  const [tab, setTab] = useState<Tab>("configure");
  const [configSearchQuery, setConfigSearchQuery] = useState("");
  const [apiOk, setApiOk] = useState<boolean | null>(null);
  const [activeStep, setActiveStep] = useState<ConfigStep>("scope");
  const [fullYaml, setFullYaml] = useState("");
  const [savedSnap, setSavedSnap] = useState("");
  const [phase, setPhase] = useState<PhaseKey | null>("status.loading");
  const [error, setError] = useState<string | null>(null);
  const [validateOut, setValidateOut] = useState("");
  const [runLog, setRunLog] = useState("");
  const [runBusy, setRunBusy] = useState(false);
  const [runStepStatuses, setRunStepStatuses] = useState<Record<RunStepId, StepRunStatus>>(
    initialStepStatuses("all")
  );
  const [runPlannedSteps, setRunPlannedSteps] = useState<RunStepId[]>([...PIPELINE_STEP_ORDER]);
  const [runItems, setRunItems] = useState<Array<{ path: string; run_scope?: unknown }>>([]);
  const runLogRef = useRef<HTMLTextAreaElement>(null);
  const [preview, setPreview] = useState("");

  const stepYaml = useMemo(() => {
    if (!fullYaml) return "";
    try {
      return stepYamlFromDefault(fullYaml, activeStep);
    } catch {
      return "";
    }
  }, [fullYaml, activeStep]);

  const filteredSteps = useMemo(
    () =>
      CONFIG_STEPS.filter((s) =>
        matchConfigSearch(configSearchQuery, t(s.labelKey), s.id, `file_asset_source.${s.id}`)
      ),
    [configSearchQuery, t]
  );

  const scopeForm = useMemo(() => {
    if (!stepYaml || activeStep !== "scope") return null;
    try {
      return scopeFromStepYaml(stepYaml);
    } catch {
      return null;
    }
  }, [stepYaml, activeStep]);
  const patternsForm = useMemo(
    () => (stepYaml && activeStep === "extract" ? patternsFromStepYaml(stepYaml) : null),
    [stepYaml, activeStep]
  );

  const dirty = fullYaml !== savedSnap;
  const phaseLabel = phase ? t(phase) : "";

  const configureHintKey: MessageKey | null =
    activeStep === "scope"
      ? "configure.hint.scope"
      : activeStep === "extract"
        ? "configure.hint.extract"
        : null;

  const primaryTabs: { id: Tab; labelKey: MessageKey }[] = [
    { id: "configure", labelKey: "tabs.configure" },
    { id: "run", labelKey: "tabs.run" },
    { id: "results", labelKey: "tabs.results" },
  ];

  const loadConfig = useCallback(async () => {
    setPhase("status.loading");
    setError(null);
    try {
      const d = await fetchDefaultConfig();
      setFullYaml(d.content);
      setSavedSnap(d.content);
      setPhase("status.loaded");
    } catch (e) {
      setError(String(e));
      setPhase(null);
    }
  }, [activeStep]);

  useEffect(() => {
    fetchHealth()
      .then(() => setApiOk(true))
      .catch(() => setApiOk(false));
  }, []);

  useEffect(() => {
    if (tab === "configure") void loadConfig();
  }, [tab, loadConfig]);

  function setStepYamlSlice(nextSlice: string) {
    try {
      setFullYaml(mergeStepYamlIntoDefault(fullYaml, activeStep, nextSlice));
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }

  async function onSave() {
    setPhase("status.saving");
    setError(null);
    try {
      await saveDefaultConfig(fullYaml);
      setSavedSnap(fullYaml);
      setPhase("status.saved");
    } catch (e) {
      setError(String(e));
      setPhase(null);
    }
  }

  function applyScope(next: ScopeHierarchyData) {
    try {
      setStepYamlSlice(mergeScopeIntoStepYaml(stepYaml, next));
    } catch (e) {
      setError(String(e));
    }
  }

  function applyPatterns(next: PatternsData) {
    try {
      setStepYamlSlice(mergePatternsIntoStepYaml(stepYaml, next));
    } catch (e) {
      setError(String(e));
    }
  }

  async function onValidate() {
    setValidateOut(t("status.loading"));
    try {
      const validateStep = activeStep === "scope" ? "create" : activeStep;
      const out = await validateConfigs([validateStep]);
      setValidateOut(JSON.stringify(out, null, 2));
    } catch (e) {
      setValidateOut(String(e));
    }
  }

  useEffect(() => {
    const el = runLogRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [runLog]);

  const runPipelineBuffered = useCallback(
    async (step: "extract" | "create" | "write" | "all") => {
      const res = await fetch(apiUrl("/api/run"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ step }),
      });
      if (!res.ok) throw new Error(`run ${res.status}`);
      const out = (await res.json()) as {
        exit_code: number;
        stdout: string;
        stderr: string;
      };
      setRunLog(
        [
          `exit_code: ${out.exit_code}`,
          out.stderr ? `stderr:\n${out.stderr}` : "",
          out.stdout ? `stdout:\n${out.stdout}` : "",
        ]
          .filter(Boolean)
          .join("\n\n")
      );
    },
    []
  );

  const onRun = useCallback(
    async (step: "extract" | "create" | "write" | "all") => {
      setRunBusy(true);
      setRunLog(`${t("status.running")}\n`);
      const plannedList = stepsForRun(step);
      setRunPlannedSteps(plannedList);
      setRunStepStatuses(initialStepStatuses(step));
      const planned = new Set(plannedList);
      const applyStepStatus = (stepId: RunStepId, status: StepRunStatus) => {
        if (!planned.has(stepId)) return;
        setRunStepStatuses((prev) => ({ ...prev, [stepId]: status }));
      };

      const handleEvent = (ev: ProgressEvent) => {
        if (ev.event === "log" && typeof ev.message === "string") {
          const prefix = ev.level ? `[${ev.level}] ` : "";
          setRunLog((prev) => `${prev}${prefix}${ev.message}\n`);
          return;
        }
        const stepId = (ev.workflow_step ?? ev.task_id ?? "") as RunStepId;
        const fn = (ev.function_external_id ?? "").trim();
        if (ev.event === "task_start" && stepId) {
          applyStepStatus(stepId, "running");
          setRunLog(
            (prev) =>
              `${prev}${t("run.localTaskStart", {
                functionId: fn || stepId,
                taskId: stepId,
              })}\n`
          );
          return;
        }
        if (ev.event === "task_end" && stepId) {
          const raw = typeof ev.status === "string" ? ev.status.trim().toLowerCase() : "";
          const failed = raw === "failed";
          applyStepStatus(stepId, failed ? "failed" : "succeeded");
          setRunLog(
            (prev) =>
              `${prev}${t("run.localTaskEnd", {
                functionId: fn || stepId,
                taskId: stepId,
                status: failed ? t("run.stepStatus.failed") : t("run.stepStatus.succeeded"),
              })}\n`
          );
          if (ev.error) {
            setRunLog((prev) => `${prev}${ev.error}\n`);
          }
          return;
        }
        if (ev.event === "exit") {
          setRunLog((prev) => `${prev}\n${t("run.localRunExitLine", { code: ev.code ?? -1 })}\n`);
        }
      };

      try {
        const res = await fetch(apiUrl("/api/run-stream"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ step }),
        });
        if (res.status === 501) {
          setRunLog((prev) => `${prev}${t("run.streamUnsupported")}\n`);
          await runPipelineBuffered(step);
        } else if (!res.ok) {
          const errText = await res.text();
          throw new Error(errText || res.statusText);
        } else {
          const reader = res.body?.getReader();
          if (!reader) throw new Error("No response body");
          const dec = new TextDecoder();
          let buf = "";
          const handleLine = (line: string) => {
            const ev = parseProgressLine(line);
            if (ev) handleEvent(ev);
          };
          while (true) {
            const { done, value } = await reader.read();
            if (value) buf += dec.decode(value, { stream: true });
            const parts = buf.split("\n");
            buf = parts.pop() ?? "";
            for (const line of parts) handleLine(line);
            if (done) break;
          }
          buf += dec.decode();
          for (const line of buf.split("\n")) handleLine(line);
        }
        const items = await fetchRunResults();
        setRunItems(items.items);
      } catch (e) {
        setRunLog((prev) => `${prev}${String(e)}\n`);
      } finally {
        setRunBusy(false);
      }
    },
    [runPipelineBuffered, t]
  );

  async function refreshResults() {
    const items = await fetchRunResults();
    setRunItems(items.items);
  }

  async function onPreviewResult(path: string) {
    try {
      const d = await previewRunResult(path);
      setPreview(JSON.stringify(d.data, null, 2));
    } catch (e) {
      setPreview(String(e));
    }
  }

  const apiBadge =
    apiOk === true ? t("api.connected") : apiOk === false ? t("api.unreachable") : t("api.checking");

  return (
    <div className={`fas-app${tab === "configure" || tab === "run" ? " fas-app--wide" : ""}`}>
      <header className="fas-header">
        <div className="fas-header__shell">
          <div className="fas-header__brand">
            <CogniteLogo />
            <div className="fas-header__brand-text">
              <h1 className="fas-header__title">{t("app.title")}</h1>
              <p className="fas-header__subtitle">{t("app.subtitle")}</p>
            </div>
          </div>
          <div className="fas-header__toolbar">
            <span
              className={`fas-status${apiOk === true ? " fas-status--ok" : apiOk === false ? " fas-status--error" : ""}`}
              style={{ fontSize: "0.8rem" }}
            >
              {apiBadge}
            </span>
            <div className="fas-header__toolbar-group">
              <label className="fas-header__control" title={t("controls.theme.tooltip")}>
                <span className="fas-header__control-label">{t("controls.theme")}</span>
                <span className="fas-theme-toggle" role="group">
                  <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                    {t("controls.themeLight")}
                  </button>
                  <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                    {t("controls.themeDark")}
                  </button>
                </span>
              </label>
              <label className="fas-header__control" title={t("controls.language.tooltip")}>
                <span className="fas-header__control-label">{t("controls.language")}</span>
                <select value={locale} onChange={(e) => setLocale(e.target.value as typeof locale)}>
                  {LOCALES.map(({ code, label }) => (
                    <option key={code} value={code}>
                      {label}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </div>
        </div>
      </header>

      <div className="fas-nav-tabs-row">
        <nav className="fas-tabs" aria-label={t("nav.primary")}>
          {primaryTabs.map(({ id, labelKey }) => (
            <button key={id} type="button" className={tabClass(tab === id)} onClick={() => setTab(id)}>
              {t(labelKey)}
            </button>
          ))}
        </nav>
      </div>

      {tab === "configure" && (
        <section className="fas-panel fas-configure">
          <div className="fas-config-sidenav" aria-label={t("config.sidebarTitle")}>
            <p className="fas-config-sidenav__heading">{t("config.sidebarTitle")}</p>
            <p className="fas-hint fas-config-sidenav__path">{DEFAULT_CONFIG_REL}</p>
            <label className="fas-label fas-config-sidenav__search">
              <span className="fas-sr-only">{t("config.searchLabel")}</span>
              <input
                type="search"
                className="fas-input"
                value={configSearchQuery}
                onChange={(e) => setConfigSearchQuery(e.target.value)}
                placeholder={t("config.searchPlaceholder")}
                autoComplete="off"
                spellCheck={false}
              />
            </label>
            {filteredSteps.length === 0 ? (
              <p className="fas-hint fas-config-sidenav__empty" role="status">
                {t("config.noSearchResults")}
              </p>
            ) : (
              <div className="fas-config-sidenav__section fas-config-sidenav__section--scroll">
                {filteredSteps.map((s) => (
                  <button
                    key={s.id}
                    type="button"
                    title={`file_asset_source.${s.id}`}
                    className={`fas-config-sidenav__btn${activeStep === s.id ? " fas-config-sidenav__btn--active" : ""}`}
                    onClick={() => setActiveStep(s.id as ConfigStep)}
                  >
                    <span className="fas-config-sidenav__btn-primary">{t(s.labelKey)}</span>
                    <span className="fas-config-sidenav__btn-secondary">
                      {s.id === "scope" ? t("config.sidenav.scopePath") : t("config.sidenav.extractPath")}
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="fas-config-main">
            <div className="fas-callout" role="status">
              {configureHintKey ? t(configureHintKey) : null}
              <span style={{ display: "block", marginTop: "0.35rem" }}>{t("callout.configure")}</span>
            </div>

            <div className="fas-toolbar fas-config-main-toolbar">
              <button
                type="button"
                className="fas-btn fas-btn--primary fas-toolbar-btn"
                title={t("btn.save")}
                onClick={() => void onSave()}
                disabled={!dirty}
              >
                <IconToolbarSave />
                <span>{t("btn.save")}</span>
              </button>
              <button
                type="button"
                className="fas-btn fas-btn--ghost fas-toolbar-btn"
                title={t("btn.reload")}
                onClick={() => void loadConfig()}
              >
                <IconToolbarReload />
                <span>{t("btn.reload")}</span>
              </button>
              <button type="button" className="fas-btn fas-btn--ghost fas-toolbar-btn" onClick={() => void onValidate()}>
                {t("btn.validateAll")}
              </button>
              <div className="fas-config-main-toolbar__tail">
                <span
                  className={
                    phase === "status.saved" || phase === "status.loaded"
                      ? "fas-status fas-status--ok"
                      : error
                        ? "fas-status fas-status--error"
                        : "fas-status"
                  }
                >
                  {error ?? phaseLabel}
                </span>
                {dirty ? (
                  <span className="fas-hint fas-hint--warn" role="status">
                    {t("status.unsavedChanges")}
                  </span>
                ) : null}
              </div>
            </div>

            {activeStep === "scope" && scopeForm ? (
              <ScopeHierarchyEditor value={scopeForm} onChange={applyScope} />
            ) : null}

            {activeStep === "scope" && !scopeForm && stepYaml ? (
              <p className="fas-hint fas-status--error" role="alert">
                {t("scope.parseError")}
              </p>
            ) : null}

            {activeStep === "extract" && patternsForm ? (
              <PatternsEditor value={patternsForm} onChange={applyPatterns} />
            ) : null}

            {activeStep === "extract" && !patternsForm && stepYaml ? (
              <p className="fas-hint fas-status--error" role="alert">
                {t("configure.parseError.extract")}
              </p>
            ) : null}

            {validateOut ? <pre className="fas-log fas-log--scroll">{validateOut}</pre> : null}

            <AdvancedYamlPanel
              initialContent={fullYaml}
              onSaveRaw={async (content) => {
                await saveDefaultConfig(content);
                setFullYaml(content);
                setSavedSnap(content);
              }}
              onAfterSave={async () => {
                await loadConfig();
              }}
            />
          </div>
        </section>
      )}

      {tab === "run" && (
        <section className="fas-panel fas-run-console">
          <RunWorkflowStrip
            plannedSteps={runPlannedSteps}
            stepStatuses={runStepStatuses}
            runBusy={runBusy}
          />
          <div className="fas-callout" role="status">
            {t("run.hint")}
          </div>
          <div className="fas-toolbar">
            <button
              type="button"
              className="fas-btn"
              disabled={runBusy}
              onClick={() => void onRun("extract")}
            >
              {t("run.extract")}
            </button>
            <button
              type="button"
              className="fas-btn"
              disabled={runBusy}
              onClick={() => void onRun("create")}
            >
              {t("run.create")}
            </button>
            <button
              type="button"
              className="fas-btn"
              disabled={runBusy}
              onClick={() => void onRun("write")}
            >
              {t("run.write")}
            </button>
            <button
              type="button"
              className="fas-btn fas-btn--primary"
              disabled={runBusy}
              onClick={() => void onRun("all")}
            >
              {runBusy ? t("status.running") : t("run.all")}
            </button>
          </div>
          <textarea
            ref={runLogRef}
            readOnly
            className="fas-textarea fas-textarea--readonly fas-textarea--run-log"
            value={runLog}
            placeholder={t("run.outputPlaceholder")}
            spellCheck={false}
          />
        </section>
      )}

      {tab === "results" && (
        <section className="fas-panel">
          <div className="fas-toolbar">
            <button type="button" className="fas-btn fas-btn--ghost" onClick={() => void refreshResults()}>
              {t("results.refresh")}
            </button>
          </div>
          {runItems.length === 0 ? (
            <p className="fas-hint">{t("results.empty")}</p>
          ) : (
            <ul className="fas-results-list">
              {runItems.map((item) => (
                <li key={item.path}>
                  <button type="button" className="fas-results-list__btn" onClick={() => void onPreviewResult(item.path)}>
                    {item.path}
                  </button>
                </li>
              ))}
            </ul>
          )}
          {preview ? <pre className="fas-log fas-log--scroll">{preview}</pre> : null}
        </section>
      )}
    </div>
  );
}
