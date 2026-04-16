import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import YAML from "yaml";
import { AdvancedYamlPanel } from "./components/AdvancedYamlPanel";
import { AliasingControls } from "./components/AliasingControls";
import { ArtifactTree } from "./components/ArtifactTree";
import { KeyExtractionControls } from "./components/KeyExtractionControls";
import { ScopeHierarchyEditor } from "./components/ScopeHierarchyEditor";
import { SourceViewsControls } from "./components/SourceViewsControls";
import { useAppSettings } from "./context/AppSettingsContext";
import { LOCALES, type MessageKey } from "./i18n";
import type { AliasingScopeHierarchy, JsonObject } from "./types/scopeConfig";

type Tab = "scope" | "sourceViews" | "keyExtraction" | "aliasing" | "build" | "artifacts";

type PhaseKey = "status.loading" | "status.saving" | "status.saved" | "status.loaded";

const MODULE_FORM_KEYS: { key: string; labelKey: MessageKey }[] = [
  { key: "function_version", labelKey: "module.field.function_version" },
  { key: "organization", labelKey: "module.field.organization" },
  { key: "location_name", labelKey: "module.field.location_name" },
  { key: "source_name", labelKey: "module.field.source_name" },
  { key: "files_dataset", labelKey: "module.field.files_dataset" },
  { key: "schemaSpace", labelKey: "module.field.schemaSpace" },
  { key: "viewVersion", labelKey: "module.field.viewVersion" },
  { key: "workflow", labelKey: "module.field.workflow" },
  { key: "scope_build_mode", labelKey: "module.field.scope_build_mode" },
  { key: "key_extraction_aliasing_schedule", labelKey: "module.field.key_extraction_aliasing_schedule" },
  {
    key: "files_location_processing_group_source_id",
    labelKey: "module.field.files_location_processing_group_source_id",
  },
  { key: "functionVersion", labelKey: "module.field.functionVersion" },
  { key: "functionClientId", labelKey: "module.field.functionClientId" },
  { key: "functionClientSecret", labelKey: "module.field.functionClientSecret" },
];

function tabClass(active: boolean): string {
  return `kea-tab${active ? " kea-tab--active" : ""}`;
}

function isTriggerPath(path: string | null): boolean {
  return path != null && /\.WorkflowTrigger\.ya?ml$/i.test(path);
}

export default function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();

  const [tab, setTab] = useState<Tab>("scope");
  const [defaultDoc, setDefaultDoc] = useState<Record<string, unknown>>({});
  const [defaultRawYaml, setDefaultRawYaml] = useState("");
  const [defaultPhase, setDefaultPhase] = useState<PhaseKey | null>("status.loading");
  const [defaultError, setDefaultError] = useState<string | null>(null);
  const [savedDefaultSnap, setSavedDefaultSnap] = useState("");

  const [scopeDoc, setScopeDoc] = useState<Record<string, unknown>>({});
  const [scopeRawYaml, setScopeRawYaml] = useState("");
  const [scopePhase, setScopePhase] = useState<PhaseKey | null>("status.loading");
  const [scopeError, setScopeError] = useState<string | null>(null);
  const [savedScopeSnap, setSavedScopeSnap] = useState("");

  const [buildLog, setBuildLog] = useState("");
  const [buildOpen, setBuildOpen] = useState(false);
  const [artifactPaths, setArtifactPaths] = useState<string[]>([]);
  const [selectedArtifact, setSelectedArtifact] = useState<string | null>(null);
  const [artifactText, setArtifactText] = useState("");
  const [artifactPhase, setArtifactPhase] = useState<PhaseKey | null>(null);
  const [artifactTriggerSub, setArtifactTriggerSub] = useState<"views" | "extraction" | "aliasing">("views");
  const [artifactPlain, setArtifactPlain] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const settingsWrapRef = useRef<HTMLDivElement>(null);

  const api = useCallback(async <T,>(path: string, init?: RequestInit): Promise<T> => {
    const r = await fetch(path, init);
    if (!r.ok) {
      const errText = await r.text();
      throw new Error(errText || r.statusText);
    }
    return r.json() as Promise<T>;
  }, []);

  const loadAll = useCallback(async () => {
    setDefaultPhase("status.loading");
    setScopePhase("status.loading");
    setDefaultError(null);
    setScopeError(null);
    try {
      const [dDef, rawDef, dScope, rawScope, arts] = await Promise.all([
        api<Record<string, unknown>>("/api/default-config/model"),
        api<{ content: string }>("/api/default-config"),
        api<Record<string, unknown>>("/api/scope-document/model"),
        api<{ content: string }>("/api/scope-document"),
        api<{ paths: string[] }>("/api/artifacts"),
      ]);
      setDefaultDoc(dDef && typeof dDef === "object" ? dDef : {});
      setDefaultRawYaml(rawDef.content ?? "");
      setSavedDefaultSnap(JSON.stringify(dDef));
      setScopeDoc(dScope && typeof dScope === "object" ? dScope : {});
      setScopeRawYaml(rawScope.content ?? "");
      setSavedScopeSnap(JSON.stringify(dScope));
      setArtifactPaths(arts.paths ?? []);
      setDefaultPhase("status.loaded");
      setScopePhase("status.loaded");
    } catch (e) {
      setDefaultError(String(e));
      setScopeError(String(e));
      setDefaultPhase(null);
      setScopePhase(null);
    }
  }, [api]);

  useEffect(() => {
    document.title = t("app.title");
  }, [t]);

  useEffect(() => {
    loadAll().catch(() => undefined);
  }, [loadAll]);

  useEffect(() => {
    if (tab === "artifacts" || tab === "build") {
      api<{ paths: string[] }>("/api/artifacts").then((d) => setArtifactPaths(d.paths ?? [])).catch(() => undefined);
    }
  }, [tab, api]);

  useEffect(() => {
    if (!settingsOpen) return;
    const onDocDown = (e: MouseEvent) => {
      if (settingsWrapRef.current?.contains(e.target as Node)) return;
      setSettingsOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setSettingsOpen(false);
    };
    document.addEventListener("mousedown", onDocDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [settingsOpen]);

  const hierarchy = useMemo((): AliasingScopeHierarchy => {
    const h = defaultDoc.aliasing_scope_hierarchy;
    if (h && typeof h === "object" && !Array.isArray(h)) return h as AliasingScopeHierarchy;
    return { levels: [], locations: [] };
  }, [defaultDoc.aliasing_scope_hierarchy]);

  const setHierarchy = (next: AliasingScopeHierarchy) => {
    setDefaultDoc((d) => ({ ...d, aliasing_scope_hierarchy: next }));
  };

  const isDefaultDirty = useMemo(() => {
    if (!savedDefaultSnap) return false;
    return JSON.stringify(defaultDoc) !== savedDefaultSnap;
  }, [defaultDoc, savedDefaultSnap]);

  const isScopeDirty = useMemo(() => {
    if (!savedScopeSnap) return false;
    return JSON.stringify(scopeDoc) !== savedScopeSnap;
  }, [scopeDoc, savedScopeSnap]);

  const saveDefault = async () => {
    setDefaultPhase("status.saving");
    setDefaultError(null);
    try {
      await api("/api/default-config/model", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(defaultDoc),
      });
      const raw = await api<{ content: string }>("/api/default-config");
      setDefaultRawYaml(raw.content ?? "");
      setSavedDefaultSnap(JSON.stringify(defaultDoc));
      setDefaultPhase("status.saved");
    } catch (e) {
      setDefaultError(String(e));
      setDefaultPhase(null);
    }
  };

  const saveScope = async () => {
    setScopePhase("status.saving");
    setScopeError(null);
    try {
      await api("/api/scope-document/model", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(scopeDoc),
      });
      const raw = await api<{ content: string }>("/api/scope-document");
      setScopeRawYaml(raw.content ?? "");
      setSavedScopeSnap(JSON.stringify(scopeDoc));
      setScopePhase("status.saved");
    } catch (e) {
      setScopeError(String(e));
      setScopePhase(null);
    }
  };

  const runBuild = async (force: boolean, dryRun: boolean) => {
    setBuildOpen(true);
    setBuildLog(`${t("status.running")}\n`);
    const d = await api<{ exit_code: number; stdout: string; stderr: string }>("/api/build", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force, dry_run: dryRun }),
    });
    setBuildLog(
      `exit_code: ${d.exit_code}\n\n--- stdout ---\n${d.stdout}\n--- stderr ---\n${d.stderr}`
    );
    const art = await api<{ paths: string[] }>("/api/artifacts");
    setArtifactPaths(art.paths ?? []);
  };

  const openArtifact = async (rel: string) => {
    setSelectedArtifact(rel);
    setArtifactPhase("status.loading");
    setArtifactPlain(false);
    const d = await api<{ content: string }>(`/api/file?rel=${encodeURIComponent(rel)}`);
    setArtifactText(d.content);
    setArtifactPhase("status.loaded");
  };

  const saveArtifactFile = async () => {
    if (!selectedArtifact) return;
    setArtifactPhase("status.saving");
    await api(`/api/file?rel=${encodeURIComponent(selectedArtifact)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content: artifactText }),
    });
    setArtifactPhase("status.saved");
    const art = await api<{ paths: string[] }>("/api/artifacts");
    setArtifactPaths(art.paths ?? []);
  };

  /** Parsed trigger document when YAML is valid and looks like a WorkflowTrigger */
  const parsedArtifact = useMemo(() => {
    if (!selectedArtifact || !isTriggerPath(selectedArtifact)) return null;
    try {
      return YAML.parse(artifactText) as JsonObject;
    } catch {
      return null;
    }
  }, [artifactText, selectedArtifact]);

  const triggerInput = parsedArtifact?.input;
  const triggerConfiguration: JsonObject | null = (() => {
    if (!triggerInput || typeof triggerInput !== "object" || Array.isArray(triggerInput)) return null;
    const ti = triggerInput as JsonObject;
    const c = ti.configuration;
    if (c !== null && typeof c === "object" && !Array.isArray(c)) return c as JsonObject;
    return {};
  })();

  const updateTriggerConfiguration = (slice: Partial<JsonObject>) => {
    if (!parsedArtifact || !triggerInput) return;
    const ti = triggerInput as JsonObject;
    const base = ti.configuration;
    const prev =
      base !== null && typeof base === "object" && !Array.isArray(base) ? (base as JsonObject) : {};
    const nextConf = { ...prev, ...slice };
    const nextInput = { ...ti, configuration: nextConf };
    const nextDoc = { ...parsedArtifact, input: nextInput };
    setArtifactText(YAML.stringify(nextDoc, { lineWidth: 0 }));
  };

  const setTriggerSourceViews = (v: unknown) => updateTriggerConfiguration({ source_views: v });
  const setTriggerKeyExtraction = (v: unknown) => updateTriggerConfiguration({ key_extraction: v });
  const setTriggerAliasing = (v: unknown) => updateTriggerConfiguration({ aliasing: v });

  const setTriggerFullRescan = (v: boolean) => {
    if (!parsedArtifact || !triggerInput) return;
    const nextInput = { ...(triggerInput as JsonObject), full_rescan: v };
    const nextDoc = { ...parsedArtifact, input: nextInput };
    setArtifactText(YAML.stringify(nextDoc, { lineWidth: 0 }));
  };

  const setTriggerRunId = (v: string) => {
    if (!parsedArtifact || !triggerInput) return;
    const nextInput = { ...(triggerInput as JsonObject), run_id: v };
    const nextDoc = { ...parsedArtifact, input: nextInput };
    setArtifactText(YAML.stringify(nextDoc, { lineWidth: 0 }));
  };

  const defaultStatus = defaultError ?? (defaultPhase ? t(defaultPhase) : "");
  const scopeStatus = scopeError ?? (scopePhase ? t(scopePhase) : "");
  const artifactStatus = artifactPhase ? t(artifactPhase) : "";

  const tabs: { id: Tab; labelKey: MessageKey }[] = [
    { id: "scope", labelKey: "tabs.scope" },
    { id: "sourceViews", labelKey: "tabs.sourceViews" },
    { id: "keyExtraction", labelKey: "tabs.keyExtraction" },
    { id: "aliasing", labelKey: "tabs.aliasing" },
    { id: "build", labelKey: "tabs.build" },
    { id: "artifacts", labelKey: "tabs.artifacts" },
  ];

  return (
    <div className="kea-app">
      <header className="kea-header">
        <div className="kea-header__shell">
          <div className="kea-header__brand">
            <h1 className="kea-header__title">{t("app.title")}</h1>
            <p className="kea-header__subtitle">{t("app.subtitle")}</p>
          </div>
          <div className="kea-header__toolbar">
            <div className="kea-header__toolbar-group">
              <label className="kea-header__control" title={t("controls.theme.tooltip")}>
                <span className="kea-header__control-label">{t("controls.theme")}</span>
                <span className="kea-theme-toggle" role="group">
                  <button
                    type="button"
                    data-active={theme === "light"}
                    onClick={() => setTheme("light")}
                  >
                    {t("controls.themeLight")}
                  </button>
                  <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                    {t("controls.themeDark")}
                  </button>
                </span>
              </label>
              <label className="kea-header__control" title={t("controls.language.tooltip")}>
                <span className="kea-header__control-label">{t("controls.language")}</span>
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

      <div className="kea-nav-tabs-row">
        <nav className="kea-tabs" aria-label={t("nav.primary")}>
          {tabs.map(({ id, labelKey }) => (
            <button
              key={id}
              type="button"
              className={tabClass(tab === id)}
              onClick={() => {
                setTab(id);
                setSettingsOpen(false);
              }}
            >
              {t(labelKey)}
            </button>
          ))}
        </nav>
        <div className="kea-tabs-actions" ref={settingsWrapRef}>
          <button
            type="button"
            className={`kea-tabs-gear${settingsOpen ? " kea-tabs-gear--open" : ""}`}
            aria-expanded={settingsOpen}
            aria-haspopup="dialog"
            title={t("nav.settings.tooltip")}
            onClick={() => setSettingsOpen((o) => !o)}
          >
            <span className="kea-tabs-gear__icon" aria-hidden>
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path
                  d="M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z"
                  stroke="currentColor"
                  strokeWidth="1.75"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
                <path
                  d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1Z"
                  stroke="currentColor"
                  strokeWidth="1.75"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </span>
            <span className="kea-sr-only">{t("nav.settings")}</span>
          </button>
          {settingsOpen && (
            <div
              className="kea-settings-popover"
              role="dialog"
              aria-labelledby="kea-settings-popover-title"
            >
              <h2 id="kea-settings-popover-title" className="kea-settings-popover__title">
                {t("moduleSettings.title")}
              </h2>
              <p className="kea-hint" style={{ marginTop: 0 }}>
                {t("callout.settings")}
              </p>
              <div className="kea-toolbar" style={{ marginBottom: "0.75rem" }}>
                <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => loadAll()}>
                  {t("btn.reload")}
                </button>
                <button type="button" className="kea-btn kea-btn--primary kea-btn--sm" onClick={() => void saveDefault()}>
                  {t("btn.saveDefault")}
                </button>
                <span
                  className={
                    defaultPhase === "status.saved" || defaultPhase === "status.loaded"
                      ? "kea-status kea-status--ok"
                      : "kea-status"
                  }
                  style={{ fontSize: "0.8rem" }}
                >
                  {defaultStatus}
                </span>
                {isDefaultDirty && (
                  <span className="kea-hint kea-hint--warn" role="status" style={{ fontSize: "0.8rem" }}>
                    {t("status.unsavedChanges")}
                  </span>
                )}
              </div>
              <div className="kea-loc-fields kea-settings-popover__fields">
                {MODULE_FORM_KEYS.map(({ key, labelKey }) => (
                  <label key={key} className="kea-label">
                    {t(labelKey)}
                    <input
                      className="kea-input"
                      value={defaultDoc[key] != null ? String(defaultDoc[key]) : ""}
                      onChange={(e) =>
                        setDefaultDoc((d) => ({
                          ...d,
                          [key]: e.target.value,
                        }))
                      }
                    />
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {tab === "scope" && (
        <section className="kea-panel">
          <div className="kea-callout" role="status">
            {t("callout.defaultConfig")}
          </div>
          <div className="kea-toolbar">
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => loadAll()}>
              {t("btn.reload")}
            </button>
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void saveDefault()}>
              {t("btn.saveDefault")}
            </button>
            <span className={defaultPhase === "status.saved" || defaultPhase === "status.loaded" ? "kea-status kea-status--ok" : "kea-status"}>
              {defaultStatus}
            </span>
            {isDefaultDirty && (
              <span className="kea-hint kea-hint--warn" role="status">
                {t("status.unsavedChanges")}
              </span>
            )}
          </div>
          <ScopeHierarchyEditor value={hierarchy} onChange={setHierarchy} />
          <AdvancedYamlPanel
            initialContent={defaultRawYaml}
            onSaveRaw={async (content) => {
              await api("/api/default-config", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ content }),
              });
            }}
            onAfterSave={async () => {
              const [model, raw] = await Promise.all([
                api<Record<string, unknown>>("/api/default-config/model"),
                api<{ content: string }>("/api/default-config"),
              ]);
              setDefaultDoc(model && typeof model === "object" ? model : {});
              setDefaultRawYaml(raw.content ?? "");
              setSavedDefaultSnap(JSON.stringify(model));
            }}
          />
        </section>
      )}

      {tab === "sourceViews" && (
        <section className="kea-panel">
          <div className="kea-callout" role="status">
            {t("callout.scopeDoc")}
          </div>
          <div className="kea-toolbar">
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => loadAll()}>
              {t("btn.reload")}
            </button>
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void saveScope()}>
              {t("btn.saveScope")}
            </button>
            <span className={scopePhase === "status.saved" || scopePhase === "status.loaded" ? "kea-status kea-status--ok" : "kea-status"}>
              {scopeStatus}
            </span>
            {isScopeDirty && (
              <span className="kea-hint kea-hint--warn" role="status">
                {t("status.unsavedChanges")}
              </span>
            )}
          </div>
          <SourceViewsControls
            value={scopeDoc.source_views}
            onChange={(v) => setScopeDoc((d) => ({ ...d, source_views: v }))}
          />
          <AdvancedYamlPanel
            initialContent={scopeRawYaml}
            onSaveRaw={async (content) => {
              await api("/api/scope-document", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ content }),
              });
            }}
            onAfterSave={async () => {
              const [model, raw] = await Promise.all([
                api<Record<string, unknown>>("/api/scope-document/model"),
                api<{ content: string }>("/api/scope-document"),
              ]);
              setScopeDoc(model && typeof model === "object" ? model : {});
              setScopeRawYaml(raw.content ?? "");
              setSavedScopeSnap(JSON.stringify(model));
            }}
          />
        </section>
      )}

      {tab === "keyExtraction" && (
        <section className="kea-panel">
          <div className="kea-callout" role="status">
            {t("callout.scopeDoc")}
          </div>
          <div className="kea-toolbar">
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => loadAll()}>
              {t("btn.reload")}
            </button>
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void saveScope()}>
              {t("btn.saveScope")}
            </button>
            <span className={scopePhase === "status.saved" || scopePhase === "status.loaded" ? "kea-status kea-status--ok" : "kea-status"}>
              {scopeStatus}
            </span>
            {isScopeDirty && (
              <span className="kea-hint kea-hint--warn" role="status">
                {t("status.unsavedChanges")}
              </span>
            )}
          </div>
          <KeyExtractionControls
            value={scopeDoc.key_extraction}
            onChange={(v) => setScopeDoc((d) => ({ ...d, key_extraction: v }))}
          />
          <AdvancedYamlPanel
            initialContent={scopeRawYaml}
            onSaveRaw={async (content) => {
              await api("/api/scope-document", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ content }),
              });
            }}
            onAfterSave={async () => {
              const [model, raw] = await Promise.all([
                api<Record<string, unknown>>("/api/scope-document/model"),
                api<{ content: string }>("/api/scope-document"),
              ]);
              setScopeDoc(model && typeof model === "object" ? model : {});
              setScopeRawYaml(raw.content ?? "");
              setSavedScopeSnap(JSON.stringify(model));
            }}
          />
        </section>
      )}

      {tab === "aliasing" && (
        <section className="kea-panel">
          <div className="kea-callout" role="status">
            {t("callout.scopeDoc")}
          </div>
          <div className="kea-toolbar">
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => loadAll()}>
              {t("btn.reload")}
            </button>
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void saveScope()}>
              {t("btn.saveScope")}
            </button>
            <span className={scopePhase === "status.saved" || scopePhase === "status.loaded" ? "kea-status kea-status--ok" : "kea-status"}>
              {scopeStatus}
            </span>
            {isScopeDirty && (
              <span className="kea-hint kea-hint--warn" role="status">
                {t("status.unsavedChanges")}
              </span>
            )}
          </div>
          <AliasingControls
            value={scopeDoc.aliasing}
            onChange={(v) => setScopeDoc((d) => ({ ...d, aliasing: v }))}
          />
          <AdvancedYamlPanel
            initialContent={scopeRawYaml}
            onSaveRaw={async (content) => {
              await api("/api/scope-document", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ content }),
              });
            }}
            onAfterSave={async () => {
              const [model, raw] = await Promise.all([
                api<Record<string, unknown>>("/api/scope-document/model"),
                api<{ content: string }>("/api/scope-document"),
              ]);
              setScopeDoc(model && typeof model === "object" ? model : {});
              setScopeRawYaml(raw.content ?? "");
              setSavedScopeSnap(JSON.stringify(model));
            }}
          />
        </section>
      )}

      {tab === "build" && (
        <section className="kea-panel">
          <h3 className="kea-section-title">{t("build.panelTitle")}</h3>
          <div className="kea-toolbar">
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void runBuild(false, false)}>
              {t("btn.runBuild")}
            </button>
            <button
              type="button"
              className="kea-btn"
              onClick={() => {
                if (!window.confirm(t("build.confirmForce"))) return;
                void runBuild(true, false);
              }}
            >
              {t("btn.runBuildForce")}
            </button>
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => void runBuild(false, true)}>
              {t("btn.dryRun")}
            </button>
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => setBuildOpen((o) => !o)}>
              {t("build.toggleShow")}
            </button>
          </div>
          <p className="kea-hint kea-hint--warn" style={{ marginTop: 8, maxWidth: "62ch" }}>
            {t("build.warnForce")}
          </p>
          {buildOpen && (
            <textarea
              readOnly
              className="kea-textarea kea-textarea--readonly"
              value={buildLog}
              placeholder={t("build.outputPlaceholder")}
              style={{ minHeight: 280, marginTop: 12 }}
            />
          )}
        </section>
      )}

      {tab === "artifacts" && (
        <section className="kea-panel kea-artifacts">
          <div className="kea-artifact-sidebar">
            <p className="kea-artifact-list-title">{t("artifacts.browse")}</p>
            <ArtifactTree
              paths={artifactPaths}
              selectedPath={selectedArtifact}
              onSelectFile={(rel) => {
                void openArtifact(rel);
              }}
            />
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              style={{ marginTop: 10 }}
              onClick={() => api<{ paths: string[] }>("/api/artifacts").then((d) => setArtifactPaths(d.paths ?? []))}
            >
              {t("btn.refreshList")}
            </button>
          </div>
          <div className="kea-artifact-editor">
            <div className="kea-toolbar">
              <button
                type="button"
                className="kea-btn kea-btn--primary"
                onClick={() => void saveArtifactFile()}
                disabled={!selectedArtifact}
              >
                {t("btn.saveFile")}
              </button>
              <span className={artifactPhase === "status.saved" || artifactPhase === "status.loaded" ? "kea-status kea-status--ok" : "kea-status"}>
                {artifactStatus}
              </span>
              {selectedArtifact && isTriggerPath(selectedArtifact) && (
                <label className="kea-label" style={{ marginLeft: "auto", alignItems: "center", gap: 8 }}>
                  <input
                    type="checkbox"
                    checked={artifactPlain}
                    onChange={(e) => setArtifactPlain(e.target.checked)}
                  />
                  {t("artifacts.plainEditor")}
                </label>
              )}
            </div>
            {selectedArtifact && isTriggerPath(selectedArtifact) && triggerConfiguration && !artifactPlain ? (
              <>
                <p className="kea-hint" style={{ marginBottom: 8 }}>
                  {t("artifacts.triggerBar")}
                </p>
                <div className="kea-trigger-inputs">
                  <label className="kea-label">
                    {t("artifacts.fullRescan")}
                    <input
                      type="checkbox"
                      checked={Boolean((triggerInput as JsonObject)?.full_rescan)}
                      onChange={(e) => setTriggerFullRescan(e.target.checked)}
                    />
                  </label>
                  <label className="kea-label">
                    {t("artifacts.runId")}
                    <input
                      className="kea-input"
                      value={String((triggerInput as JsonObject)?.run_id ?? "")}
                      onChange={(e) => setTriggerRunId(e.target.value)}
                    />
                  </label>
                </div>
                <div className="kea-trigger-subtabs" role="tablist">
                  {(
                    [
                      ["views", "artifacts.sub.views"] as const,
                      ["extraction", "artifacts.sub.extraction"] as const,
                      ["aliasing", "artifacts.sub.aliasing"] as const,
                    ] as const
                  ).map(([id, lk]) => (
                    <button
                      key={id}
                      type="button"
                      role="tab"
                      className={tabClass(artifactTriggerSub === id)}
                      onClick={() => setArtifactTriggerSub(id)}
                    >
                      {t(lk)}
                    </button>
                  ))}
                </div>
                {artifactTriggerSub === "views" && (
                  <SourceViewsControls
                    value={triggerConfiguration.source_views}
                    onChange={(v) => setTriggerSourceViews(v)}
                  />
                )}
                {artifactTriggerSub === "extraction" && (
                  <KeyExtractionControls
                    value={triggerConfiguration.key_extraction}
                    onChange={(v) => setTriggerKeyExtraction(v)}
                  />
                )}
                {artifactTriggerSub === "aliasing" && (
                  <AliasingControls
                    value={triggerConfiguration.aliasing}
                    onChange={(v) => setTriggerAliasing(v)}
                  />
                )}
              </>
            ) : (
              <textarea
                className="kea-textarea"
                value={artifactText}
                onChange={(e) => setArtifactText(e.target.value)}
                spellCheck={false}
                style={{ minHeight: 420 }}
              />
            )}
          </div>
        </section>
      )}
    </div>
  );
}
