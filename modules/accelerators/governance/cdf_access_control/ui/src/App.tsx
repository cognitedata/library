import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import YAML from "yaml";
import { AdvancedYamlPanel } from "./components/AdvancedYamlPanel";
import { ArtifactTree } from "./components/ArtifactTree";
import { DimensionsEditor } from "./components/DimensionsEditor";
import { GroupsEditor } from "./components/GroupsEditor";
import { SpacesEditor } from "./components/SpacesEditor";
import { DeferredCommitInput } from "./components/DeferredCommitTextField";
import { useAppSettings } from "./context/AppSettingsContext";
import { LOCALES, type Locale, type MessageKey } from "./i18n";
import type { GovernanceDocument } from "./types/governanceConfig";
import { emptyGovernanceDocument } from "./types/governanceConfig";
import { groupNameFromYaml, literalSourceIdFromGroupYaml } from "./utils/groupYamlSourceId";

type Tab = "dimensions" | "spaces" | "groups" | "build" | "artifacts";

type PhaseKey = "status.loading" | "status.saving" | "status.saved" | "status.loaded";

const API = "";
const PRIMARY_CONFIG = "default.config.yaml";

function apiHeaders(): HeadersInit {
  return { "X-Config-Rel": PRIMARY_CONFIG };
}

function tabClass(active: boolean): string {
  return `gov-tab${active ? " gov-tab--active" : ""}`;
}

export function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();
  const [tab, setTab] = useState<Tab>("dimensions");
  const [doc, setDoc] = useState<GovernanceDocument>(() => emptyGovernanceDocument());
  const [savedSnapshot, setSavedSnapshot] = useState("");
  const [phase, setPhase] = useState<PhaseKey | null>(null);
  const [errorDetail, setErrorDetail] = useState<string | null>(null);
  const [buildLog, setBuildLog] = useState("");
  const [buildBusy, setBuildBusy] = useState(false);
  const [artifactPaths, setArtifactPaths] = useState<string[]>([]);
  const [artifactRel, setArtifactRel] = useState<string | null>(null);
  const [artifactYaml, setArtifactYaml] = useState("");
  const [artifactPhase, setArtifactPhase] = useState<PhaseKey | null>(null);
  const [entraDraft, setEntraDraft] = useState("");
  const [entraStatus, setEntraStatus] = useState("");
  const [sourceIdHint, setSourceIdHint] = useState("");
  const loadGen = useRef(0);

  const dirty = useMemo(() => savedSnapshot !== JSON.stringify(doc), [doc, savedSnapshot]);
  const phaseOk = phase === "status.saved" || phase === "status.loaded";
  const statusLabel = phase ? t(phase) : errorDetail ?? "";

  const loadModel = useCallback(async () => {
    const gen = ++loadGen.current;
    setPhase("status.loading");
    setErrorDetail(null);
    const r = await fetch(`${API}/api/config/model`, { headers: apiHeaders() });
    if (!r.ok) {
      setPhase(null);
      setErrorDetail(String(r.status));
      return;
    }
    const data = (await r.json()) as GovernanceDocument;
    if (gen !== loadGen.current) return;
    const merged: GovernanceDocument = {
      ...emptyGovernanceDocument(),
      ...data,
      dimensions: data.dimensions ?? {},
      spaces: { ...emptyGovernanceDocument().spaces, ...data.spaces },
      groups: { ...emptyGovernanceDocument().groups, ...data.groups },
    };
    setDoc(merged);
    setSavedSnapshot(JSON.stringify(merged));
    setPhase("status.loaded");
  }, []);

  useEffect(() => {
    void loadModel();
  }, [loadModel]);

  const saveModel = async () => {
    setPhase("status.saving");
    setErrorDetail(null);
    const r = await fetch(`${API}/api/config/model`, {
      method: "PUT",
      headers: { "Content-Type": "application/json", ...apiHeaders() },
      body: JSON.stringify(doc),
    });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      setPhase(null);
      setErrorDetail(String((body as { detail?: string }).detail ?? r.status));
      return;
    }
    setSavedSnapshot(JSON.stringify(doc));
    setPhase("status.saved");
  };

  const mirrorAccessControl = async () => {
    setPhase("status.loading");
    setErrorDetail(null);
    const r = await fetch(`${API}/api/config/mirror`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: {
          dimensions: doc.dimensions,
          spaces: doc.spaces,
          groups: doc.groups,
        },
      }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) {
      setPhase(null);
      setErrorDetail(String((body as { detail?: string }).detail ?? r.status));
      return;
    }
    setPhase("status.loaded");
    setErrorDetail(
      t("mirror.done", {
        written: String((body as { written?: string[] }).written?.join(", ") ?? ""),
        skipped: String((body as { skipped?: string[] }).skipped?.join(", ") ?? ""),
      })
    );
    await loadModel();
  };

  const formatBuildLog = (d: {
    exit_code?: number;
    stdout?: string;
    stderr?: string;
  }) =>
    `exit_code: ${d.exit_code ?? "?"}\n\n--- stdout ---\n${d.stdout ?? ""}\n--- stderr ---\n${d.stderr ?? ""}`;

  const runBuild = async (force: boolean, dryRun = false) => {
    if (force && !dryRun && !window.confirm(t("build.confirmForce"))) return;
    setBuildBusy(true);
    setBuildLog(`${t("status.running")}\n`);
    try {
      const r = await fetch(`${API}/api/build`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...apiHeaders() },
        body: JSON.stringify({ force, dry_run: dryRun }),
      });
      const body = (await r.json().catch(() => ({}))) as {
        detail?: unknown;
        exit_code?: number;
        stdout?: string;
        stderr?: string;
      };
      if (!r.ok) {
        const detail =
          typeof body.detail === "string"
            ? body.detail
            : body.detail != null
              ? JSON.stringify(body.detail)
              : String(r.status);
        setBuildLog(`${t("status.loading")}\n\n${detail}`);
        return;
      }
      setBuildLog(formatBuildLog(body));
      await refreshArtifacts();
    } catch (e) {
      setBuildLog(String(e));
    } finally {
      setBuildBusy(false);
    }
  };

  const refreshArtifacts = useCallback(async () => {
    const r = await fetch(`${API}/api/artifacts?kind=all`);
    if (!r.ok) return;
    const data = await r.json();
    const paths = [...(data.spaces ?? []), ...(data.groups ?? [])].sort();
    setArtifactPaths(paths);
  }, []);

  useEffect(() => {
    if (tab === "artifacts") void refreshArtifacts();
  }, [tab, refreshArtifacts]);

  const loadArtifact = async (rel: string) => {
    setArtifactPhase("status.loading");
    const r = await fetch(`${API}/api/file?rel=${encodeURIComponent(rel)}`);
    if (!r.ok) {
      setArtifactPhase(null);
      return;
    }
    const data = await r.json();
    setArtifactRel(rel);
    const content = data.content ?? "";
    setArtifactYaml(content);
    const groupName = groupNameFromYaml(content);
    const sid = groupName ? doc.groups?.global?.source_ids?.[groupName] : undefined;
    setEntraDraft(sid ?? literalSourceIdFromGroupYaml(content) ?? "");
    setEntraStatus("");
    setSourceIdHint("");
    setArtifactPhase("status.loaded");
  };

  const saveArtifact = async () => {
    if (!artifactRel) return;
    setArtifactPhase("status.saving");
    const r = await fetch(`${API}/api/file?rel=${encodeURIComponent(artifactRel)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json", ...apiHeaders() },
      body: JSON.stringify({ content: artifactYaml }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) {
      setArtifactPhase(null);
      return;
    }
    if ((body as { source_ids_synced?: boolean }).source_ids_synced) {
      setArtifactPhase("status.saved");
      await loadModel();
    } else {
      setArtifactPhase("status.loaded");
    }
  };

  const saveEntraToConfig = () => {
    if (!artifactRel?.includes(".Group.yaml")) return;
    const name = groupNameFromYaml(artifactYaml);
    if (!name) {
      setEntraStatus(t("artifacts.error.noGroupName"));
      return;
    }
    setDoc({
      ...doc,
      groups: {
        ...doc.groups,
        global: {
          ...doc.groups?.global,
          source_ids: {
            ...(doc.groups?.global?.source_ids ?? {}),
            [name]: entraDraft.trim(),
          },
        },
      },
    });
    setEntraStatus(t("artifacts.hint.entraSavedConfig"));
  };

  const checkEntraGuid = async () => {
    const r = await fetch(`${API}/api/source-id-hint?source_id=${encodeURIComponent(entraDraft)}`);
    if (!r.ok) return;
    const data = await r.json();
    if (data.empty) setSourceIdHint(t("sourceId.empty"));
    else if (data.valid) setSourceIdHint(t("sourceId.valid"));
    else setSourceIdHint(t("sourceId.invalid"));
  };

  const rawYaml = useMemo(
    () => YAML.stringify(doc, { lineWidth: 0, defaultKeyType: "PLAIN", defaultStringType: "QUOTE_DOUBLE" }),
    [doc]
  );

  const wideTab = tab === "dimensions" || tab === "artifacts";

  const configToolbar = (
    <div className="gov-config-toolbar">
      <button type="button" className="gov-btn gov-btn--ghost gov-btn--sm" onClick={() => void loadModel()}>
        {t("btn.reload")}
      </button>
      <button
        type="button"
        className="gov-btn gov-btn--primary gov-btn--sm"
        disabled={!dirty}
        onClick={() => void saveModel()}
      >
        {t("btn.saveConfiguration")}
      </button>
      <button type="button" className="gov-btn gov-btn--sm" onClick={() => void mirrorAccessControl()}>
        {t("btn.mirrorAccessControl")}
      </button>
      <span className={phaseOk ? "gov-status gov-status--ok" : "gov-status"}>{statusLabel}</span>
      {dirty && (
        <span className="gov-hint gov-hint--warn" role="status">
          {t("status.unsavedChanges")}
        </span>
      )}
    </div>
  );

  const advancedYamlPanel = (
    <AdvancedYamlPanel
      initialContent={rawYaml}
      onSaveRaw={async (content) => {
        const r = await fetch(`${API}/api/config`, {
          method: "PUT",
          headers: { "Content-Type": "application/json", ...apiHeaders() },
          body: JSON.stringify({ content }),
        });
        if (!r.ok) throw new Error(String(r.status));
      }}
      onAfterSave={loadModel}
    />
  );

  return (
    <div className={`gov-app${wideTab ? " gov-app--wide" : ""}`}>
      <header className="gov-header">
        <div className="gov-header__shell">
          <div className="gov-header__brand">
            <h1 className="gov-header__title">{t("app.title")}</h1>
            <p className="gov-header__subtitle">{t("app.subtitle")}</p>
          </div>
          <div className="gov-header__toolbar">
            <div className="gov-header__toolbar-group">
              <label className="gov-header__control" title={t("controls.theme.tooltip")}>
                <span className="gov-header__control-label">{t("controls.theme")}</span>
                <span className="gov-theme-toggle" role="group">
                  <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                    {t("controls.themeLight")}
                  </button>
                  <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                    {t("controls.themeDark")}
                  </button>
                </span>
              </label>
              <label className="gov-header__control" title={t("controls.language.tooltip")}>
                <span className="gov-header__control-label">{t("controls.language")}</span>
                <select value={locale} onChange={(e) => setLocale(e.target.value as Locale)}>
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

      <div className="gov-nav-tabs-row">
        <nav className="gov-tabs" aria-label={t("nav.primary")}>
          {(["dimensions", "spaces", "groups", "build", "artifacts"] as Tab[]).map((id) => (
            <button key={id} type="button" className={tabClass(tab === id)} onClick={() => setTab(id)}>
              {t(`tabs.${id}` as MessageKey)}
            </button>
          ))}
        </nav>
      </div>

      {tab !== "build" && tab !== "artifacts" && (
        <div className="gov-callout" role="status">
          {t("callout.structuredPath", { path: PRIMARY_CONFIG })}
        </div>
      )}

      {tab === "dimensions" && (
        <section className="gov-panel">
          {configToolbar}
          <DimensionsEditor doc={doc} onChange={setDoc} />
          {advancedYamlPanel}
        </section>
      )}

      {tab === "spaces" && (
        <section className="gov-panel">
          {configToolbar}
          <SpacesEditor doc={doc} onChange={setDoc} />
          {advancedYamlPanel}
        </section>
      )}

      {tab === "groups" && (
        <section className="gov-panel">
          {configToolbar}
          <GroupsEditor doc={doc} onChange={setDoc} />
          {advancedYamlPanel}
        </section>
      )}

      {tab === "build" && (
        <section className="gov-panel">
          <h3 className="gov-section-title">{t("tabs.build")}</h3>
          <div className="gov-toolbar">
            <button
              type="button"
              className="gov-btn gov-btn--primary"
              disabled={buildBusy}
              onClick={() => void runBuild(false, false)}
            >
              {t("btn.runBuild")}
            </button>
            <button
              type="button"
              className="gov-btn"
              disabled={buildBusy}
              onClick={() => void runBuild(true, false)}
            >
              {t("btn.runBuildForce")}
            </button>
            <button
              type="button"
              className="gov-btn gov-btn--ghost"
              disabled={buildBusy}
              onClick={() => void runBuild(false, true)}
            >
              {t("btn.dryRun")}
            </button>
            {buildBusy && <span className="gov-status">{t("status.running")}</span>}
          </div>
          <p className="gov-hint" style={{ marginTop: 0, marginBottom: 8 }}>
            {t("controls.activeConfig")}: <code>{PRIMARY_CONFIG}</code>
          </p>
          <p className="gov-hint gov-hint--warn" style={{ marginTop: 8, maxWidth: "62ch" }}>
            {t("build.warnForce")}
          </p>
          <textarea
            readOnly
            className="gov-textarea gov-textarea--readonly"
            value={buildLog}
            placeholder={t("build.outputPlaceholder")}
            style={{ minHeight: 280, marginTop: 12 }}
            spellCheck={false}
          />
        </section>
      )}

      {tab === "artifacts" && (
        <section className="gov-panel gov-artifacts">
          <div className="gov-artifact-sidebar">
            <p className="gov-artifact-list-title">{t("artifacts.browse")}</p>
            <div className="gov-artifact-tree-wrap">
              <ArtifactTree
                paths={artifactPaths}
                selectedPath={artifactRel}
                onSelectFile={(rel) => void loadArtifact(rel)}
              />
            </div>
            <button type="button" className="gov-btn gov-btn--sm" style={{ marginTop: 10 }} onClick={() => void refreshArtifacts()}>
              {t("btn.refreshList")}
            </button>
          </div>
          <div className="gov-artifact-editor">
            <div className="gov-toolbar">
              <button
                type="button"
                className="gov-btn gov-btn--primary"
                onClick={() => void saveArtifact()}
                disabled={!artifactRel}
              >
                {t("btn.saveFile")}
              </button>
              <span
                className={
                  artifactPhase === "status.saved" || artifactPhase === "status.loaded"
                    ? "gov-status gov-status--ok"
                    : "gov-status"
                }
              >
                {artifactPhase ? t(artifactPhase) : ""}
              </span>
            </div>
            {artifactRel ? (
              <>
                <p className="gov-hint" style={{ margin: 0, fontFamily: "JetBrains Mono, ui-monospace, monospace", fontSize: "0.78rem" }}>
                  {artifactRel}
                </p>
                {artifactRel.includes(".Group.yaml") && (
                  <div className="gov-callout gov-stack" style={{ marginBottom: 0 }}>
                    <label className="gov-label" title={t("artifacts.sourceIdCheck.tooltip")}>
                      {t("artifacts.sourceIdCheck")}
                      <div className="gov-inline-row">
                        <DeferredCommitInput
                          className="gov-input"
                          committedValue={entraDraft}
                          onCommit={setEntraDraft}
                          placeholder={t("artifacts.pasteGuid")}
                        />
                        <button type="button" className="gov-btn gov-btn--sm" onClick={() => void checkEntraGuid()}>
                          {t("artifacts.sourceIdCheck")}
                        </button>
                        <button type="button" className="gov-btn gov-btn--sm gov-btn--primary" onClick={saveEntraToConfig}>
                          {t("btn.saveConfiguration")}
                        </button>
                      </div>
                    </label>
                    {sourceIdHint && <p className="gov-hint">{sourceIdHint}</p>}
                    {entraStatus && <p className="gov-hint">{entraStatus}</p>}
                  </div>
                )}
                <textarea
                  className="gov-textarea"
                  value={artifactYaml}
                  onChange={(e) => setArtifactYaml(e.target.value)}
                  spellCheck={false}
                  style={{ minHeight: 420 }}
                />
              </>
            ) : (
              <p className="gov-hint">{t("artifacts.browse")}</p>
            )}
          </div>
        </section>
      )}
    </div>
  );
}
