import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import "@xyflow/react/dist/style.css";
import YAML from "yaml";
import { AdvancedYamlPanel } from "./components/AdvancedYamlPanel";
import { AliasingControls } from "./components/AliasingControls";
import { ArtifactTree } from "./components/ArtifactTree";
import { KeyExtractionControls } from "./components/KeyExtractionControls";
import { ScopeHierarchyEditor } from "./components/ScopeHierarchyEditor";
import { WorkflowFlowCanvasPreview } from "./components/flow/WorkflowFlowCanvasPreview";
import type { WorkflowPreviewLocalRun } from "./components/flow/WorkflowFlowCanvasPreview";
import { WorkflowFlowPanel } from "./components/flow/WorkflowFlowPanel";
import { syncWorkflowScopeFromCanvas } from "./components/flow/canvasScopeSync";
import { canvasDocWithScopeSeedIfEmpty } from "./components/flow/seedCanvasFromScope";
import { MatchDefinitionsScopePanel } from "./components/MatchDefinitionsScopePanel";
import { SourceViewsControls } from "./components/SourceViewsControls";
import { useAppSettings } from "./context/AppSettingsContext";
import { LOCALES, type MessageKey } from "./i18n";
import type { AliasingScopeHierarchy, JsonObject } from "./types/scopeConfig";
import {
  emptyWorkflowCanvasDocument,
  parseWorkflowCanvasDocument,
  type WorkflowCanvasDocument,
} from "./types/workflowCanvas";
import { displayNameFromRoot } from "./utils/configDisplayName";
import { matchConfigSearch } from "./utils/configPanelSearch";

type Tab = "scope" | "configure" | "build" | "artifacts";

type ConfigSubTab =
  | "sourceViews"
  | "matchDefinitions"
  | "keyExtraction"
  | "aliasing"
  | "flowCanvas"
  | "runPipeline";

type TriggerTopTab = "triggerAuth" | "schedule" | "pipeline";

type PhaseKey = "status.loading" | "status.saving" | "status.saved" | "status.loaded";

type ConfigureTarget =
  | { id: "workflowLocal" }
  | { id: "workflowTemplate" }
  | { id: "trigger"; path: string };

const SCOPE_REL = "workflow.local.config.yaml";
const TEMPLATE_REL = "workflow_template/workflow.template.config.yaml";

/** Embedded in WorkflowTrigger `input.configuration` (same schema as sibling `.canvas.yaml` for local/template). */
const TRIGGER_WORKFLOW_CANVAS_KEY = "workflow_canvas";

const MODULE_FORM_KEYS: { key: string; labelKey: MessageKey }[] = [
  { key: "function_version", labelKey: "module.field.function_version" },
  { key: "organization", labelKey: "module.field.organization" },
  { key: "location_name", labelKey: "module.field.location_name" },
  { key: "source_name", labelKey: "module.field.source_name" },
  { key: "files_dataset", labelKey: "module.field.files_dataset" },
  { key: "schemaSpace", labelKey: "module.field.schemaSpace" },
  { key: "viewVersion", labelKey: "module.field.viewVersion" },
  { key: "workflow", labelKey: "module.field.workflow" },
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

/** ``workflows/<suffix>/…`` → ``suffix`` for scoped deploy / SDK commands. */
function scopeSuffixFromWorkflowTriggerPath(rel: string): string | null {
  const parts = rel.split("/").filter(Boolean);
  if (parts.length < 3 || parts[0] !== "workflows") return null;
  return parts[1] ?? null;
}

function configureTargetsEqual(a: ConfigureTarget, b: ConfigureTarget): boolean {
  if (a.id !== b.id) return false;
  if (a.id === "trigger" && b.id === "trigger") return a.path === b.path;
  return true;
}

type UnsavedPrompt =
  | { kind: "configureTarget"; next: ConfigureTarget }
  | { kind: "tab"; next: Tab }
  | { kind: "artifactPath"; next: string };

export default function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();

  const [tab, setTab] = useState<Tab>("configure");
  const [configureTarget, setConfigureTarget] = useState<ConfigureTarget>({ id: "workflowLocal" });
  const [configSubTab, setConfigSubTab] = useState<ConfigSubTab>("flowCanvas");
  const [flowCanvasEditorOpen, setFlowCanvasEditorOpen] = useState(false);
  const [triggerTopTab, setTriggerTopTab] = useState<TriggerTopTab>("pipeline");
  const [configSearchQuery, setConfigSearchQuery] = useState("");

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

  const [templateDoc, setTemplateDoc] = useState<Record<string, unknown>>({});
  const [templateRawYaml, setTemplateRawYaml] = useState("");
  const [templatePhase, setTemplatePhase] = useState<PhaseKey | null>("status.loading");
  const [templateError, setTemplateError] = useState<string | null>(null);
  const [savedTemplateSnap, setSavedTemplateSnap] = useState("");

  const [scopeCanvasDoc, setScopeCanvasDoc] = useState<WorkflowCanvasDocument>(() =>
    emptyWorkflowCanvasDocument()
  );
  const [savedScopeCanvasSnap, setSavedScopeCanvasSnap] = useState("");
  const [scopeCanvasReloadNonce, setScopeCanvasReloadNonce] = useState(0);
  /** Shown after POST /api/promote-local-workflow-templates (local → template files on disk). */
  const [templatePromoteMessage, setTemplatePromoteMessage] = useState<string | null>(null);
  const [promoteTemplatesBusy, setPromoteTemplatesBusy] = useState(false);

  const [templateCanvasDoc, setTemplateCanvasDoc] = useState<WorkflowCanvasDocument>(() =>
    emptyWorkflowCanvasDocument()
  );
  const [savedTemplateCanvasSnap, setSavedTemplateCanvasSnap] = useState("");
  const [templateCanvasReloadNonce, setTemplateCanvasReloadNonce] = useState(0);
  const [triggerCanvasReloadNonce, setTriggerCanvasReloadNonce] = useState(0);

  const [configTriggerText, setConfigTriggerText] = useState("");
  const [configTriggerPhase, setConfigTriggerPhase] = useState<PhaseKey | null>(null);
  const [savedConfigTriggerSnap, setSavedConfigTriggerSnap] = useState("");

  const [buildLog, setBuildLog] = useState("");
  const [runLog, setRunLog] = useState("");
  const [runAll, setRunAll] = useState(false);
  const [canvasPreviewExecutingIds, setCanvasPreviewExecutingIds] = useState<string[]>([]);
  const [canvasPreviewRunBusy, setCanvasPreviewRunBusy] = useState(false);
  /** task_id → labels for streamed run log / “now executing” lines. */
  const localRunTaskMetaRef = useRef<Map<string, { fn?: string; node?: string }>>(new Map());
  const [cdfToolLog, setCdfToolLog] = useState("");
  const [artifactPaths, setArtifactPaths] = useState<string[]>([]);
  const [artifactPath, setArtifactPath] = useState<string | null>(null);
  const [artifactText, setArtifactText] = useState("");
  const [savedArtifactSnap, setSavedArtifactSnap] = useState("");
  const [artifactPhase, setArtifactPhase] = useState<PhaseKey | null>(null);
  const [unsavedPrompt, setUnsavedPrompt] = useState<UnsavedPrompt | null>(null);
  const [unsavedBusy, setUnsavedBusy] = useState(false);
  /** Display names from root `name` in WorkflowTrigger YAML (from /api/workflow-trigger-meta + live edits). */
  const [triggerNamesByPath, setTriggerNamesByPath] = useState<Record<string, string>>({});
  const [settingsOpen, setSettingsOpen] = useState(false);
  const settingsWrapRef = useRef<HTMLDivElement>(null);
  const triggerTypeDatalistId = useId();

  useEffect(() => {
    if (configureTarget.id !== "trigger") setTriggerTopTab("pipeline");
  }, [configureTarget.id]);

  const api = useCallback(async <T,>(path: string, init?: RequestInit): Promise<T> => {
    const r = await fetch(path, init);
    if (!r.ok) {
      const errText = await r.text();
      throw new Error(errText || r.statusText);
    }
    return r.json() as Promise<T>;
  }, []);

  const refreshArtifactLists = useCallback(async () => {
    try {
      const [arts, trigMeta] = await Promise.all([
        api<{ paths: string[] }>("/api/artifacts"),
        api<{ entries: { path: string; name: string | null }[] }>("/api/workflow-trigger-meta"),
      ]);
      setArtifactPaths(arts.paths ?? []);
      const nm: Record<string, string> = {};
      for (const e of trigMeta.entries ?? []) {
        if (e.name) nm[e.path] = e.name;
      }
      setTriggerNamesByPath(nm);
    } catch {
      /* ignore */
    }
  }, [api]);

  const loadConfigTrigger = useCallback(
    async (rel: string) => {
      setConfigTriggerPhase("status.loading");
      try {
        const d = await api<{ content: string }>(`/api/file?rel=${encodeURIComponent(rel)}`);
        setConfigTriggerText(d.content ?? "");
        setSavedConfigTriggerSnap(d.content ?? "");
        setConfigTriggerPhase("status.loaded");
      } catch {
        setConfigTriggerText("");
        setSavedConfigTriggerSnap("");
        setConfigTriggerPhase(null);
      }
    },
    [api]
  );

  const loadAll = useCallback(async () => {
    setDefaultPhase("status.loading");
    setScopePhase("status.loading");
    setTemplatePhase("status.loading");
    setDefaultError(null);
    setScopeError(null);
    setTemplateError(null);
    try {
      // Local workflow + module defaults first so a failing template or heavy workflows/ scan
      // cannot block ``workflow.local`` / canvas from loading.
      const [dDef, rawDef, dScope, rawScope, dScopeCanvas] = await Promise.all([
        api<Record<string, unknown>>("/api/default-config/model"),
        api<{ content: string }>("/api/default-config"),
        api<Record<string, unknown>>(`/api/scope-document/model?rel=${encodeURIComponent(SCOPE_REL)}`),
        api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(SCOPE_REL)}`),
        api<Record<string, unknown>>(`/api/canvas-document/model?rel=${encodeURIComponent(SCOPE_REL)}`),
      ]);
      setDefaultDoc(dDef && typeof dDef === "object" ? dDef : {});
      setDefaultRawYaml(rawDef.content ?? "");
      setSavedDefaultSnap(JSON.stringify(dDef));
      setScopeDoc(dScope && typeof dScope === "object" ? dScope : {});
      setScopeRawYaml(rawScope.content ?? "");
      setSavedScopeSnap(JSON.stringify(dScope));
      {
        const scopeModel = dScope && typeof dScope === "object" ? dScope : {};
        const c = canvasDocWithScopeSeedIfEmpty(parseWorkflowCanvasDocument(dScopeCanvas), scopeModel);
        setScopeCanvasDoc(c);
        setSavedScopeCanvasSnap(JSON.stringify(c));
        setScopeCanvasReloadNonce((n) => n + 1);
      }
      setDefaultPhase("status.loaded");
      setScopePhase("status.loaded");
    } catch (e) {
      setDefaultError(String(e));
      setScopeError(String(e));
      setDefaultPhase(null);
      setScopePhase(null);
      setTemplatePhase(null);
      return;
    }

    try {
      const [dTpl, rawTpl, dTplCanvas] = await Promise.all([
        api<Record<string, unknown>>(`/api/scope-document/model?rel=${encodeURIComponent(TEMPLATE_REL)}`),
        api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(TEMPLATE_REL)}`),
        api<Record<string, unknown>>(`/api/canvas-document/model?rel=${encodeURIComponent(TEMPLATE_REL)}`),
      ]);
      setTemplateDoc(dTpl && typeof dTpl === "object" ? dTpl : {});
      setTemplateRawYaml(rawTpl.content ?? "");
      setSavedTemplateSnap(JSON.stringify(dTpl));
      {
        const tplModel = dTpl && typeof dTpl === "object" ? dTpl : {};
        const c = canvasDocWithScopeSeedIfEmpty(parseWorkflowCanvasDocument(dTplCanvas), tplModel);
        setTemplateCanvasDoc(c);
        setSavedTemplateCanvasSnap(JSON.stringify(c));
        setTemplateCanvasReloadNonce((n) => n + 1);
      }
      setTemplateError(null);
      setTemplatePhase("status.loaded");
    } catch (e) {
      setTemplateError(String(e));
      setTemplateDoc({});
      setTemplateRawYaml("");
      setSavedTemplateSnap("{}");
      setTemplateCanvasDoc(emptyWorkflowCanvasDocument());
      setSavedTemplateCanvasSnap(JSON.stringify(emptyWorkflowCanvasDocument()));
      setTemplateCanvasReloadNonce((n) => n + 1);
      setTemplatePhase("status.loaded");
    }

    try {
      const [arts, trigMeta] = await Promise.all([
        api<{ paths: string[] }>("/api/artifacts"),
        api<{ entries: { path: string; name: string | null }[] }>("/api/workflow-trigger-meta"),
      ]);
      setArtifactPaths(arts.paths ?? []);
      const nm: Record<string, string> = {};
      for (const e of trigMeta.entries ?? []) {
        if (e.name) nm[e.path] = e.name;
      }
      setTriggerNamesByPath(nm);
    } catch {
      setArtifactPaths([]);
      setTriggerNamesByPath({});
    }
  }, [api]);

  useEffect(() => {
    document.title = t("app.title");
  }, [t]);

  useEffect(() => {
    loadAll().catch(() => undefined);
  }, [loadAll]);

  useEffect(() => {
    if (configureTarget.id === "trigger") {
      void loadConfigTrigger(configureTarget.path);
    }
  }, [configureTarget, loadConfigTrigger]);

  useEffect(() => {
    if (configureTarget.id !== "trigger") return;
    const path = configureTarget.path;
    try {
      const doc = YAML.parse(configTriggerText) as Record<string, unknown>;
      const d = displayNameFromRoot(doc);
      setTriggerNamesByPath((prev) => {
        const next = { ...prev };
        if (d) next[path] = d;
        else delete next[path];
        return next;
      });
    } catch {
      /* keep previous label while editing broken YAML */
    }
  }, [configTriggerText, configureTarget]);

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

  useEffect(() => {
    if (!unsavedPrompt) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !unsavedBusy) setUnsavedPrompt(null);
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [unsavedPrompt, unsavedBusy]);

  const hierarchy = useMemo((): AliasingScopeHierarchy => {
    const h = defaultDoc.aliasing_scope_hierarchy;
    if (h && typeof h === "object" && !Array.isArray(h)) return h as AliasingScopeHierarchy;
    return { levels: [], locations: [] };
  }, [defaultDoc.aliasing_scope_hierarchy]);

  const moduleSchemaSpace = useMemo((): string | undefined => {
    const v = defaultDoc.schemaSpace;
    const s = v != null ? String(v).trim() : "";
    return s || undefined;
  }, [defaultDoc.schemaSpace]);

  const setHierarchy = (next: AliasingScopeHierarchy) => {
    setDefaultDoc((d) => ({ ...d, aliasing_scope_hierarchy: next }));
  };

  const isDefaultDirty = useMemo(() => {
    if (!savedDefaultSnap) return false;
    return JSON.stringify(defaultDoc) !== savedDefaultSnap;
  }, [defaultDoc, savedDefaultSnap]);

  const isScopeDocDirty = useMemo(() => {
    if (!savedScopeSnap) return false;
    return JSON.stringify(scopeDoc) !== savedScopeSnap;
  }, [scopeDoc, savedScopeSnap]);

  const isScopeCanvasDirty = useMemo(() => {
    if (!savedScopeCanvasSnap) return false;
    return JSON.stringify(scopeCanvasDoc) !== savedScopeCanvasSnap;
  }, [scopeCanvasDoc, savedScopeCanvasSnap]);

  const isScopeDirty = useMemo(
    () => isScopeDocDirty || isScopeCanvasDirty,
    [isScopeDocDirty, isScopeCanvasDirty]
  );

  useEffect(() => {
    if (configureTarget.id === "workflowLocal" && isScopeDirty) {
      setTemplatePromoteMessage(null);
    }
  }, [configureTarget.id, isScopeDirty]);

  const isTemplateDocDirty = useMemo(() => {
    if (!savedTemplateSnap) return false;
    return JSON.stringify(templateDoc) !== savedTemplateSnap;
  }, [templateDoc, savedTemplateSnap]);

  const isTemplateCanvasDirty = useMemo(() => {
    if (!savedTemplateCanvasSnap) return false;
    return JSON.stringify(templateCanvasDoc) !== savedTemplateCanvasSnap;
  }, [templateCanvasDoc, savedTemplateCanvasSnap]);

  const isTemplateDirty = useMemo(
    () => isTemplateDocDirty || isTemplateCanvasDirty,
    [isTemplateDocDirty, isTemplateCanvasDirty]
  );

  const flowFullscreenOpen = useMemo(
    () =>
      tab === "configure" &&
      flowCanvasEditorOpen &&
      (configureTarget.id === "workflowLocal" ||
        configureTarget.id === "workflowTemplate" ||
        (configureTarget.id === "trigger" &&
          triggerTopTab === "pipeline" &&
          configSubTab === "flowCanvas")),
    [tab, configureTarget, flowCanvasEditorOpen, triggerTopTab, configSubTab]
  );

  useEffect(() => {
    if (tab !== "configure") setFlowCanvasEditorOpen(false);
  }, [tab]);

  useEffect(() => {
    if (configSubTab !== "flowCanvas") setFlowCanvasEditorOpen(false);
  }, [configSubTab]);

  useEffect(() => {
    setFlowCanvasEditorOpen(false);
  }, [configureTarget]);

  useEffect(() => {
    if (!flowFullscreenOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        setFlowCanvasEditorOpen(false);
      }
    };
    document.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [flowFullscreenOpen]);

  const isConfigTriggerDirty = useMemo(() => {
    return configTriggerText !== savedConfigTriggerSnap;
  }, [configTriggerText, savedConfigTriggerSnap]);

  const isConfigureDirty = useMemo(() => {
    switch (configureTarget.id) {
      case "workflowLocal":
        return isScopeDirty;
      case "workflowTemplate":
        return isTemplateDirty;
      case "trigger":
        return isConfigTriggerDirty;
      default:
        return false;
    }
  }, [configureTarget.id, isScopeDirty, isTemplateDirty, isConfigTriggerDirty]);

  const isArtifactDirty = useMemo(() => {
    if (!artifactPath) return false;
    return artifactText !== savedArtifactSnap;
  }, [artifactPath, artifactText, savedArtifactSnap]);

  const isCurrentEditorDirty = useMemo(() => {
    switch (tab) {
      case "scope":
        return isDefaultDirty;
      case "configure":
        return isConfigureDirty;
      case "artifacts":
        return isArtifactDirty;
      case "build":
        return false;
      default:
        return false;
    }
  }, [tab, isDefaultDirty, isConfigureDirty, isArtifactDirty]);

  const saveDefault = async (): Promise<boolean> => {
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
      return true;
    } catch (e) {
      setDefaultError(String(e));
      setDefaultPhase(null);
      return false;
    }
  };

  const promoteLocalWorkflowTemplates = async (): Promise<void> => {
    const confirmMsg = isScopeDirty
      ? t("config.promoteTemplateDirtyConfirm")
      : t("config.promoteTemplateConfirm");
    if (!window.confirm(confirmMsg)) return;
    setPromoteTemplatesBusy(true);
    setTemplatePromoteMessage(null);
    setScopeError(null);
    try {
      await api("/api/promote-local-workflow-templates", { method: "POST" });
      setTemplatePromoteMessage(t("config.templatePromoted"));
      window.setTimeout(() => setTemplatePromoteMessage(null), 8000);
      if (configureTarget.id === "workflowTemplate") {
        await reloadCurrentConfigure();
      }
    } catch (e) {
      setTemplatePromoteMessage(null);
      setScopeError(String(e));
    } finally {
      setPromoteTemplatesBusy(false);
    }
  };

  const saveScope = async (): Promise<boolean> => {
    setScopePhase("status.saving");
    setScopeError(null);
    try {
      await api(`/api/scope-document/model?rel=${encodeURIComponent(SCOPE_REL)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(scopeDoc),
      });
      await api(`/api/canvas-document/model?rel=${encodeURIComponent(SCOPE_REL)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(scopeCanvasDoc),
      });
      const raw = await api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(SCOPE_REL)}`);
      setScopeRawYaml(raw.content ?? "");
      setSavedScopeSnap(JSON.stringify(scopeDoc));
      setSavedScopeCanvasSnap(JSON.stringify(scopeCanvasDoc));
      setScopePhase("status.saved");
      return true;
    } catch (e) {
      setScopeError(String(e));
      setScopePhase(null);
      return false;
    }
  };

  const saveTemplate = async (): Promise<boolean> => {
    setTemplatePhase("status.saving");
    setTemplateError(null);
    try {
      await api(`/api/scope-document/model?rel=${encodeURIComponent(TEMPLATE_REL)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(templateDoc),
      });
      await api(`/api/canvas-document/model?rel=${encodeURIComponent(TEMPLATE_REL)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(templateCanvasDoc),
      });
      const raw = await api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(TEMPLATE_REL)}`);
      setTemplateRawYaml(raw.content ?? "");
      setSavedTemplateSnap(JSON.stringify(templateDoc));
      setSavedTemplateCanvasSnap(JSON.stringify(templateCanvasDoc));
      setTemplatePhase("status.saved");
      return true;
    } catch (e) {
      setTemplateError(String(e));
      setTemplatePhase(null);
      return false;
    }
  };

  const saveConfigureTrigger = async (): Promise<boolean> => {
    if (configureTarget.id !== "trigger") return false;
    const rel = configureTarget.path;
    setConfigTriggerPhase("status.saving");
    try {
      let content = configTriggerText;
      try {
        const doc = YAML.parse(content) as Record<string, unknown> | null;
        if (doc && typeof doc === "object" && !Array.isArray(doc) && "authentication" in doc) {
          const { authentication: _removed, ...rest } = doc;
          content = YAML.stringify(rest, { lineWidth: 0 });
          setConfigTriggerText(content);
        }
      } catch {
        /* invalid YAML: save raw; server may reject */
      }
      await api(`/api/file?rel=${encodeURIComponent(rel)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content }),
      });
      setSavedConfigTriggerSnap(configTriggerText);
      setConfigTriggerPhase("status.saved");
      const art = await api<{ paths: string[] }>("/api/artifacts");
      setArtifactPaths(art.paths ?? []);
      return true;
    } catch {
      setConfigTriggerPhase(null);
      return false;
    }
  };

  const reloadCurrentConfigure = async () => {
    switch (configureTarget.id) {
      case "workflowLocal": {
        setScopePhase("status.loading");
        setScopeError(null);
        try {
          const [model, raw, cModel] = await Promise.all([
            api<Record<string, unknown>>(`/api/scope-document/model?rel=${encodeURIComponent(SCOPE_REL)}`),
            api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(SCOPE_REL)}`),
            api<Record<string, unknown>>(`/api/canvas-document/model?rel=${encodeURIComponent(SCOPE_REL)}`),
          ]);
          const scopeModel = model && typeof model === "object" ? model : {};
          setScopeDoc(scopeModel);
          setScopeRawYaml(raw.content ?? "");
          setSavedScopeSnap(JSON.stringify(model));
          {
            const c = canvasDocWithScopeSeedIfEmpty(parseWorkflowCanvasDocument(cModel), scopeModel);
            setScopeCanvasDoc(c);
            setSavedScopeCanvasSnap(JSON.stringify(c));
            setScopeCanvasReloadNonce((n) => n + 1);
          }
          setScopePhase("status.loaded");
        } catch (e) {
          setScopeError(String(e));
          setScopePhase(null);
        }
        break;
      }
      case "workflowTemplate": {
        setTemplatePhase("status.loading");
        setTemplateError(null);
        try {
          const [model, raw, cModel] = await Promise.all([
            api<Record<string, unknown>>(`/api/scope-document/model?rel=${encodeURIComponent(TEMPLATE_REL)}`),
            api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(TEMPLATE_REL)}`),
            api<Record<string, unknown>>(`/api/canvas-document/model?rel=${encodeURIComponent(TEMPLATE_REL)}`),
          ]);
          const tplModel = model && typeof model === "object" ? model : {};
          setTemplateDoc(tplModel);
          setTemplateRawYaml(raw.content ?? "");
          setSavedTemplateSnap(JSON.stringify(model));
          {
            const c = canvasDocWithScopeSeedIfEmpty(parseWorkflowCanvasDocument(cModel), tplModel);
            setTemplateCanvasDoc(c);
            setSavedTemplateCanvasSnap(JSON.stringify(c));
            setTemplateCanvasReloadNonce((n) => n + 1);
          }
          setTemplatePhase("status.loaded");
        } catch (e) {
          setTemplateError(String(e));
          setTemplatePhase(null);
        }
        break;
      }
      case "trigger":
        await loadConfigTrigger(configureTarget.path);
        break;
      default:
        break;
    }
  };

  const reloadDefaultFromDisk = async () => {
    setDefaultPhase("status.loading");
    setDefaultError(null);
    try {
      const [model, raw] = await Promise.all([
        api<Record<string, unknown>>("/api/default-config/model"),
        api<{ content: string }>("/api/default-config"),
      ]);
      setDefaultDoc(model && typeof model === "object" ? model : {});
      setDefaultRawYaml(raw.content ?? "");
      setSavedDefaultSnap(JSON.stringify(model));
      setDefaultPhase("status.loaded");
    } catch (e) {
      setDefaultError(String(e));
      setDefaultPhase(null);
    }
  };

  const runBuild = async (force: boolean, dryRun: boolean) => {
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

  const runLocalPipelineStreamed = useCallback(async () => {
    setCanvasPreviewRunBusy(true);
    setCanvasPreviewExecutingIds([]);
    localRunTaskMetaRef.current = new Map();
    setRunLog(`${t("status.running")}\n`);
    const body: {
      run_all: boolean;
      target: "workflow_local" | "workflow_template" | "workflow_trigger";
      workflow_trigger_rel?: string;
    } = { run_all: runAll, target: "workflow_local" };
    if (configureTarget.id === "workflowTemplate") {
      body.target = "workflow_template";
    } else if (configureTarget.id === "trigger") {
      body.target = "workflow_trigger";
      body.workflow_trigger_rel = configureTarget.path;
    }
    try {
      const res = await fetch("/api/run-stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (res.status === 501) {
        setRunLog(
          `${t("status.running")}\n${t("flow.previewRunProgressUnsupported")}\n`
        );
        try {
          const d = await api<{ exit_code: number; stdout: string; stderr: string }>("/api/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          });
          setRunLog(
            `exit_code: ${d.exit_code}\n\n--- stdout ---\n${d.stdout}\n--- stderr ---\n${d.stderr}`
          );
        } catch (e) {
          setRunLog(String(e));
        }
        return;
      }
      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText || res.statusText);
      }
      const reader = res.body?.getReader();
      if (!reader) throw new Error("No response body");
      const dec = new TextDecoder();
      let buf = "";
      const active = new Set<string>();
      const flushExecuting = () => setCanvasPreviewExecutingIds([...active]);
      const appendExecutingLine = () => {
        if (active.size === 0) {
          setRunLog(
            (prev) => `${prev}${t("run.localExecutingNow", { list: t("run.localExecutingNone") })}\n`
          );
          return;
        }
        const parts: string[] = [];
        for (const id of [...active].sort()) {
          const m = localRunTaskMetaRef.current.get(id);
          const fn = (m?.fn ?? "").trim();
          const node = (m?.node ?? "").trim();
          const label = fn || id;
          parts.push(node ? `${label} (${node})` : label);
        }
        setRunLog((prev) => `${prev}${t("run.localExecutingNow", { list: parts.join("; ") })}\n`);
      };
      type ProgressEv = {
        event?: string;
        task_id?: string;
        code?: number;
        level?: string;
        message?: string;
        function_external_id?: string;
        canvas_node_id?: string;
        pipeline_node_id?: string;
      };
      const nodeSuffixFor = (ev: ProgressEv, taskId: string): string => {
        const canvas = (ev.canvas_node_id ?? "").trim();
        if (canvas) return ` — ${canvas}`;
        const p = (ev.pipeline_node_id ?? "").trim();
        if (p && p !== taskId) return ` — ${p}`;
        return "";
      };
      const handleLine = (line: string) => {
        if (!line.trim()) return;
        let ev: ProgressEv;
        try {
          ev = JSON.parse(line) as ProgressEv;
        } catch {
          return;
        }
        if (ev.event === "log" && typeof ev.message === "string") {
          const prefix = ev.level ? `[${ev.level}] ` : "";
          setRunLog((prev) => `${prev}${prefix}${ev.message}\n`);
          return;
        }
        if (ev.event === "task_start" && ev.task_id) {
          const taskId = ev.task_id;
          const fn = (ev.function_external_id ?? "").trim();
          const canvas = (ev.canvas_node_id ?? "").trim();
          const pnode = (ev.pipeline_node_id ?? "").trim();
          const node = canvas || (pnode && pnode !== taskId ? pnode : "");
          localRunTaskMetaRef.current.set(taskId, {
            fn: fn || undefined,
            node: node || undefined,
          });
          active.add(taskId);
          setRunLog(
            (prev) =>
              `${prev}${t("run.localTaskStart", {
                functionId: fn || taskId,
                taskId,
                nodeSuffix: nodeSuffixFor(ev, taskId),
              })}\n`
          );
          appendExecutingLine();
          flushExecuting();
          return;
        }
        if (ev.event === "task_end" && ev.task_id) {
          const taskId = ev.task_id;
          const fn = (ev.function_external_id ?? "").trim();
          active.delete(taskId);
          localRunTaskMetaRef.current.delete(taskId);
          setRunLog(
            (prev) =>
              `${prev}${t("run.localTaskEnd", {
                functionId: fn || taskId,
                taskId,
                nodeSuffix: nodeSuffixFor(ev, taskId),
              })}\n`
          );
          appendExecutingLine();
          flushExecuting();
          return;
        }
        if (ev.event === "exit") {
          setRunLog((prev) => `${prev}\n${t("run.localRunExitLine", { code: ev.code ?? -1 })}\n`);
        }
        flushExecuting();
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
    } catch (e) {
      setRunLog((prev) => `${prev}\n${String(e)}\n`);
    } finally {
      setCanvasPreviewRunBusy(false);
      setCanvasPreviewExecutingIds([]);
      try {
        const art = await api<{ paths: string[] }>("/api/artifacts");
        setArtifactPaths(art.paths ?? []);
      } catch {
        /* ignore */
      }
    }
  }, [api, configureTarget, runAll, t]);

  const flowPreviewLocalRun: WorkflowPreviewLocalRun = useMemo(
    () => ({
      runAll,
      onRunAllChange: setRunAll,
      busy: canvasPreviewRunBusy,
      executingTaskIds: canvasPreviewExecutingIds,
      onRun: () => void runLocalPipelineStreamed(),
    }),
    [runAll, canvasPreviewRunBusy, canvasPreviewExecutingIds, runLocalPipelineStreamed]
  );

  type CdfCliResult = { exit_code: number; stdout: string; stderr: string };

  const runDeployScope = async (dryRun: boolean) => {
    if (configureTarget.id !== "trigger" || !selectedScopeSuffix) return;
    setCdfToolLog(`${t("status.running")}\n`);
    const body = {
      scope_suffix: selectedScopeSuffix,
      workflow_trigger_rel: configureTarget.path,
      dry_run: dryRun,
      skip_build: false,
      /** Generated triggers often still contain Toolkit ``{{…}}`` tokens until edited. */
      allow_unresolved_placeholders: true,
    };
    try {
      const d = await api<CdfCliResult>("/api/deploy-scope", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      setCdfToolLog(
        `exit_code: ${d.exit_code}\n\n--- stdout ---\n${d.stdout}\n--- stderr ---\n${d.stderr}`
      );
    } catch (e) {
      setCdfToolLog(String(e));
    }
  };

  const runCdfWorkflowRemote = async (dryRun: boolean) => {
    if (configureTarget.id !== "trigger" || !selectedScopeSuffix) return;
    setCdfToolLog(`${t("status.running")}\n`);
    const body = {
      scope_suffix: selectedScopeSuffix,
      workflow_trigger_rel: configureTarget.path,
      dry_run: dryRun,
    };
    try {
      const d = await api<CdfCliResult>("/api/cdf-workflow-run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      setCdfToolLog(
        `exit_code: ${d.exit_code}\n\n--- stdout ---\n${d.stdout}\n--- stderr ---\n${d.stderr}`
      );
    } catch (e) {
      setCdfToolLog(String(e));
    }
  };

  const openArtifact = async (rel: string) => {
    setArtifactPath(rel);
    setArtifactPhase("status.loading");
    const d = await api<{ content: string }>(`/api/file?rel=${encodeURIComponent(rel)}`);
    const c = d.content ?? "";
    setArtifactText(c);
    setSavedArtifactSnap(c);
    setArtifactPhase("status.loaded");
  };

  const saveArtifactFile = async (): Promise<boolean> => {
    if (!artifactPath) return false;
    setArtifactPhase("status.saving");
    try {
      await api(`/api/file?rel=${encodeURIComponent(artifactPath)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content: artifactText }),
      });
      setSavedArtifactSnap(artifactText);
      setArtifactPhase("status.saved");
      const art = await api<{ paths: string[] }>("/api/artifacts");
      setArtifactPaths(art.paths ?? []);
      return true;
    } catch {
      setArtifactPhase(null);
      return false;
    }
  };

  const parsedConfigTrigger = useMemo(() => {
    if (configureTarget.id !== "trigger") return null;
    try {
      return YAML.parse(configTriggerText) as JsonObject;
    } catch {
      return null;
    }
  }, [configTriggerText, configureTarget.id]);

  const triggerInput = parsedConfigTrigger?.input;
  const triggerConfiguration: JsonObject | null = (() => {
    if (!triggerInput || typeof triggerInput !== "object" || Array.isArray(triggerInput)) return null;
    const ti = triggerInput as JsonObject;
    const c = ti.configuration;
    if (c !== null && typeof c === "object" && !Array.isArray(c)) return c as JsonObject;
    return {};
  })();

  const triggerRuleForForm: JsonObject = useMemo(() => {
    const tr = parsedConfigTrigger?.triggerRule;
    if (tr !== null && typeof tr === "object" && !Array.isArray(tr)) return tr as JsonObject;
    return {};
  }, [parsedConfigTrigger]);

  const pipelineConfiguration: JsonObject = triggerConfiguration ?? {};

  const triggerCanvasDoc = useMemo((): WorkflowCanvasDocument => {
    if (configureTarget.id !== "trigger") return emptyWorkflowCanvasDocument();
    const pc = pipelineConfiguration as Record<string, unknown>;
    const raw = pc[TRIGGER_WORKFLOW_CANVAS_KEY] ?? pc["canvas"];
    const parsed = parseWorkflowCanvasDocument(raw);
    return canvasDocWithScopeSeedIfEmpty(parsed, pipelineConfiguration as Record<string, unknown>);
  }, [configureTarget.id, pipelineConfiguration, configTriggerText]);

  useEffect(() => {
    if (configureTarget.id !== "trigger") return;
    setTriggerCanvasReloadNonce((n) => n + 1);
  }, [configureTarget, savedConfigTriggerSnap]);

  const updateTriggerConfiguration = (slice: Partial<JsonObject>) => {
    if (!parsedConfigTrigger) return;
    const ti =
      triggerInput && typeof triggerInput === "object" && !Array.isArray(triggerInput)
        ? (triggerInput as JsonObject)
        : {};
    const base = ti.configuration;
    const prev =
      base !== null && typeof base === "object" && !Array.isArray(base) ? (base as JsonObject) : {};
    const nextConf = { ...prev, ...slice };
    const nextInput = { ...ti, configuration: nextConf };
    const nextDoc = { ...parsedConfigTrigger, input: nextInput };
    setConfigTriggerText(YAML.stringify(nextDoc, { lineWidth: 0 }));
  };

  const setTriggerSourceViews = (v: unknown) => updateTriggerConfiguration({ source_views: v });
  const setTriggerKeyExtraction = (v: unknown) => updateTriggerConfiguration({ key_extraction: v });
  const setTriggerAliasing = (v: unknown) => updateTriggerConfiguration({ aliasing: v });

  const mergeParsedTriggerDoc = useCallback((next: JsonObject) => {
    setConfigTriggerText(YAML.stringify(next, { lineWidth: 0 }));
  }, []);

  const patchTriggerRootFields = useCallback(
    (patch: Partial<JsonObject>) => {
      if (!parsedConfigTrigger) return;
      mergeParsedTriggerDoc({ ...parsedConfigTrigger, ...patch });
    },
    [parsedConfigTrigger, mergeParsedTriggerDoc]
  );

  const patchTriggerRule = useCallback(
    (patch: Partial<JsonObject>) => {
      if (!parsedConfigTrigger) return;
      const tr = parsedConfigTrigger.triggerRule;
      const base =
        tr !== null && typeof tr === "object" && !Array.isArray(tr) ? ({ ...(tr as JsonObject) }) : {};
      mergeParsedTriggerDoc({ ...parsedConfigTrigger, triggerRule: { ...base, ...patch } });
    },
    [parsedConfigTrigger, mergeParsedTriggerDoc]
  );

  const setTriggerRunAll = (v: boolean) => {
    if (!parsedConfigTrigger) return;
    const ti =
      triggerInput && typeof triggerInput === "object" && !Array.isArray(triggerInput)
        ? (triggerInput as JsonObject)
        : {};
    const nextInput = { ...ti, run_all: v };
    const nextDoc = { ...parsedConfigTrigger, input: nextInput };
    setConfigTriggerText(YAML.stringify(nextDoc, { lineWidth: 0 }));
  };

  const setTriggerRunId = (v: string) => {
    if (!parsedConfigTrigger) return;
    const ti =
      triggerInput && typeof triggerInput === "object" && !Array.isArray(triggerInput)
        ? (triggerInput as JsonObject)
        : {};
    const nextInput = { ...ti, run_id: v };
    const nextDoc = { ...parsedConfigTrigger, input: nextInput };
    setConfigTriggerText(YAML.stringify(nextDoc, { lineWidth: 0 }));
  };

  const setTriggerRootName = (v: string) => {
    try {
      const doc = YAML.parse(configTriggerText) as Record<string, unknown>;
      const next: Record<string, unknown> = { ...doc };
      if (v.trim().length === 0) delete next.name;
      else next.name = v;
      setConfigTriggerText(YAML.stringify(next, { lineWidth: 0 }));
    } catch {
      /* invalid YAML */
    }
  };

  const triggerPaths = useMemo(
    () => artifactPaths.filter((p) => isTriggerPath(p)),
    [artifactPaths]
  );

  const workflowLocalNavPrimary = useMemo(
    () => displayNameFromRoot(scopeDoc) ?? t("config.workflowLocalNav"),
    [scopeDoc, t]
  );
  const workflowTemplateNavPrimary = useMemo(
    () => displayNameFromRoot(templateDoc) ?? t("config.workflowTemplateNav"),
    [templateDoc, t]
  );

  const showWorkflowLocalInPanel = useMemo(
    () =>
      matchConfigSearch(
        configSearchQuery,
        workflowLocalNavPrimary,
        t("config.fileHint.workflowLocal"),
        scopeDoc.name != null ? String(scopeDoc.name) : ""
      ),
    [configSearchQuery, workflowLocalNavPrimary, t, scopeDoc.name]
  );

  const showWorkflowTemplateInPanel = useMemo(
    () =>
      matchConfigSearch(
        configSearchQuery,
        workflowTemplateNavPrimary,
        t("config.fileHint.workflowTemplate"),
        templateDoc.name != null ? String(templateDoc.name) : ""
      ),
    [configSearchQuery, workflowTemplateNavPrimary, t, templateDoc.name]
  );

  const filteredTriggerPaths = useMemo(
    () =>
      triggerPaths.filter((p) => {
        const shortPath = p.replace(/^workflows\//, "");
        const custom = triggerNamesByPath[p];
        const primary = custom ?? shortPath;
        return matchConfigSearch(configSearchQuery, primary, shortPath, p, custom ?? "");
      }),
    [triggerPaths, triggerNamesByPath, configSearchQuery]
  );

  const showFullPanelEmpty =
    configSearchQuery.trim().length > 0 &&
    !showWorkflowLocalInPanel &&
    !showWorkflowTemplateInPanel &&
    filteredTriggerPaths.length === 0;

  const discardCurrentEditor = async () => {
    switch (tab) {
      case "scope":
        await reloadDefaultFromDisk();
        break;
      case "configure":
        await reloadCurrentConfigure();
        break;
      case "artifacts":
        if (artifactPath) {
          const d = await api<{ content: string }>(
            `/api/file?rel=${encodeURIComponent(artifactPath)}`
          );
          const c = d.content ?? "";
          setArtifactText(c);
          setSavedArtifactSnap(c);
        }
        break;
      default:
        break;
    }
  };

  const applyPendingNavigation = (p: UnsavedPrompt) => {
    if (p.kind === "configureTarget") {
      setConfigureTarget(p.next);
    } else if (p.kind === "tab") {
      setTab(p.next);
      setSettingsOpen(false);
    } else {
      void openArtifact(p.next);
    }
    setUnsavedPrompt(null);
  };

  const commitUnsavedSave = async () => {
    const p = unsavedPrompt;
    if (!p) return;
    if (p.kind === "artifactPath") {
      setUnsavedBusy(true);
      try {
        const ok = await saveArtifactFile();
        if (ok) {
          await openArtifact(p.next);
          setUnsavedPrompt(null);
        }
      } finally {
        setUnsavedBusy(false);
      }
      return;
    }
    setUnsavedBusy(true);
    try {
      let ok = false;
      if (tab === "scope") ok = await saveDefault();
      else if (tab === "configure") ok = await saveConfigure();
      else if (tab === "artifacts") ok = await saveArtifactFile();
      else ok = true;
      if (ok) applyPendingNavigation(p);
    } finally {
      setUnsavedBusy(false);
    }
  };

  const commitUnsavedDiscard = async () => {
    const p = unsavedPrompt;
    if (!p) return;
    if (p.kind === "artifactPath") {
      setUnsavedBusy(true);
      try {
        await discardCurrentEditor();
        await openArtifact(p.next);
        setUnsavedPrompt(null);
      } finally {
        setUnsavedBusy(false);
      }
      return;
    }
    setUnsavedBusy(true);
    try {
      await discardCurrentEditor();
      applyPendingNavigation(p);
    } finally {
      setUnsavedBusy(false);
    }
  };

  const requestArtifactOpen = (rel: string) => {
    if (rel === artifactPath) return;
    if (isArtifactDirty) {
      setUnsavedPrompt({ kind: "artifactPath", next: rel });
      return;
    }
    void openArtifact(rel);
  };

  const requestTabChange = (next: Tab) => {
    if (next === tab) return;
    if (isCurrentEditorDirty) {
      setUnsavedPrompt({ kind: "tab", next });
      return;
    }
    setTab(next);
    setSettingsOpen(false);
  };

  const selectConfigureTarget = (next: ConfigureTarget) => {
    if (configureTargetsEqual(configureTarget, next)) return;
    if (isConfigureDirty) {
      setUnsavedPrompt({ kind: "configureTarget", next });
      return;
    }
    setConfigureTarget(next);
  };

  const workflowDoc =
    configureTarget.id === "workflowTemplate" ? templateDoc : scopeDoc;
  const setWorkflowDoc =
    configureTarget.id === "workflowTemplate" ? setTemplateDoc : setScopeDoc;
  const workflowRawYaml =
    configureTarget.id === "workflowTemplate" ? templateRawYaml : scopeRawYaml;
  const workflowPhase =
    configureTarget.id === "workflowTemplate" ? templatePhase : scopePhase;
  const workflowError =
    configureTarget.id === "workflowTemplate" ? templateError : scopeError;
  const workflowStatus =
    workflowError ?? (workflowPhase ? t(workflowPhase) : "");

  const defaultStatus = defaultError ?? (defaultPhase ? t(defaultPhase) : "");
  const configTriggerStatus = configTriggerPhase ? t(configTriggerPhase) : "";

  const configureStatus = (() => {
    switch (configureTarget.id) {
      case "workflowLocal":
        return templatePromoteMessage ?? workflowStatus;
      case "workflowTemplate":
        return workflowStatus;
      case "trigger":
        return configTriggerStatus;
      default:
        return "";
    }
  })();

  const configurePhaseOk =
    configureTarget.id === "workflowLocal" || configureTarget.id === "workflowTemplate"
      ? !workflowError &&
          (Boolean(templatePromoteMessage) ||
            workflowPhase === "status.saved" ||
            workflowPhase === "status.loaded")
      : configureTarget.id === "trigger"
        ? configTriggerPhase === "status.saved" || configTriggerPhase === "status.loaded"
        : false;

  const saveConfigure = async (): Promise<boolean> => {
    switch (configureTarget.id) {
      case "workflowLocal":
        return saveScope();
      case "workflowTemplate":
        return saveTemplate();
      case "trigger":
        return saveConfigureTrigger();
      default:
        return false;
    }
  };

  const calloutForConfigure = () => {
    switch (configureTarget.id) {
      case "workflowLocal":
        return t("callout.scopeDoc");
      case "workflowTemplate":
        return t("callout.workflowTemplate");
      case "trigger":
        return t("callout.triggerEditor", { path: configureTarget.path });
      default:
        return "";
    }
  };

  const saveButtonLabel = (): MessageKey => {
    switch (configureTarget.id) {
      case "workflowLocal":
        return "btn.saveScope";
      case "workflowTemplate":
        return "btn.saveTemplateFile";
      case "trigger":
        return "btn.saveFile";
      default:
        return "btn.saveFile";
    }
  };

  const runContextLine = useMemo(() => {
    if (configureTarget.id === "workflowLocal") return t("run.contextWorkflowLocal");
    if (configureTarget.id === "workflowTemplate") return t("run.contextWorkflowTemplate");
    const short = configureTarget.path.replace(/^workflows\//, "");
    return t("run.contextWorkflowTrigger", { path: short });
  }, [configureTarget, t]);

  const selectedScopeSuffix = useMemo(() => {
    if (configureTarget.id !== "trigger") return null;
    return scopeSuffixFromWorkflowTriggerPath(configureTarget.path);
  }, [configureTarget]);

  /** Local pipeline run in progress — lock edits and unify preview + Run Workflow tab state. */
  const configureEditsLocked = canvasPreviewRunBusy;

  const configureRunPipelineSubpanel = (
    <div className="kea-config-run">
      <p className="kea-hint" style={{ marginBottom: "0.5rem", maxWidth: "72ch" }}>
        {runContextLine}
      </p>
      {configureTarget.id === "trigger" && isConfigTriggerDirty && (
        <p className="kea-hint kea-hint--warn" style={{ marginBottom: "0.5rem", maxWidth: "72ch" }}>
          {t("run.triggerUnsaved")}
        </p>
      )}
      <div
        className="kea-toolbar"
        style={{
          flexWrap: "wrap",
          gap: "0.75rem",
          alignItems: "center",
          marginBottom: "0.5rem",
        }}
      >
        <button
          type="button"
          className="kea-btn kea-btn--primary"
          disabled={configureEditsLocked}
          onClick={() => void runLocalPipelineStreamed()}
        >
          {configureEditsLocked ? t("status.running") : t("btn.runPipeline")}
        </button>
        <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
          <input
            type="checkbox"
            checked={runAll}
            disabled={configureEditsLocked}
            onChange={(e) => setRunAll(e.target.checked)}
          />
          {t("run.runAll")}
        </label>
      </div>
      <p className="kea-hint" style={{ marginBottom: "0.5rem", maxWidth: "72ch" }}>
        {t("run.runAllHint")}
      </p>
      <textarea
        readOnly
        className="kea-textarea kea-textarea--readonly"
        value={runLog}
        placeholder={t("run.outputPlaceholder")}
        style={{ minHeight: 160, width: "100%" }}
      />

      <hr className="kea-run-divider" style={{ margin: "1.25rem 0", border: 0, borderTop: "1px solid var(--kea-border)" }} />
      {configureTarget.id === "trigger" ? (
        <>
          <p className="kea-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
            {t("run.cdfToolsHint")}
          </p>
          {!selectedScopeSuffix && (
            <p className="kea-hint kea-hint--warn" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
              {t("run.needTriggerScope")}
            </p>
          )}
          <div
            className="kea-toolbar"
            style={{
              flexWrap: "wrap",
              gap: "0.75rem",
              alignItems: "center",
              marginBottom: "0.5rem",
            }}
          >
            <button
              type="button"
              className="kea-btn"
              disabled={configureEditsLocked || !selectedScopeSuffix}
              title={!selectedScopeSuffix ? t("run.needTriggerScope") : undefined}
              onClick={() => void runDeployScope(false)}
            >
              {t("btn.deployScope")}
            </button>
            <button
              type="button"
              className="kea-btn"
              disabled={configureEditsLocked || !selectedScopeSuffix}
              title={!selectedScopeSuffix ? t("run.needTriggerScope") : undefined}
              onClick={() => void runCdfWorkflowRemote(false)}
            >
              {t("btn.cdfWorkflowRun")}
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--ghost"
              disabled={configureEditsLocked || !selectedScopeSuffix}
              title={!selectedScopeSuffix ? t("run.needTriggerScope") : undefined}
              onClick={() => void runCdfWorkflowRemote(true)}
            >
              {t("btn.cdfWorkflowRunDryRun")}
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--ghost"
              disabled={configureEditsLocked || !selectedScopeSuffix}
              title={!selectedScopeSuffix ? t("run.needTriggerScope") : undefined}
              onClick={() => void runDeployScope(true)}
            >
              {t("btn.deployScopeDryRun")}
            </button>
          </div>
          <textarea
            readOnly
            className="kea-textarea kea-textarea--readonly"
            value={cdfToolLog}
            placeholder={t("run.cdfDeployOutputPlaceholder")}
            style={{ minHeight: 140, width: "100%" }}
          />
        </>
      ) : (
        <p className="kea-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
          {t("run.cdfScopedOnly")}
        </p>
      )}
    </div>
  );

  const tabs: { id: Tab; labelKey: MessageKey }[] = [
    { id: "configure", labelKey: "tabs.configure" },
    { id: "scope", labelKey: "tabs.scope" },
    { id: "build", labelKey: "tabs.build" },
    { id: "artifacts", labelKey: "tabs.artifacts" },
  ];

  const configSubtabs: { id: ConfigSubTab; labelKey: MessageKey }[] = [
    { id: "flowCanvas", labelKey: "tabs.flowCanvas" },
    { id: "sourceViews", labelKey: "tabs.sourceViews" },
    { id: "matchDefinitions", labelKey: "tabs.matchDefinitions" },
    { id: "keyExtraction", labelKey: "tabs.keyExtraction" },
    { id: "aliasing", labelKey: "tabs.aliasing" },
    { id: "runPipeline", labelKey: "tabs.runPipeline" },
  ];

  return (
    <div className={`kea-app${tab === "configure" ? " kea-app--wide" : ""}`}>
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
                requestTabChange(id);
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
          <div className="kea-config-display-name">
            <label className="kea-label">
              {t("config.displayName")}
              <input
                className="kea-input"
                placeholder={t("config.displayNamePlaceholder")}
                value={defaultDoc.name != null ? String(defaultDoc.name) : ""}
                onChange={(e) => {
                  const v = e.target.value;
                  setDefaultDoc((d) => {
                    const next = { ...d };
                    if (!v.trim()) delete next.name;
                    else next.name = v;
                    return next;
                  });
                }}
              />
            </label>
          </div>
          <div className="kea-toolbar">
            <button type="button" className="kea-btn kea-btn--ghost" onClick={() => loadAll()}>
              {t("btn.reload")}
            </button>
            <button type="button" className="kea-btn kea-btn--primary" onClick={() => void saveDefault()}>
              {t("btn.saveDefault")}
            </button>
            <span
              className={
                defaultPhase === "status.saved" || defaultPhase === "status.loaded"
                  ? "kea-status kea-status--ok"
                  : "kea-status"
              }
            >
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

      {tab === "configure" && (
        <section className="kea-panel kea-configure">
          <div className="kea-config-sidenav" aria-label={t("config.sidebarTitle")}>
            <p className="kea-config-sidenav__heading">{t("config.sidebarTitle")}</p>
            <label className="kea-label kea-config-sidenav__search">
              <span className="kea-sr-only">{t("config.searchLabel")}</span>
              <input
                type="search"
                className="kea-input"
                value={configSearchQuery}
                onChange={(e) => setConfigSearchQuery(e.target.value)}
                placeholder={t("config.searchPlaceholder")}
                title={t("config.searchPlaceholder")}
                autoComplete="off"
                spellCheck={false}
                disabled={configureEditsLocked}
              />
            </label>
            {showFullPanelEmpty && (
              <p className="kea-hint kea-config-sidenav__empty" role="status">
                {t("config.noSearchResults")}
              </p>
            )}
            {!showFullPanelEmpty && (
              <>
                <div className="kea-config-sidenav__section">
                  {showWorkflowLocalInPanel && (
                    <button
                      type="button"
                      className={`kea-config-sidenav__btn${configureTarget.id === "workflowLocal" ? " kea-config-sidenav__btn--active" : ""}`}
                      onClick={() => selectConfigureTarget({ id: "workflowLocal" })}
                    >
                      <span className="kea-config-sidenav__btn-primary">{workflowLocalNavPrimary}</span>
                      <span className="kea-config-sidenav__btn-secondary">{t("config.fileHint.workflowLocal")}</span>
                    </button>
                  )}
                  {showWorkflowTemplateInPanel && (
                    <button
                      type="button"
                      className={`kea-config-sidenav__btn${configureTarget.id === "workflowTemplate" ? " kea-config-sidenav__btn--active" : ""}`}
                      onClick={() => selectConfigureTarget({ id: "workflowTemplate" })}
                    >
                      <span className="kea-config-sidenav__btn-primary">{workflowTemplateNavPrimary}</span>
                      <span className="kea-config-sidenav__btn-secondary">{t("config.fileHint.workflowTemplate")}</span>
                    </button>
                  )}
                </div>
                <p className="kea-config-sidenav__heading">{t("config.triggersSection")}</p>
                <div className="kea-config-sidenav__section kea-config-sidenav__section--scroll">
                  {triggerPaths.length === 0 ? (
                    <p className="kea-hint kea-config-sidenav__empty">{t("config.noTriggers")}</p>
                  ) : filteredTriggerPaths.length === 0 ? (
                    <p className="kea-hint kea-config-sidenav__empty" role="status">
                      {t("config.noSearchResults")}
                    </p>
                  ) : (
                    filteredTriggerPaths.map((p) => {
                      const shortPath = p.replace(/^workflows\//, "");
                      const custom = triggerNamesByPath[p];
                      const primary = custom ?? shortPath;
                      return (
                        <button
                          key={p}
                          type="button"
                          title={p}
                          className={`kea-config-sidenav__btn kea-config-sidenav__btn--path${configureTarget.id === "trigger" && configureTarget.path === p ? " kea-config-sidenav__btn--active" : ""}`}
                          onClick={() => selectConfigureTarget({ id: "trigger", path: p })}
                        >
                          <span className="kea-config-sidenav__btn-primary">{primary}</span>
                          {custom ? (
                            <span className="kea-config-sidenav__btn-secondary">{shortPath}</span>
                          ) : null}
                        </button>
                      );
                    })
                  )}
                </div>
              </>
            )}
            <button
              type="button"
              className="kea-btn kea-btn--sm kea-config-sidenav__refresh"
              disabled={configureEditsLocked}
              onClick={() => void refreshArtifactLists()}
            >
              {t("btn.refreshList")}
            </button>
          </div>

          <div className="kea-config-main">
            <div className="kea-callout" role="status">
              {calloutForConfigure()}
            </div>
            {(configureTarget.id === "workflowLocal" || configureTarget.id === "workflowTemplate") && (
              <div className="kea-config-display-name">
                <label className="kea-label">
                  {t("config.displayName")}
                  <input
                    className="kea-input"
                    placeholder={t("config.displayNamePlaceholder")}
                    readOnly={configureEditsLocked}
                    value={
                      (configureTarget.id === "workflowTemplate" ? templateDoc : scopeDoc).name != null
                        ? String((configureTarget.id === "workflowTemplate" ? templateDoc : scopeDoc).name)
                        : ""
                    }
                    onChange={(e) => {
                      const v = e.target.value;
                      const patch = (d: Record<string, unknown>) => {
                        const next = { ...d };
                        if (!v.trim()) delete next.name;
                        else next.name = v;
                        return next;
                      };
                      if (configureTarget.id === "workflowTemplate") setTemplateDoc(patch);
                      else setScopeDoc(patch);
                    }}
                  />
                </label>
              </div>
            )}
            {configureTarget.id === "trigger" && parsedConfigTrigger && (
              <div className="kea-config-display-name">
                <label className="kea-label">
                  {t("config.displayName")}
                  <input
                    className="kea-input"
                    placeholder={t("config.displayNamePlaceholder")}
                    readOnly={configureEditsLocked}
                    value={
                      typeof parsedConfigTrigger.name === "string" ? parsedConfigTrigger.name : ""
                    }
                    onChange={(e) => setTriggerRootName(e.target.value)}
                  />
                </label>
              </div>
            )}
            <div className="kea-toolbar">
              <button
                type="button"
                className="kea-btn kea-btn--ghost"
                disabled={configureEditsLocked}
                onClick={() => void reloadCurrentConfigure()}
              >
                {t("btn.reload")}
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--primary"
                disabled={configureEditsLocked}
                onClick={() => void saveConfigure()}
              >
                {t(saveButtonLabel())}
              </button>
              {configureTarget.id === "workflowLocal" && (
                <button
                  type="button"
                  className="kea-btn"
                  disabled={
                    configureEditsLocked || promoteTemplatesBusy || scopePhase === "status.saving"
                  }
                  title={t("config.updateTemplateTooltip")}
                  onClick={() => void promoteLocalWorkflowTemplates()}
                >
                  {promoteTemplatesBusy ? t("status.running") : t("config.updateTemplate")}
                </button>
              )}
              <span className={configurePhaseOk ? "kea-status kea-status--ok" : "kea-status"}>{configureStatus}</span>
              {isConfigureDirty && (
                <span className="kea-hint kea-hint--warn" role="status">
                  {t("status.unsavedChanges")}
                </span>
              )}
            </div>

            {(configureTarget.id === "workflowLocal" || configureTarget.id === "workflowTemplate") && (
              <>
                <nav className="kea-tabs kea-tabs--sub" aria-label={t("nav.subtabs")}>
                  {configSubtabs.map(({ id, labelKey }) => (
                    <button
                      key={id}
                      type="button"
                      className={tabClass(configSubTab === id)}
                      onClick={() => setConfigSubTab(id)}
                    >
                      {t(labelKey)}
                    </button>
                  ))}
                </nav>
                <div hidden={configSubTab !== "flowCanvas"}>
                  <WorkflowFlowCanvasPreview
                    t={t}
                    document={
                      configureTarget.id === "workflowTemplate" ? templateCanvasDoc : scopeCanvasDoc
                    }
                    reloadNonce={
                      configureTarget.id === "workflowTemplate"
                        ? templateCanvasReloadNonce
                        : scopeCanvasReloadNonce
                    }
                    onEdit={() => {
                      if (!configureEditsLocked) setFlowCanvasEditorOpen(true);
                    }}
                    localRun={flowPreviewLocalRun}
                  />
                </div>
                {configSubTab === "sourceViews" && (
                  <div
                    style={
                      configureEditsLocked
                        ? { pointerEvents: "none", opacity: 0.65 }
                        : undefined
                    }
                  >
                    <SourceViewsControls
                      value={workflowDoc.source_views}
                      onChange={(v) => setWorkflowDoc((d) => ({ ...d, source_views: v }))}
                      schemaSpace={moduleSchemaSpace}
                    />
                  </div>
                )}
                {configSubTab === "matchDefinitions" && (
                  <div
                    style={
                      configureEditsLocked
                        ? { pointerEvents: "none", opacity: 0.65 }
                        : undefined
                    }
                  >
                    <MatchDefinitionsScopePanel
                      scopeDocument={workflowDoc as Record<string, unknown>}
                      onPatch={(recipe) =>
                        setWorkflowDoc((d) => recipe({ ...(d as Record<string, unknown>) }) as typeof d)
                      }
                    />
                  </div>
                )}
                {configSubTab === "keyExtraction" && (
                  <div
                    style={
                      configureEditsLocked
                        ? { pointerEvents: "none", opacity: 0.65 }
                        : undefined
                    }
                  >
                    <KeyExtractionControls
                      value={workflowDoc.key_extraction}
                      onChange={(v) => setWorkflowDoc((d) => ({ ...d, key_extraction: v }))}
                      scopeDocument={workflowDoc as Record<string, unknown>}
                    />
                  </div>
                )}
                {configSubTab === "aliasing" && (
                  <div
                    style={
                      configureEditsLocked
                        ? { pointerEvents: "none", opacity: 0.65 }
                        : undefined
                    }
                  >
                    <AliasingControls
                      value={workflowDoc.aliasing}
                      onChange={(v) => setWorkflowDoc((d) => ({ ...d, aliasing: v }))}
                      scopeDocument={workflowDoc as Record<string, unknown>}
                    />
                  </div>
                )}
                {configSubTab === "runPipeline" && configureRunPipelineSubpanel}
                <div
                  style={
                    configureEditsLocked
                      ? { pointerEvents: "none", opacity: 0.65 }
                      : undefined
                  }
                >
                  <AdvancedYamlPanel
                  initialContent={workflowRawYaml}
                  onSaveRaw={async (content) => {
                    const rel =
                      configureTarget.id === "workflowTemplate" ? TEMPLATE_REL : SCOPE_REL;
                    await api(`/api/scope-document?rel=${encodeURIComponent(rel)}`, {
                      method: "PUT",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ content }),
                    });
                  }}
                  onAfterSave={async () => {
                    const rel =
                      configureTarget.id === "workflowTemplate" ? TEMPLATE_REL : SCOPE_REL;
                    const [model, raw, cModel] = await Promise.all([
                      api<Record<string, unknown>>(
                        `/api/scope-document/model?rel=${encodeURIComponent(rel)}`
                      ),
                      api<{ content: string }>(`/api/scope-document?rel=${encodeURIComponent(rel)}`),
                      api<Record<string, unknown>>(
                        `/api/canvas-document/model?rel=${encodeURIComponent(rel)}`
                      ),
                    ]);
                    if (configureTarget.id === "workflowTemplate") {
                      const tplModel = model && typeof model === "object" ? model : {};
                      setTemplateDoc(tplModel);
                      setTemplateRawYaml(raw.content ?? "");
                      setSavedTemplateSnap(JSON.stringify(model));
                      {
                        const c = canvasDocWithScopeSeedIfEmpty(parseWorkflowCanvasDocument(cModel), tplModel);
                        setTemplateCanvasDoc(c);
                        setSavedTemplateCanvasSnap(JSON.stringify(c));
                        setTemplateCanvasReloadNonce((n) => n + 1);
                      }
                    } else {
                      const scopeModel = model && typeof model === "object" ? model : {};
                      setScopeDoc(scopeModel);
                      setScopeRawYaml(raw.content ?? "");
                      setSavedScopeSnap(JSON.stringify(model));
                      {
                        const c = canvasDocWithScopeSeedIfEmpty(parseWorkflowCanvasDocument(cModel), scopeModel);
                        setScopeCanvasDoc(c);
                        setSavedScopeCanvasSnap(JSON.stringify(c));
                        setScopeCanvasReloadNonce((n) => n + 1);
                      }
                    }
                  }}
                  />
                </div>
              </>
            )}

            {configureTarget.id === "trigger" && (
              <>
                {!parsedConfigTrigger ? (
                  <textarea
                    className="kea-textarea"
                    value={configTriggerText}
                    readOnly={configureEditsLocked}
                    onChange={(e) => setConfigTriggerText(e.target.value)}
                    spellCheck={false}
                    style={{ minHeight: 320 }}
                  />
                ) : (
                  <>
                    <p className="kea-hint" style={{ marginBottom: 8 }}>
                      {t("artifacts.triggerBar")}
                    </p>
                    <nav
                      className="kea-tabs kea-tabs--sub kea-trigger-editor-top"
                      aria-label={t("nav.triggerEditor")}
                    >
                      <button
                        type="button"
                        className={tabClass(triggerTopTab === "triggerAuth")}
                        onClick={() => setTriggerTopTab("triggerAuth")}
                      >
                        {t("triggerEditor.tab.triggerAuth")}
                      </button>
                      <button
                        type="button"
                        className={tabClass(triggerTopTab === "schedule")}
                        onClick={() => setTriggerTopTab("schedule")}
                      >
                        {t("triggerEditor.tab.schedule")}
                      </button>
                      <button
                        type="button"
                        className={tabClass(triggerTopTab === "pipeline")}
                        onClick={() => setTriggerTopTab("pipeline")}
                      >
                        {t("triggerEditor.tab.pipeline")}
                      </button>
                    </nav>

                    {triggerTopTab === "triggerAuth" && (
                      <div
                        style={
                          configureEditsLocked
                            ? { pointerEvents: "none", opacity: 0.65 }
                            : undefined
                        }
                      >
                      <div className="kea-trigger-root-grid kea-trigger-root-grid--two">
                        <p
                          className="kea-trigger-editor-section-title"
                          style={{ gridColumn: "1 / -1" }}
                        >
                          {t("triggerEditor.section.identity")}
                        </p>
                        <label className="kea-label">
                          {t("triggerEditor.externalId")}
                          <input
                            className="kea-input"
                            value={String(parsedConfigTrigger.externalId ?? "")}
                            onChange={(e) => patchTriggerRootFields({ externalId: e.target.value })}
                          />
                        </label>
                        <label className="kea-label">
                          {t("triggerEditor.workflowExternalId")}
                          <input
                            className="kea-input"
                            value={String(parsedConfigTrigger.workflowExternalId ?? "")}
                            onChange={(e) =>
                              patchTriggerRootFields({ workflowExternalId: e.target.value })
                            }
                          />
                        </label>
                        <label className="kea-label" style={{ gridColumn: "1 / -1" }}>
                          {t("triggerEditor.workflowVersion")}
                          <input
                            className="kea-input"
                            value={String(parsedConfigTrigger.workflowVersion ?? "")}
                            onChange={(e) =>
                              patchTriggerRootFields({ workflowVersion: e.target.value })
                            }
                          />
                        </label>
                        <p
                          className="kea-trigger-editor-section-title"
                          style={{ gridColumn: "1 / -1" }}
                        >
                          {t("triggerEditor.section.auth")}
                        </p>
                        <p className="kea-hint" style={{ gridColumn: "1 / -1", maxWidth: "72ch" }}>
                          {t("triggerEditor.authDeployHint")}
                        </p>
                        <p
                          className="kea-trigger-editor-section-title"
                          style={{ gridColumn: "1 / -1" }}
                        >
                          {t("triggerEditor.section.input")}
                        </p>
                        <label className="kea-label">
                          {t("artifacts.runAll")}
                          <input
                            type="checkbox"
                            checked={Boolean((triggerInput as JsonObject | undefined)?.run_all)}
                            onChange={(e) => setTriggerRunAll(e.target.checked)}
                          />
                        </label>
                        <label className="kea-label">
                          {t("artifacts.runId")}
                          <input
                            className="kea-input"
                            value={String((triggerInput as JsonObject | undefined)?.run_id ?? "")}
                            onChange={(e) => setTriggerRunId(e.target.value)}
                          />
                        </label>
                      </div>
                      </div>
                    )}

                    {triggerTopTab === "schedule" && (
                      <div
                        style={
                          configureEditsLocked
                            ? { pointerEvents: "none", opacity: 0.65 }
                            : undefined
                        }
                      >
                      <div className="kea-trigger-root-grid">
                        <p className="kea-trigger-editor-section-title">
                          {t("triggerEditor.section.schedule")}
                        </p>
                        <label className="kea-label">
                          {t("triggerEditor.triggerType")}
                          <input
                            className="kea-input"
                            list={triggerTypeDatalistId}
                            value={String(triggerRuleForForm.triggerType ?? "")}
                            onChange={(e) => patchTriggerRule({ triggerType: e.target.value })}
                          />
                          <datalist id={triggerTypeDatalistId}>
                            <option value="schedule" />
                            <option value="dataModeling" />
                            <option value="recordStream" />
                          </datalist>
                        </label>
                        <label className="kea-label">
                          {t("triggerEditor.cronExpression")}
                          <input
                            className="kea-input"
                            value={String(triggerRuleForForm.cronExpression ?? "")}
                            onChange={(e) => patchTriggerRule({ cronExpression: e.target.value })}
                          />
                        </label>
                        <p className="kea-hint" style={{ marginTop: 4 }}>
                          {t("triggerEditor.scheduleHint")}
                        </p>
                      </div>
                      </div>
                    )}

                    {triggerTopTab === "pipeline" && (
                      <>
                        <nav
                          className="kea-tabs kea-tabs--sub kea-trigger-pipeline-sub"
                          aria-label={t("nav.subtabs")}
                        >
                          {configSubtabs.map(({ id, labelKey }) => (
                            <button
                              key={id}
                              type="button"
                              className={tabClass(configSubTab === id)}
                              onClick={() => setConfigSubTab(id)}
                            >
                              {t(labelKey)}
                            </button>
                          ))}
                        </nav>
                        <div hidden={configSubTab !== "flowCanvas"}>
                          <WorkflowFlowCanvasPreview
                            t={t}
                            document={triggerCanvasDoc}
                            reloadNonce={triggerCanvasReloadNonce}
                            onEdit={() => {
                              if (!configureEditsLocked) setFlowCanvasEditorOpen(true);
                            }}
                            localRun={flowPreviewLocalRun}
                          />
                        </div>
                        {configSubTab === "sourceViews" && (
                          <div
                            style={
                              configureEditsLocked
                                ? { pointerEvents: "none", opacity: 0.65 }
                                : undefined
                            }
                          >
                            <SourceViewsControls
                              value={pipelineConfiguration.source_views}
                              onChange={(v) => setTriggerSourceViews(v)}
                              schemaSpace={moduleSchemaSpace}
                            />
                          </div>
                        )}
                        {configSubTab === "matchDefinitions" && (
                          <div
                            style={
                              configureEditsLocked
                                ? { pointerEvents: "none", opacity: 0.65 }
                                : undefined
                            }
                          >
                            <MatchDefinitionsScopePanel
                              scopeDocument={pipelineConfiguration as Record<string, unknown>}
                              onPatch={(recipe) => {
                                const next = recipe({
                                  ...(pipelineConfiguration as Record<string, unknown>),
                                });
                                updateTriggerConfiguration(next as Partial<JsonObject>);
                              }}
                            />
                          </div>
                        )}
                        {configSubTab === "keyExtraction" && (
                          <div
                            style={
                              configureEditsLocked
                                ? { pointerEvents: "none", opacity: 0.65 }
                                : undefined
                            }
                          >
                            <KeyExtractionControls
                              value={pipelineConfiguration.key_extraction}
                              onChange={(v) => setTriggerKeyExtraction(v)}
                              scopeDocument={pipelineConfiguration as Record<string, unknown>}
                            />
                          </div>
                        )}
                        {configSubTab === "aliasing" && (
                          <div
                            style={
                              configureEditsLocked
                                ? { pointerEvents: "none", opacity: 0.65 }
                                : undefined
                            }
                          >
                            <AliasingControls
                              value={pipelineConfiguration.aliasing}
                              onChange={(v) => setTriggerAliasing(v)}
                              scopeDocument={pipelineConfiguration as Record<string, unknown>}
                            />
                          </div>
                        )}
                        {configSubTab === "runPipeline" && configureRunPipelineSubpanel}
                      </>
                    )}
                  </>
                )}
                <div
                  style={
                    configureEditsLocked
                      ? { pointerEvents: "none", opacity: 0.65 }
                      : undefined
                  }
                >
                  <AdvancedYamlPanel
                  initialContent={configTriggerText}
                  onSaveRaw={async (content) => {
                    await api(`/api/file?rel=${encodeURIComponent(configureTarget.path)}`, {
                      method: "PUT",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ content }),
                    });
                  }}
                  onAfterSave={async () => {
                    await loadConfigTrigger(configureTarget.path);
                  }}
                  />
                </div>
              </>
            )}
          </div>
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
          </div>
          <p className="kea-hint kea-hint--warn" style={{ marginTop: 8, maxWidth: "62ch" }}>
            {t("build.warnForce")}
          </p>
          <textarea
            readOnly
            className="kea-textarea kea-textarea--readonly"
            value={buildLog}
            placeholder={t("build.outputPlaceholder")}
            style={{ minHeight: 280, marginTop: 12 }}
          />
        </section>
      )}

      {tab === "artifacts" && (
        <section className="kea-panel kea-artifacts">
          <div className="kea-artifact-sidebar">
            <p className="kea-artifact-list-title">{t("artifacts.browse")}</p>
            <ArtifactTree
              paths={artifactPaths}
              selectedPath={artifactPath}
              onSelectFile={(rel) => {
                requestArtifactOpen(rel);
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
                disabled={!artifactPath}
              >
                {t("btn.saveFile")}
              </button>
              <span
                className={
                  artifactPhase === "status.saved" || artifactPhase === "status.loaded"
                    ? "kea-status kea-status--ok"
                    : "kea-status"
                }
              >
                {artifactPhase ? t(artifactPhase) : ""}
              </span>
            </div>
            <textarea
              className="kea-textarea"
              value={artifactText}
              onChange={(e) => setArtifactText(e.target.value)}
              spellCheck={false}
              style={{ minHeight: 420 }}
            />
          </div>
        </section>
      )}

      {flowFullscreenOpen && (
        <div
          className="kea-flow-fullscreen"
          role="dialog"
          aria-modal="true"
          aria-labelledby="kea-flow-fullscreen-title"
          aria-describedby="kea-flow-fullscreen-config"
        >
          <div className="kea-flow-fullscreen__bar">
            <div className="kea-flow-fullscreen__title-row">
              <h2 id="kea-flow-fullscreen-title" className="kea-flow-fullscreen__title">
                {t("tabs.flowCanvas")}
              </h2>
              <p id="kea-flow-fullscreen-config" className="kea-flow-fullscreen__config-name">
                {configureTarget.id === "workflowTemplate"
                  ? workflowTemplateNavPrimary
                  : configureTarget.id === "workflowLocal"
                    ? workflowLocalNavPrimary
                    : configureTarget.id === "trigger"
                      ? triggerNamesByPath[configureTarget.path] ??
                        configureTarget.path.replace(/^workflows\//, "")
                      : ""}
              </p>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", flexWrap: "wrap" }}>
              {(configureTarget.id === "workflowLocal"
                ? isScopeCanvasDirty
                : configureTarget.id === "workflowTemplate"
                  ? isTemplateCanvasDirty
                  : isConfigTriggerDirty) && (
                <span className="kea-hint kea-hint--warn" role="status">
                  {t("status.unsavedChanges")}
                </span>
              )}
              <button
                type="button"
                className="kea-btn kea-btn--primary"
                disabled={
                  configureEditsLocked ||
                  (configureTarget.id === "workflowLocal"
                    ? scopePhase === "status.saving"
                    : configureTarget.id === "workflowTemplate"
                      ? templatePhase === "status.saving"
                      : configTriggerPhase === "status.saving")
                }
                onClick={() => {
                  if (configureTarget.id === "workflowLocal") void saveScope();
                  else if (configureTarget.id === "workflowTemplate") void saveTemplate();
                  else void saveConfigureTrigger();
                }}
              >
                {(configureTarget.id === "workflowLocal"
                  ? scopePhase === "status.saving"
                  : configureTarget.id === "workflowTemplate"
                    ? templatePhase === "status.saving"
                    : configTriggerPhase === "status.saving")
                  ? t("status.saving")
                  : t("flow.save")}
              </button>
              <button
                type="button"
                className="kea-btn"
                onClick={() => setFlowCanvasEditorOpen(false)}
              >
                {t("flow.close")}
              </button>
            </div>
          </div>
          <div className="kea-flow-fullscreen__body">
            {configureTarget.id === "workflowLocal" ? (
              <WorkflowFlowPanel
                t={t}
                initialDocument={scopeCanvasDoc}
                reloadNonce={scopeCanvasReloadNonce}
                workflowScopeDoc={workflowDoc as Record<string, unknown>}
                onPatchWorkflowScope={(recipe) =>
                  setWorkflowDoc((d) => recipe({ ...(d as Record<string, unknown>) }))
                }
                onChange={setScopeCanvasDoc}
                onSyncScopeFromCanvas={(canvas) =>
                  setScopeDoc((d) =>
                    syncWorkflowScopeFromCanvas(canvas, { ...(d as Record<string, unknown>) })
                  )
                }
                schemaSpace={moduleSchemaSpace}
                readOnly={configureEditsLocked}
              />
            ) : configureTarget.id === "workflowTemplate" ? (
              <WorkflowFlowPanel
                t={t}
                initialDocument={templateCanvasDoc}
                reloadNonce={templateCanvasReloadNonce}
                workflowScopeDoc={workflowDoc as Record<string, unknown>}
                onPatchWorkflowScope={(recipe) =>
                  setWorkflowDoc((d) => recipe({ ...(d as Record<string, unknown>) }))
                }
                onChange={setTemplateCanvasDoc}
                onSyncScopeFromCanvas={(canvas) =>
                  setTemplateDoc((d) =>
                    syncWorkflowScopeFromCanvas(canvas, { ...(d as Record<string, unknown>) })
                  )
                }
                schemaSpace={moduleSchemaSpace}
                readOnly={configureEditsLocked}
              />
            ) : (
              <WorkflowFlowPanel
                t={t}
                initialDocument={triggerCanvasDoc}
                reloadNonce={triggerCanvasReloadNonce}
                workflowScopeDoc={pipelineConfiguration as Record<string, unknown>}
                onPatchWorkflowScope={(recipe) => {
                  const next = recipe({ ...(pipelineConfiguration as Record<string, unknown>) });
                  updateTriggerConfiguration(next as Partial<JsonObject>);
                }}
                onChange={(doc) =>
                  updateTriggerConfiguration({
                    [TRIGGER_WORKFLOW_CANVAS_KEY]: doc as unknown as JsonObject,
                  })
                }
                schemaSpace={moduleSchemaSpace}
                onSyncScopeFromCanvas={(canvas) => {
                  const next = syncWorkflowScopeFromCanvas(
                    canvas,
                    pipelineConfiguration as Record<string, unknown>
                  );
                  updateTriggerConfiguration(next as Partial<JsonObject>);
                }}
                readOnly={configureEditsLocked}
              />
            )}
          </div>
        </div>
      )}

      {unsavedPrompt && (
        <div
          className="kea-modal-backdrop"
          role="presentation"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget && !unsavedBusy) setUnsavedPrompt(null);
          }}
        >
          <div
            className="kea-modal"
            role="dialog"
            aria-modal
            aria-labelledby="kea-unsaved-title"
            onMouseDown={(e) => e.stopPropagation()}
          >
            <h2 id="kea-unsaved-title" className="kea-modal__title">
              {t("unsaved.title")}
            </h2>
            <p className="kea-hint" style={{ marginTop: 0 }}>
              {t("unsaved.message")}
            </p>
            <div className="kea-modal__actions">
              <button
                type="button"
                className="kea-btn kea-btn--primary"
                disabled={unsavedBusy}
                onClick={() => void commitUnsavedSave()}
              >
                {t("unsaved.save")}
              </button>
              <button
                type="button"
                className="kea-btn"
                disabled={unsavedBusy}
                onClick={() => void commitUnsavedDiscard()}
              >
                {t("unsaved.discard")}
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost"
                disabled={unsavedBusy}
                onClick={() => setUnsavedPrompt(null)}
              >
                {t("btn.cancel")}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
