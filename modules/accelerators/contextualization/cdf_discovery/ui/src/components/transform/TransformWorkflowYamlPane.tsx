import { useCallback, useEffect, useRef, useState } from "react";
import { fetchTransformWorkflowYaml, saveTransformWorkflowYaml } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { AdvancedYamlPanel } from "../governance/AdvancedYamlPanel";
import { GovernanceToolbar } from "../governance/GovernanceToolbar";
import type { EtlWorkflowYamlDocumentTab } from "../../types/discoveryNodes";

type Props = {
  tab: EtlWorkflowYamlDocumentTab;
  onTabUpdate: (tab: EtlWorkflowYamlDocumentTab) => void;
};

export function TransformWorkflowYamlPane({ tab, onTabUpdate }: Props) {
  const { t } = useAppSettings();
  const [content, setContent] = useState("");
  const [savedSnapshot, setSavedSnapshot] = useState("");
  const savedSnapshotRef = useRef("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadGen = useRef(0);
  const tabRef = useRef(tab);
  const onTabUpdateRef = useRef(onTabUpdate);
  tabRef.current = tab;
  onTabUpdateRef.current = onTabUpdate;

  const dirty = savedSnapshot !== content;

  const load = useCallback(async () => {
    const gen = ++loadGen.current;
    setLoading(true);
    setError(null);
    onTabUpdateRef.current({ ...tabRef.current, loading: true, error: null });
    try {
      const data = await fetchTransformWorkflowYaml(tabRef.current.relPath);
      if (gen !== loadGen.current) return;
      setContent(data.content);
      setSavedSnapshot(data.content);
      savedSnapshotRef.current = data.content;
      onTabUpdateRef.current({
        ...tabRef.current,
        loading: false,
        error: null,
        dirty: false,
      });
    } catch (e) {
      if (gen !== loadGen.current) return;
      const message = String(e);
      setError(message);
      onTabUpdateRef.current({ ...tabRef.current, loading: false, error: message });
    } finally {
      if (gen === loadGen.current) setLoading(false);
    }
  }, [tab.relPath]);

  useEffect(() => {
    setContent("");
    setSavedSnapshot("");
    savedSnapshotRef.current = "";
    void load();
  }, [load]);

  const save = useCallback(async () => {
    setSaving(true);
    setError(null);
    try {
      await saveTransformWorkflowYaml(tabRef.current.relPath, content);
      setSavedSnapshot(content);
      savedSnapshotRef.current = content;
      onTabUpdateRef.current({ ...tabRef.current, dirty: false, error: null });
    } catch (e) {
      const message = String(e);
      setError(message);
      onTabUpdateRef.current({ ...tabRef.current, error: message });
    } finally {
      setSaving(false);
    }
  }, [content]);

  const onSaveRaw = useCallback(async (raw: string) => {
    setContent(raw);
    await saveTransformWorkflowYaml(tabRef.current.relPath, raw);
    setSavedSnapshot(raw);
    savedSnapshotRef.current = raw;
    onTabUpdateRef.current({ ...tabRef.current, dirty: false, error: null });
  }, []);

  const onContentChange = useCallback((next: string) => {
    setContent(next);
  }, []);

  useEffect(() => {
    const id = window.setTimeout(() => {
      const dirty = content !== savedSnapshotRef.current;
      if (tabRef.current.dirty === dirty) return;
      onTabUpdateRef.current({ ...tabRef.current, dirty });
    }, 300);
    return () => window.clearTimeout(id);
  }, [content]);

  return (
    <div className="disc-gov-pane transform-workflow-yaml-pane">
      <header className="disc-gov-pane-header">
        <div className="disc-gov-pane-header__row">
          <p className="disc-gov-pane-header__hint">{t("transform.workflowYaml.hint")}</p>
          <GovernanceToolbar
            dirty={dirty}
            loading={loading}
            saving={saving}
            error={error}
            onReload={() => void load()}
            onSave={() => void save()}
          />
        </div>
      </header>
      <div className="disc-gov-pane-body gov-stack transform-workflow-yaml-pane__body">
        <p className="transform-workflow-yaml-pane__path">
          <code>{tab.relPath}</code>
        </p>
        {loading && !content ? (
          <p className="disc-empty-hint">{t("tree.loading")}</p>
        ) : error && !content ? (
          <p className="disc-banner--error" role="alert">
            {error}
          </p>
        ) : (
          <>
            <label className="gov-label" htmlFor={`workflow-yaml-${tab.id}`}>
              {tab.label}
            </label>
            <textarea
              id={`workflow-yaml-${tab.id}`}
              className="gov-textarea transform-workflow-yaml-pane__editor"
              value={content}
              onChange={(e) => onContentChange(e.target.value)}
              spellCheck={false}
              disabled={loading}
              aria-busy={loading}
            />
            <AdvancedYamlPanel
              key={`${tab.relPath}:${savedSnapshot.length}`}
              initialContent={content}
              onSaveRaw={onSaveRaw}
              onAfterSave={load}
            />
          </>
        )}
      </div>
    </div>
  );
}
