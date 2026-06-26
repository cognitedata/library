import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchConfig, saveConfig } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { ConfigSection } from "../../types/invertedIndexConfig";
import {
  annotationFromDoc,
  CONFIG_SECTIONS,
  directRelationFromDoc,
  docFromYaml,
  generalFromDoc,
  indexFieldsFromDoc,
  instanceSpacesFromDoc,
  mergeAnnotationIntoDoc,
  mergeDirectRelationIntoDoc,
  mergeGeneralIntoDoc,
  mergeIndexFieldsIntoDoc,
  mergeScopeIntoDoc,
  mergeTargetDrivenIntoDoc,
  scopeFromDoc,
  subscriptionFromDoc,
  yamlFromDoc,
} from "../../utils/defaultConfigYaml";
import { AdvancedYamlSection } from "../config/AdvancedYamlSection";
import { AnnotationIndexEditor } from "../config/AnnotationIndexEditor";
import { DirectRelationEditor } from "../config/DirectRelationEditor";
import { GeneralConfigEditor } from "../config/GeneralConfigEditor";
import { IndexFieldConfigEditor } from "../config/IndexFieldConfigEditor";
import { ScopeResolveEditor } from "../config/ScopeResolveEditor";
import { TargetDrivenConfigEditor } from "../config/TargetDrivenConfigEditor";

export function ConfigPane({ embedded = false }: { embedded?: boolean }) {
  const { t } = useAppSettings();
  const [activeSection, setActiveSection] = useState<ConfigSection>("general");
  const [yamlText, setYamlText] = useState("");
  const [savedText, setSavedText] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const cfg = await fetchConfig();
        if (cancelled) return;
        setYamlText(cfg.yaml_text);
        setSavedText(cfg.yaml_text);
      } catch (e) {
        if (!cancelled) setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const doc = useMemo(() => {
    try {
      return docFromYaml(yamlText);
    } catch {
      return null;
    }
  }, [yamlText]);

  const dirty = yamlText !== savedText;

  const updateDoc = useCallback((mutate: (d: Record<string, unknown>) => void) => {
    try {
      const next = docFromYaml(yamlText);
      mutate(next);
      setYamlText(yamlFromDoc(next));
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, [yamlText]);

  const onSave = useCallback(async () => {
    setSaving(true);
    setError(null);
    setMessage(null);
    try {
      const cfg = await saveConfig(yamlText);
      setSavedText(cfg.yaml_text);
      setYamlText(cfg.yaml_text);
      setMessage(t("config.saved"));
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }, [t, yamlText]);

  const onRevert = useCallback(() => {
    setYamlText(savedText);
    setMessage(null);
    setError(null);
  }, [savedText]);

  if (loading) {
    return (
      <div className={embedded ? undefined : "idx-pane"}>
        <p>{t("config.loading")}</p>
      </div>
    );
  }

  const sectionHintKey = `config.sectionHint.${activeSection}` as const;

  return (
    <div className={embedded ? "idx-config-pane idx-config-pane--embedded" : "idx-pane idx-config-pane"}>
      <header className="idx-config-pane__header">
        {embedded ? null : (
          <div>
            <h2 className="idx-pane__title">{t("config.title")}</h2>
            <p className="idx-pane__hint">{t("config.hint")}</p>
          </div>
        )}
        <div className={`idx-config-pane__toolbar${embedded ? " idx-config-pane__toolbar--embedded" : ""}`}>
          {dirty ? <span className="idx-badge">{t("config.unsaved")}</span> : null}
          <button type="button" className="idx-btn idx-btn--primary" disabled={saving || !dirty} onClick={onSave}>
            {t("config.save")}
          </button>
          <button type="button" className="idx-btn" disabled={saving || !dirty} onClick={onRevert}>
            {t("config.revert")}
          </button>
        </div>
      </header>

      {message ? <p className="idx-pane__hint">{message}</p> : null}
      {error ? (
        <p className="idx-operation-result__status--error">
          {t("config.saveError")}: {error}
        </p>
      ) : null}

      <nav className="idx-config-tabs" aria-label={t("config.sectionsLabel")}>
        {CONFIG_SECTIONS.map((section) => (
          <button
            key={section.id}
            type="button"
            className={`idx-config-tab${activeSection === section.id ? " idx-config-tab--active" : ""}`}
            aria-current={activeSection === section.id ? "page" : undefined}
            onClick={() => setActiveSection(section.id)}
          >
            {t(section.labelKey)}
          </button>
        ))}
      </nav>

      <p className="idx-pane__hint">{t(sectionHintKey)}</p>

      <div className="idx-config-pane__body">
        {!doc ? (
          <p className="idx-operation-result__status--error">{t("config.parseError")}</p>
        ) : null}

        {doc && activeSection === "general" ? (
          <GeneralConfigEditor
            value={generalFromDoc(doc)}
            onChange={(general) => updateDoc((d) => mergeGeneralIntoDoc(d, general))}
          />
        ) : null}

        {doc && activeSection === "scope" ? (
          <ScopeResolveEditor
            value={scopeFromDoc(doc)}
            onChange={(scope) => updateDoc((d) => mergeScopeIntoDoc(d, scope))}
          />
        ) : null}

        {doc && activeSection === "indexFields" ? (
          <IndexFieldConfigEditor
            value={indexFieldsFromDoc(doc)}
            onChange={(views) => updateDoc((d) => mergeIndexFieldsIntoDoc(d, views))}
          />
        ) : null}

        {doc && activeSection === "annotation" ? (
          <AnnotationIndexEditor
            value={annotationFromDoc(doc)}
            onChange={(cfg) => updateDoc((d) => mergeAnnotationIntoDoc(d, cfg))}
          />
        ) : null}

        {doc && activeSection === "targetDriven" ? (
          <TargetDrivenConfigEditor
            subscription={subscriptionFromDoc(doc)}
            instanceSpaces={instanceSpacesFromDoc(doc)}
            onSubscriptionChange={(subscription) =>
              updateDoc((d) =>
                mergeTargetDrivenIntoDoc(d, subscription, instanceSpacesFromDoc(d))
              )
            }
            onInstanceSpacesChange={(instanceSpaces) =>
              updateDoc((d) =>
                mergeTargetDrivenIntoDoc(d, subscriptionFromDoc(d), instanceSpaces)
              )
            }
          />
        ) : null}

        {doc && activeSection === "linking" ? (
          <DirectRelationEditor
            value={directRelationFromDoc(doc)}
            onChange={(cfg) => updateDoc((d) => mergeDirectRelationIntoDoc(d, cfg))}
          />
        ) : null}
      </div>

      <AdvancedYamlSection
        yamlText={yamlText}
        onApply={(content) => {
          setYamlText(content);
          setError(null);
        }}
      />
    </div>
  );
}
